use anyhow::{anyhow, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::thread::JoinHandle;

use crate::network::remote::remote_recv;
use crate::network::{Coord, NetworkSender};

/// The capacity of the in-buffer.
const CHANNEL_CAPACITY: usize = 10;

/// The receiving end of a connection between two replicas.
///
/// This works for both a local in-memory connection and for a remote socket connection. This will
/// always be able to listen to a socket. No socket will be bound until a message is sent to the
/// starter returned by the constructor.
///
/// Internally it contains a in-memory sender-receiver pair, to get the local sender call
/// `.sender()`. When the socket will be bound an task will be spawned, it will bind the
/// socket and send to the same in-memory channel the received messages.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct NetworkReceiver<In> {
    /// The coord of the current receiver.
    pub coord: Coord,
    /// The actual receiver where the users of this struct will wait upon.
    #[derivative(Debug = "ignore")]
    receiver: Receiver<In>,
    /// The sender associated with `self.receiver`.
    #[derivative(Debug = "ignore")]
    local_sender: SyncSender<In>,
}

impl<In> NetworkReceiver<In>
where
    In: Send + Serialize + DeserializeOwned + 'static,
{
    /// Construct a new `NetworkReceiver` that will be ready to bind a socket on the specified
    /// address.
    ///
    /// The returned value is:
    /// - the `NetworkReceiver`
    /// - a sender that will control the binding of the socket: sending a positive value will bind
    ///   a socket on the specified address. The value sent is the number of senders that will
    ///   connect remotely to that socket. When they all connects the socket is unbound.
    /// - an handle where to wait for the socket task
    pub fn new(coord: Coord, address: (String, u16)) -> (Self, SyncSender<usize>, JoinHandle<()>) {
        let (sender, receiver) = sync_channel(CHANNEL_CAPACITY);
        let (bind_socket, bind_socket_rx) = sync_channel(CHANNEL_CAPACITY);
        let local_sender = sender.clone();
        let join_handle = std::thread::Builder::new()
            .name(format!("NetRecv{}", coord))
            .spawn(move || {
                NetworkReceiver::bind_remote(coord, local_sender, address, bind_socket_rx);
            })
            .unwrap();
        (
            Self {
                coord,
                receiver,
                local_sender: sender,
            },
            bind_socket,
            join_handle,
        )
    }

    /// Obtain a `NetworkSender` that will send messages that will arrive to this receiver.
    pub fn sender(&self) -> NetworkSender<In> {
        NetworkSender::local(self.coord, self.local_sender.clone())
    }

    /// Receive a message from any sender.
    pub fn recv(&self) -> Result<In> {
        self.receiver
            .recv()
            .map_err(|e| anyhow!("Failed to receive from channel at {}: {:?}", self.coord, e))
    }

    /// The task that will eventually bind the socket.
    ///
    /// If 0 is sent to `bind_socket` no socket will be bound, otherwise exactly that number of
    /// connections will be awaited and served. After that the task will exit unbinding the socket.
    fn bind_remote(
        coord: Coord,
        local_sender: SyncSender<In>,
        address: (String, u16),
        bind_socket: Receiver<usize>,
    ) {
        // wait the signal before binding the socket
        let num_connections = bind_socket.recv().unwrap();
        if num_connections == 0 {
            debug!(
                "Remote receiver at {} is asked not to bind, exiting...",
                coord
            );
            return;
        }
        // from now we can start binding the socket
        let address = (address.0.as_ref(), address.1);
        let address: Vec<_> = address
            .to_socket_addrs()
            .map_err(|e| format!("Failed to get the address for {}: {:?}", coord, e))
            .unwrap()
            .collect();
        let listener = TcpListener::bind(&*address)
            .map_err(|e| {
                anyhow!(
                    "Failed to bind socket for {} at {:?}: {:?}",
                    coord,
                    address,
                    e
                )
            })
            .unwrap();
        let address = listener
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        info!(
            "Remote receiver at {} is ready to accept {} connections to {}",
            coord, num_connections, address
        );
        let local_sender = local_sender.clone();
        let mut incoming = listener.incoming();
        let mut join_handles = Vec::new();
        for conn_num in 1..=num_connections {
            let stream = incoming.next().unwrap();
            let stream = match stream {
                Ok(stream) => stream,
                Err(e) => {
                    warn!("Failed to accept incoming connection at {}: {:?}", coord, e);
                    continue;
                }
            };
            info!(
                "Remote receiver at {} accepted a new connection from {:?} ({} / {})",
                coord,
                stream.peer_addr(),
                conn_num,
                num_connections
            );
            let local_sender = local_sender.clone();
            let join_handle = std::thread::Builder::new()
                .name(format!("NetClient{}", coord))
                .spawn(move || NetworkReceiver::handle_remote_client(coord, local_sender, stream))
                .unwrap();
            join_handles.push(join_handle);
        }
        debug!(
            "Remote receiver at {} ({}) accepted all {} connections, joining them...",
            coord, address, num_connections
        );
        for join_handle in join_handles {
            join_handle.join().unwrap();
        }
        debug!(
            "Remote receiver at {} ({}) finished, exiting...",
            coord, address
        );
    }

    /// Handle the connection with a remote sender.
    ///
    /// This will receiver every message, deserialize it and send it back to the receiver using the
    /// local channel.
    fn handle_remote_client(coord: Coord, local_sender: SyncSender<In>, mut receiver: TcpStream) {
        let address = receiver
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        while let Some(message) = remote_recv(coord, &mut receiver) {
            local_sender.send(message).unwrap_or_else(|e| {
                panic!("Failed to send to local receiver at {}: {:?}", coord, e)
            });
        }
        let _ = receiver.shutdown(Shutdown::Both);
        debug!("Remote receiver for {} at {} exited", coord, address);
    }
}
