use std::collections::HashMap;
use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::thread::JoinHandle;

use anyhow::anyhow;
use derivative::Derivative;

use crate::channel::{
    BoundedChannelReceiver, BoundedChannelSender, UnboundedChannelReceiver, UnboundedChannelSender,
};
use crate::network::remote::{deserialize, remote_recv, SerializedMessage, CHANNEL_CAPACITY};
use crate::network::{DemuxCoord, ReceiverEndpoint};
use crate::operator::Data;

/// Like `NetworkReceiver`, but this should be used in a multiplexed channel (i.e. a remote one).
///
/// This receiver is handled in a separate thread that keeps track of the local registered receivers
/// and the open connections. The incoming messages are tagged with the receiver endpoint. Upon
/// arrival they are routed to the correct receiver according to the `ReceiverEndpoint` the message
/// is tagged with.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct DemultiplexingReceiver<In: Data> {
    /// The coordinate of this demultiplexer.
    coord: DemuxCoord,
    /// Tell the demultiplexer that a new receiver is present,
    register_receiver: UnboundedChannelSender<(ReceiverEndpoint, BoundedChannelSender<In>)>,
}

impl<In: Data> DemultiplexingReceiver<In> {
    /// Construct a new `DemultiplexingReceiver` for a block.
    ///
    /// All the local replicas of this block should be registered to this demultiplexer.
    /// `num_client` is the number of multiplexers that will connect to this demultiplexer. Since
    /// the remote senders are all multiplexed this corresponds to the number of remote replicas in
    /// the previous block (relative to the block this demultiplexer refers to).
    pub fn new(
        coord: DemuxCoord,
        address: (String, u16),
        num_clients: usize,
    ) -> (Self, JoinHandle<()>) {
        let (sender, receiver) = UnboundedChannelReceiver::new();
        let join_handle = std::thread::Builder::new()
            .name(format!("Net{}", coord))
            .spawn(move || Self::bind_remote(coord, address, receiver, num_clients))
            .unwrap();
        (
            Self {
                coord,
                register_receiver: sender,
            },
            join_handle,
        )
    }

    /// Register a local receiver to this demultiplexer.
    pub fn register(
        &mut self,
        receiver_endpoint: ReceiverEndpoint,
        local_sender: BoundedChannelSender<In>,
    ) {
        debug!(
            "Registering {} to the demultiplexer of {}",
            receiver_endpoint, self.coord
        );
        self.register_receiver
            .send((receiver_endpoint, local_sender))
            .unwrap();
    }

    /// Bind the socket of this demultiplexer.
    fn bind_remote(
        coord: DemuxCoord,
        address: (String, u16),
        register_receiver: UnboundedChannelReceiver<(ReceiverEndpoint, BoundedChannelSender<In>)>,
        num_clients: usize,
    ) {
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
            coord, num_clients, address
        );

        let (message_sender, message_receiver) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);
        // spawn an extra thread that keeps track of the connected clients and registered receivers
        let join_handle = std::thread::Builder::new()
            .name(format!("{}", coord))
            .spawn(move || Self::demultiplexer_thread(coord, message_receiver, register_receiver))
            .unwrap();

        // the list of JoinHandle of all the spawned threads, including the demultiplexer one
        let mut join_handles = vec![join_handle];

        let mut incoming = listener.incoming();
        let mut connected_clients = 0;
        while connected_clients < num_clients {
            let stream = incoming.next().unwrap();
            let stream = match stream {
                Ok(stream) => stream,
                Err(e) => {
                    warn!("Failed to accept incoming connection at {}: {:?}", coord, e);
                    continue;
                }
            };
            connected_clients += 1;
            let peer_addr = stream.peer_addr().unwrap();
            info!(
                "Remote receiver at {} accepted a new connection from {} ({} / {})",
                coord, peer_addr, connected_clients, num_clients
            );

            let message_sender = message_sender.clone();
            let join_handle = std::thread::Builder::new()
                .name(format!("Client{}", coord))
                .spawn(move || Self::handle_remote_client(coord, message_sender, stream))
                .unwrap();
            join_handles.push(join_handle);
        }
        debug!(
            "All connection to {} started, waiting for them to finish",
            coord
        );
        // make sure the demultiplexer thread can exit
        drop(message_sender);
        for handle in join_handles {
            handle.join().unwrap();
        }
        debug!("Demultiplexer of {} finished", coord);
    }

    /// The body of the thread that receives serialized messages and local replicas and routes the
    /// messages to the correct replica.
    fn demultiplexer_thread(
        coord: DemuxCoord,
        message_receiver: BoundedChannelReceiver<(ReceiverEndpoint, SerializedMessage)>,
        register_receiver: UnboundedChannelReceiver<(ReceiverEndpoint, BoundedChannelSender<In>)>,
    ) {
        debug!("Starting demultiplexer for {}", coord);
        let mut known_receivers = HashMap::new();
        while let Ok((dest, message)) = message_receiver.recv() {
            // a message arrived to a not-yet-registered local receiver, wait for the missing
            // receiver
            while !known_receivers.contains_key(&dest) {
                let (dest, sender) = register_receiver.recv().unwrap();
                known_receivers.insert(dest, sender);
            }
            let message = deserialize::<In>(message).unwrap();
            if let Err(e) = known_receivers[&dest].send(message) {
                warn!("Failed to send message to {}: {:?}", dest, e);
            }
        }
    }

    /// Handle the connection with a remote sender.
    ///
    /// This will receive every message and send it to the demultiplexer using the local channel.
    fn handle_remote_client(
        coord: DemuxCoord,
        local_sender: BoundedChannelSender<(ReceiverEndpoint, SerializedMessage)>,
        mut receiver: TcpStream,
    ) {
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
