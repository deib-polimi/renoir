use std::io::ErrorKind;
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::thread::{sleep, JoinHandle};
use std::time::Duration;

use anyhow::{anyhow, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::network::remote::remote_send;
use crate::network::{wait_start, Coord, NetworkStarterRecv};

/// The capacity of the out-buffer.
const CHANNEL_CAPACITY: usize = 10;

/// Maximum number of attempts to make for connecting to a remote host.
const CONNECT_ATTEMPTS: usize = 10;
/// Timeout for connecting to a remote host.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
/// To avoid spamming the connections, wait this timeout before trying again. If the connection
/// fails again this timeout will be doubled up to `RETRY_MAX_TIMEOUT`.
const RETRY_INITIAL_TIMEOUT: Duration = Duration::from_millis(50);
/// Maximum timeout between connection attempts.
const RETRY_MAX_TIMEOUT: Duration = Duration::from_secs(1);

/// The sender part of a connection between two replicas.
///
/// This works for both a local in-memory connection and for a remote socket connection. When this
/// is bound to a local channel the receiver will be a `Receiver`. When it's bound to a remote
/// connection the receiver is inside an task that receives locally via the in-memory channel
/// and then send the message via a socket.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub(crate) struct NetworkSender<Out> {
    /// The coord of the recipient.
    pub coord: Coord,
    /// The channel that will be used to send the message. It will be either to the local recipient
    /// or to an task that will send the message remotely.
    #[derivative(Debug = "ignore")]
    sender: SyncSender<Out>,
}

impl<Out> NetworkSender<Out>
where
    Out: Serialize + DeserializeOwned + Send + 'static,
{
    /// Create a new local sender that sends the data directly to the recipient.
    pub fn local(coord: Coord, sender: SyncSender<Out>) -> Self {
        Self { coord, sender }
    }

    /// Create a new remote sender that will send the data via a socket to the specified address.
    pub fn remote(
        coord: Coord,
        address: (String, u16),
    ) -> (Self, SyncSender<bool>, JoinHandle<()>) {
        let (sender, receiver) = sync_channel(CHANNEL_CAPACITY);
        let (connect_socket, connect_socket_rx) = sync_channel(CHANNEL_CAPACITY);
        let join_handle = std::thread::Builder::new()
            .name(format!("NetSender{}", coord))
            .spawn(move || {
                NetworkSender::connect_remote(coord, receiver, address, connect_socket_rx);
            })
            .unwrap();
        (Self { coord, sender }, connect_socket, join_handle)
    }

    /// Send a message to a replica.
    pub fn send(&self, item: Out) -> Result<()> {
        self.sender
            .send(item)
            .map_err(|e| anyhow!("Failed to send to channel to {}: {:?}", self.coord, e))
    }

    /// Connect the sender to a remote channel located at the specified address.
    ///
    /// - At first the address is resolved to an actual address (DNS resolution)
    /// - Then at most `CONNECT_ATTEMPTS` are performed, and an exponential backoff is used in case
    ///   of errors.
    /// - If the connection cannot be established this function will panic.
    fn connect_remote(
        coord: Coord,
        local_receiver: Receiver<Out>,
        address: (String, u16),
        connect_socket: NetworkStarterRecv,
    ) {
        // wait the signal before connecting the socket
        if !wait_start(connect_socket) {
            debug!(
                "Remote sender at {} is asked not to connect, exiting...",
                coord
            );
            return;
        }

        let address = (address.0.as_str(), address.1);
        let socket_addrs: Vec<_> = address
            .to_socket_addrs()
            .map_err(|e| format!("Failed to get the address for {}: {:?}", coord, e))
            .unwrap()
            .collect();
        let mut retry_delay = RETRY_INITIAL_TIMEOUT;
        for attempt in 1..=CONNECT_ATTEMPTS {
            debug!(
                "Attempt {} to connect to {} at {:?}",
                attempt, coord, socket_addrs
            );

            for address in socket_addrs.iter() {
                match TcpStream::connect_timeout(address, CONNECT_TIMEOUT) {
                    Ok(stream) => {
                        NetworkSender::handle_remote_connection(coord, local_receiver, stream);
                        return;
                    }
                    Err(err) => match err.kind() {
                        ErrorKind::TimedOut => {
                            debug!("Timeout connecting to {} at {:?}", coord, address);
                        }
                        _ => {
                            debug!("Failed to connect to {} at {:?}: {:?}", coord, address, err);
                        }
                    },
                }
            }

            debug!(
                "Retrying connection to {} at {:?} in {}s",
                coord,
                socket_addrs,
                retry_delay.as_secs_f32(),
            );

            sleep(retry_delay);
            retry_delay = (2 * retry_delay).min(RETRY_MAX_TIMEOUT);
        }
        panic!(
            "Failed to connect to remote {} at {:?} after {} attempts",
            coord, address, CONNECT_ATTEMPTS
        );
    }

    /// Handle the connection to the remote replica.
    ///
    /// Waits messages from the local receiver, then serialize the message and send it to the remote
    /// replica.
    fn handle_remote_connection(
        coord: Coord,
        local_receiver: Receiver<Out>,
        mut stream: TcpStream,
    ) {
        let address = stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        debug!("Connection to {} at {} established", coord, address);
        while let Ok(out) = local_receiver.recv() {
            remote_send(out, coord, &mut stream);
        }
        let _ = stream.shutdown(Shutdown::Both);
        debug!("Remote sender for {} exited", coord);
    }
}
