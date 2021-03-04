use std::io::ErrorKind;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_std::channel::{bounded, Receiver, Sender};
use async_std::io::timeout;
use async_std::net::{Shutdown, TcpStream, ToSocketAddrs};
use async_std::task::spawn;
use async_std::task::{sleep, JoinHandle};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::network::remote::remote_send;
use crate::network::{wait_start, Coord, NetworkStarter, NetworkStarterRecv};

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
/// connection the receiver is inside an async task that receives locally via the in-memory channel
/// and then send the message via a socket.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub(crate) struct NetworkSender<Out> {
    /// The coord of the recipient.
    pub coord: Coord,
    /// The channel that will be used to send the message. It will be either to the local recipient
    /// or to an async task that will send the message remotely.
    #[derivative(Debug = "ignore")]
    sender: Sender<Out>,
}

impl<Out> NetworkSender<Out>
where
    Out: Serialize + DeserializeOwned + Send + 'static,
{
    /// Create a new local sender that sends the data directly to the recipient.
    pub fn local(coord: Coord, sender: Sender<Out>) -> Self {
        Self { coord, sender }
    }

    /// Create a new remote sender that will send the data via a socket to the specified address.
    pub fn remote(coord: Coord, address: (String, u16)) -> (Self, NetworkStarter, JoinHandle<()>) {
        let (sender, receiver) = bounded(CHANNEL_CAPACITY);
        let (connect_socket, connect_socket_rx) = bounded(CHANNEL_CAPACITY);
        let join_handle = spawn(async move {
            NetworkSender::connect_remote(coord, receiver, address, connect_socket_rx).await;
        });
        (Self { coord, sender }, connect_socket, join_handle)
    }

    /// Send a message to a replica.
    pub async fn send(&self, item: Out) -> Result<()> {
        self.sender
            .send(item)
            .await
            .map_err(|e| anyhow!("Failed to send to channel to {}: {:?}", self.coord, e))
    }

    /// Connect the sender to a remote channel located at the specified address.
    ///
    /// - At first the address is resolved to an actual address (DNS resolution)
    /// - Then at most `CONNECT_ATTEMPTS` are performed, and an exponential backoff is used in case
    ///   of errors.
    /// - If the connection cannot be established this function will panic.
    async fn connect_remote(
        coord: Coord,
        local_receiver: Receiver<Out>,
        address: (String, u16),
        connect_socket: NetworkStarterRecv,
    ) {
        // wait the signal before connecting the socket
        if !wait_start(connect_socket).await {
            debug!(
                "Remote sender at {} is asked not to connect, exiting...",
                coord
            );
            return;
        }

        let address = (address.0.as_str(), address.1);
        let address: Vec<_> = address
            .to_socket_addrs()
            .await
            .map_err(|e| format!("Failed to get the address for {}: {:?}", coord, e))
            .unwrap()
            .collect();
        let mut retry_delay = RETRY_INITIAL_TIMEOUT;
        for attempt in 1..=CONNECT_ATTEMPTS {
            debug!(
                "Attempt {} to connect to {} at {:?}",
                attempt, coord, address
            );
            let connect_fut = TcpStream::connect(&*address);
            match timeout(CONNECT_TIMEOUT, connect_fut).await {
                Ok(stream) => {
                    NetworkSender::handle_remote_connection(coord, local_receiver, stream).await;
                    return;
                }
                Err(err) => {
                    match err.kind() {
                        ErrorKind::TimedOut => {
                            debug!(
                                "Timeout connecting to {} at {:?}, retry in {}s",
                                coord,
                                address,
                                retry_delay.as_secs_f32(),
                            );
                        }
                        _ => {
                            debug!(
                                "Failed to connect to {} at {:?}, retry in {}s: {:?}",
                                coord,
                                address,
                                retry_delay.as_secs_f32(),
                                err
                            );
                        }
                    }
                    sleep(retry_delay).await;
                    retry_delay = (2 * retry_delay).min(RETRY_MAX_TIMEOUT);
                }
            };
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
    async fn handle_remote_connection(
        coord: Coord,
        local_receiver: Receiver<Out>,
        mut stream: TcpStream,
    ) {
        let address = stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        debug!("Connection to {} at {} established", coord, address);
        while let Ok(out) = local_receiver.recv().await {
            remote_send(out, coord, &mut stream).await;
        }
        let _ = stream.shutdown(Shutdown::Both);
        debug!("Remote sender for {} exited", coord);
    }
}
