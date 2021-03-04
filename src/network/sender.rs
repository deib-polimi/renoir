use anyhow::{anyhow, Result};
use async_std::channel::{bounded, Receiver, Sender};

use crate::network::{wait_start, Coord, NetworkStarter, NetworkStarterRecv};
use async_std::io::timeout;
use async_std::net::{TcpStream, ToSocketAddrs};
use async_std::task::spawn;
use async_std::task::{sleep, JoinHandle};
use std::io::ErrorKind;
use std::time::Duration;

const CONNECT_ATTEMPTS: usize = 10;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const RETRY_INITIAL_TIMEOUT: Duration = Duration::from_millis(50);
const RETRY_MAX_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct NetworkSender<Out> {
    pub coord: Coord,
    #[derivative(Debug = "ignore")]
    sender: Sender<Out>,
}

impl<Out> NetworkSender<Out>
where
    Out: Send + 'static,
{
    /// Store the sender of a local channel. No connection is required since the channel born
    /// connected.
    pub fn local(coord: Coord, sender: Sender<Out>) -> Self {
        Self { coord, sender }
    }

    pub fn remote<A>(coord: Coord, address: A) -> (Self, NetworkStarter, JoinHandle<()>)
    where
        A: ToSocketAddrs + Send + Sync + 'static,
        <A as ToSocketAddrs>::Iter: Send,
    {
        let (sender, receiver) = bounded(1);
        let (connect_socket, connect_socket_rx) = bounded(1);
        let join_handle = spawn(async move {
            NetworkSender::connect_remote(coord, receiver, address, connect_socket_rx).await;
        });
        (Self { coord, sender }, connect_socket, join_handle)
    }

    pub async fn send(&self, item: Out) -> Result<()> {
        self.sender
            .send(item)
            .await
            .map_err(|e| anyhow!("Failed to send to local channel to {}: {:?}", self.coord, e))
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
        address: impl ToSocketAddrs,
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
                    NetworkSender::handle_remote_client(coord, local_receiver, stream).await;
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

    async fn handle_remote_client(
        _coord: Coord,
        _local_receiver: Receiver<Out>,
        _stream: TcpStream,
    ) {
        todo!("receive message, serialize data and send it")
    }
}
