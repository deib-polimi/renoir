use std::io::ErrorKind;
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::thread::{sleep, JoinHandle};
use std::time::Duration;

use crate::channel::{self, Receiver, SendError, Sender, RecvError, UnboundedSender, Selector};
use crate::network::remote::{remote_send, CHANNEL_CAPACITY};
use crate::network::{DemuxCoord, NetworkMessage, ReceiverEndpoint};
use crate::operator::ExchangeData;

use super::NetworkSender;

/// Maximum number of attempts to make for connecting to a remote host.
const CONNECT_ATTEMPTS: usize = 4;
/// Timeout for connecting to a remote host.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(4);
/// To avoid spamming the connections, wait this timeout before trying again. If the connection
/// fails again this timeout will be doubled up to `RETRY_MAX_TIMEOUT`.
const RETRY_INITIAL_TIMEOUT: Duration = Duration::from_millis(125);
/// Maximum timeout between connection attempts.
const RETRY_MAX_TIMEOUT: Duration = Duration::from_secs(1);

/// Like `NetworkSender`, but this should be used in a multiplexed channel (i.e. a remote one).
///
/// The `ReceiverEndpoint` is sent alongside the actual message in order to demultiplex it.
#[derive(Debug)]
pub struct MultiplexingSender<Out: ExchangeData> {
    /// The internal sender that points to the actual multiplexed channel.
    // sender: Sender<(ReceiverEndpoint, NetworkMessage<Out>)>,
    tx: UnboundedSender<(ReceiverEndpoint, Receiver<NetworkMessage<Out>>)>
}

impl<Out: ExchangeData> MultiplexingSender<Out> {
    /// Construct a new `MultiplexingSender` for a block.
    ///
    /// All the replicas of this block should point to this multiplexer (or one of its clones).
    pub fn new(coord: DemuxCoord, address: (String, u16)) -> (Self, JoinHandle<()>) {
        let (tx, rx) = channel::unbounded();
        let join_handle = std::thread::Builder::new()
            .name(format!("noir-mux-{}", coord))
            .spawn(move || {
                let mut receivers = Vec::new();
                loop {
                    match rx.recv() {
                        Ok(t) => receivers.push(t),
                        Err(RecvError::Disconnected) => break,
                    }
                }
                let stream = connect_remote(coord, address);
                mux_thread::<Out>(coord, receivers, stream);
            })
            .unwrap();
        (Self { tx }, join_handle)
    }

    /// Send a message to the channel.
    ///
    /// Unlikely the normal channels, the destination is required since the channel is multiplexed.
    // pub fn send(
    //     &self,
    //     destination: ReceiverEndpoint,
    //     message: NetworkMessage<Out>,
    // ) -> Result<(), SendError<(ReceiverEndpoint, NetworkMessage<Out>)>> {
    //     self.sender.send((destination, message))
    // }

    pub(crate) fn get_sender(&mut self, receiver_endpoint: ReceiverEndpoint) -> NetworkSender<Out> {
        let (sender, receiver) = channel::bounded(CHANNEL_CAPACITY);
        self.tx.send((receiver_endpoint, receiver)).unwrap();
        NetworkSender{ receiver_endpoint, sender }
    }
}

/// Connect the sender to a remote channel located at the specified address.
///
/// - At first the address is resolved to an actual address (DNS resolution)
/// - Then at most `CONNECT_ATTEMPTS` are performed, and an exponential backoff is used in case
///   of errors.
/// - If the connection cannot be established this function will panic.
fn connect_remote(
    coord: DemuxCoord,
    address: (String, u16),
) -> TcpStream {
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
                    return stream;
                }
                Err(err) => match err.kind() {
                    ErrorKind::TimedOut => {
                        debug!("Timeout connecting to {} at {:?}", coord, address);
                    }
                    _ => {
                        debug!("Failed to connect to {} at {}: {:?}", coord, address, err);
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
///
/// # Upgrade path
///
/// Instead of using a single mpsc channel, use multiple channels, one per block
/// Use a fair (round robin?) selection from each channel when sending
///
/// Before popping from a channel, check that a Yield request was not received for that block
/// In that case, do not pop from the channel and only select from others
/// (this handles backpressure since the sender will block when the channel is full)
/// If a Resume request was received then allow popping from the channel
fn mux_thread<Out: ExchangeData>(
    coord: DemuxCoord,
    receivers: Vec<(ReceiverEndpoint, Receiver<NetworkMessage<Out>>)>,
    mut stream: TcpStream,
) {
    let address = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    debug!("Connection to {} at {} established", coord, address);

    let mut selector = Selector::new(receivers);

    while let Ok((dest, message)) = selector.recv() {
        remote_send(message, dest, &mut stream);
    }
    let _ = stream.shutdown(Shutdown::Both);
    debug!("Remote sender for {} exited", coord);
}