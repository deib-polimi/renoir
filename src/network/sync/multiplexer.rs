use std::io::ErrorKind;
use std::time::Duration;

use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::thread::{sleep, JoinHandle};

use crate::channel::{self, Receiver, Sender};
use crate::network::remote::remote_send;
use crate::network::{DemuxCoord, NetworkMessage, ReceiverEndpoint};
use crate::operator::ExchangeData;

//
// use crate::channel::Selector;

use crate::network::NetworkSender;

/// Maximum number of attempts to make for connecting to a remote host.
const CONNECT_ATTEMPTS: usize = 32;
/// Timeout for connecting to a remote host.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// To avoid spamming the connections, wait this timeout before trying again. If the connection
/// fails again this timeout will be doubled up to `RETRY_MAX_TIMEOUT`.
const RETRY_INITIAL_TIMEOUT: Duration = Duration::from_millis(8);
/// Maximum timeout between connection attempts.
const RETRY_MAX_TIMEOUT: Duration = Duration::from_secs(1);

const MUX_CHANNEL_CAPACITY: usize = 10;
/// Like `NetworkSender`, but this should be used in a multiplexed channel (i.e. a remote one).
///
/// The `ReceiverEndpoint` is sent alongside the actual message in order to demultiplex it.
#[derive(Debug)]
pub struct MultiplexingSender<Out: ExchangeData> {
    tx: Option<Sender<(ReceiverEndpoint, NetworkMessage<Out>)>>,
}

impl<Out: ExchangeData> MultiplexingSender<Out> {
    pub fn new(coord: DemuxCoord, address: (String, u16)) -> (Self, JoinHandle<()>) {
        let (tx, rx) = channel::bounded(MUX_CHANNEL_CAPACITY);

        let join_handle = std::thread::Builder::new()
            .name(format!(
                "mux-{}:{}-{}",
                coord.coord.host_id, coord.prev_block_id, coord.coord.block_id
            ))
            .spawn(move || {
                log::debug!(
                    "mux {coord} connecting to {}",
                    address.to_socket_addrs().unwrap().next().unwrap()
                );
                let stream = connect_remote(coord, address);

                mux_thread::<Out>(coord, rx, stream);
            })
            .unwrap();
        (Self { tx: Some(tx) }, join_handle)
    }

    pub(crate) fn get_sender(&mut self, receiver_endpoint: ReceiverEndpoint) -> NetworkSender<Out> {
        crate::network::mux_sender(receiver_endpoint, self.tx.as_ref().unwrap().clone())
    }
}

/// Connect the sender to a remote channel located at the specified address.
///
/// - At first the address is resolved to an actual address (DNS resolution)
/// - Then at most `CONNECT_ATTEMPTS` are performed, and an exponential backoff is used in case
///   of errors.
/// - If the connection cannot be established this function will panic.
fn connect_remote(coord: DemuxCoord, address: (String, u16)) -> TcpStream {
    let socket_addrs: Vec<_> = address
        .to_socket_addrs()
        .map_err(|e| format!("Failed to get the address for {coord}: {e:?}",))
        .unwrap()
        .collect();
    let mut retry_delay = RETRY_INITIAL_TIMEOUT;
    for attempt in 1..=CONNECT_ATTEMPTS {
        log::debug!(
            "{} connecting to {:?} ({} attempt)",
            coord,
            socket_addrs,
            attempt,
        );

        for address in socket_addrs.iter() {
            match TcpStream::connect_timeout(address, CONNECT_TIMEOUT) {
                Ok(stream) => {
                    return stream;
                }
                Err(err) => match err.kind() {
                    ErrorKind::TimedOut => {
                        log::debug!("{coord} timeout connecting to {address:?}");
                    }
                    ErrorKind::ConnectionRefused => {
                        log::log!(
                            if attempt > 4 {
                                log::Level::Warn
                            } else {
                                log::Level::Debug
                            },
                            "{coord} connection refused connecting to {address:?} ({attempt})"
                        );
                    }
                    _ => {
                        log::warn!("{coord} failed to connect to {address:?}: {err:?}");
                    }
                },
            }
        }

        log::debug!(
            "{coord} retrying connection to {socket_addrs:?} in {}s",
            retry_delay.as_secs_f32(),
        );

        sleep(retry_delay);
        retry_delay = (2 * retry_delay).min(RETRY_MAX_TIMEOUT);
    }
    panic!("Failed to connect to remote {coord} at {address:?} after {CONNECT_ATTEMPTS} attempts",);
}

fn mux_thread<Out: ExchangeData>(
    coord: DemuxCoord,
    rx: Receiver<(ReceiverEndpoint, NetworkMessage<Out>)>,
    mut stream: TcpStream,
) {
    use std::io::Write;

    let address = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    log::debug!("{} connected to {:?}", coord, address);

    // let mut w = std::io::BufWriter::new(&mut stream);
    let mut w = &mut stream;
    let mut scratch = Vec::new();

    while let Ok((dest, message)) = rx.recv() {
        remote_send(message, dest, &mut w, &mut scratch, &address);
    }

    w.flush().unwrap();
    let _ = stream.shutdown(Shutdown::Both);
    log::debug!("{} finished", coord);
}
