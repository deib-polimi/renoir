use std::collections::HashMap;
use std::io::Write;
use std::io::{ErrorKind, Read};
use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender};
use std::thread::{sleep, JoinHandle};
use std::time::Duration;

use anyhow::{anyhow, Result};
use bincode::config::{FixintEncoding, RejectTrailing, WithOtherIntEncoding, WithOtherTrailing};
use bincode::{DefaultOptions, Options};
use serde::{Deserialize, Serialize};

use crate::config::RemoteRuntimeConfig;
use crate::network::{BlockCoord, Coord, ReceiverEndpoint};
use crate::operator::Data;
use crate::scheduler::ReplicaId;
use crate::stream::BlockId;

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

lazy_static! {
/// Configuration of the header serializer: the integers must have a fixed length encoding.
    static ref HEADER_CONFIG: WithOtherTrailing<WithOtherIntEncoding<DefaultOptions, FixintEncoding>, RejectTrailing> =
        bincode::DefaultOptions::new().with_fixint_encoding().reject_trailing_bytes();
}

lazy_static! {
/// Configuration of the message serializer
    static ref MESSAGE_CONFIG: DefaultOptions = bincode::DefaultOptions::new();
}

/// Header of a message sent before the actual message.
#[derive(Serialize, Deserialize, Default)]
struct MessageHeader {
    /// The size of the actual message
    size: u32,
    /// The id of the replica this message is for.
    replica_id: ReplicaId,
    /// The id of the block that is sending the message.
    sender_block_id: BlockId,
}

/// Like `NetworkSender`, but this should be used in a multiplexed channel (i.e. a remote one).
///
/// With the actual message the receiver endpoint is sent in order to demultiplex the message.
#[derive(Debug, Clone)]
pub(crate) struct MultiplexingSender<Out: Data> {
    /// The internal sender that points to the actual multiplexed channel.
    sender: SyncSender<(ReceiverEndpoint, Out)>,
}

/// Like `NetworkReceiver`, but this should be used in a multiplexed channel (i.e. a remote one).
///
/// This receiver is handled in a separate thread that keeps track of the local registered receivers
/// and the open connections. The incoming messages are tagged with the receiver endpoint. Upon
/// arrival they are routed to the correct receiver if it's registered, otherwise the receiver waits
/// for the registration of the local receiver.
#[derive(Debug)]
pub(crate) struct DemultiplexingReceiver<In: Data> {
    register_receiver: Sender<(ReceiverEndpoint, SyncSender<In>)>,
}

impl<Out: Data> MultiplexingSender<Out> {
    /// Construct a new `MultiplexingSender` for a block.
    ///
    /// All the replicas of this block should point to this multiplexer (or one of its clones).
    pub fn new(
        coord: BlockCoord,
        remote_runtime_config: &RemoteRuntimeConfig,
    ) -> (Self, JoinHandle<()>) {
        let address = coord.address(remote_runtime_config);
        let (sender, receiver) = sync_channel(CHANNEL_CAPACITY);
        let join_handle = std::thread::Builder::new()
            .name(format!("NetSender{}", coord))
            .spawn(move || Self::connect_remote(coord, address, receiver))
            .unwrap();
        (Self { sender }, join_handle)
    }

    /// Send a message to the channel.
    ///
    /// Unlikely the normal channels, the destination is required since the channel is multiplexed.
    pub fn send(&self, destination: ReceiverEndpoint, message: Out) -> Result<()> {
        self.sender
            .send((destination, message))
            .map_err(|e| anyhow!("Failed to send to channel to {}: {:?}", destination, e))
    }

    /// Connect the sender to a remote channel located at the specified address.
    ///
    /// - At first the address is resolved to an actual address (DNS resolution)
    /// - Then at most `CONNECT_ATTEMPTS` are performed, and an exponential backoff is used in case
    ///   of errors.
    /// - If the connection cannot be established this function will panic.
    fn connect_remote(
        coord: BlockCoord,
        address: (String, u16),
        local_receiver: Receiver<(ReceiverEndpoint, Out)>,
    ) {
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
                        Self::handle_remote_connection(coord, local_receiver, stream);
                        return;
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
    fn handle_remote_connection(
        coord: BlockCoord,
        local_receiver: Receiver<(ReceiverEndpoint, Out)>,
        mut stream: TcpStream,
    ) {
        let address = stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        debug!("Connection to {} at {} established", coord, address);
        while let Ok((dest, message)) = local_receiver.recv() {
            remote_send(message, dest, &mut stream);
        }
        let _ = stream.shutdown(Shutdown::Both);
        debug!("Remote sender for {} exited", coord);
    }
}

impl<In: Data> DemultiplexingReceiver<In> {
    /// Construct a new `DemultiplexingReceiver` for a block.
    ///
    /// All the local replicas of this block should be registered to this demultiplexer.
    /// `num_client` is the number of multiplexers that will connect to this demultiplexer. Since
    /// the remote senders are all multiplexed this corresponds to the number of previous blocks
    /// inside remote hosts.
    pub fn new(
        coord: BlockCoord,
        remote_runtime_config: &RemoteRuntimeConfig,
        num_clients: usize,
    ) -> (Self, JoinHandle<()>) {
        let (sender, receiver) = channel();
        let address = coord.address(remote_runtime_config);
        let join_handle = std::thread::Builder::new()
            .name(format!("NetRecv{}", coord))
            .spawn(move || Self::bind_remote(coord, address, receiver, num_clients))
            .unwrap();
        (
            Self {
                register_receiver: sender,
            },
            join_handle,
        )
    }

    /// Register a local receiver to this demultiplexer.
    pub fn register(&self, receiver_endpoint: ReceiverEndpoint, local_sender: SyncSender<In>) {
        self.register_receiver
            .send((receiver_endpoint, local_sender))
            .unwrap();
    }

    /// Bind the socket of this demultiplexer.
    fn bind_remote(
        coord: BlockCoord,
        address: (String, u16),
        register_receiver: Receiver<(ReceiverEndpoint, SyncSender<In>)>,
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
            "Remote receiver at {} is ready to accept connections to {}",
            coord, address
        );

        let (message_sender, message_receiver) = sync_channel(CHANNEL_CAPACITY);
        // spawn an extra thread that keeps track of the connected clients and registered receivers
        let join_handle = std::thread::Builder::new()
            .name(format!("Demux{}", coord))
            .spawn(move || Self::demultiplexer_thread(message_receiver, register_receiver))
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
                .name(format!("NetClient{}", coord))
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

    /// Demultiplexer thread that routes the incoming messages to the registered local receivers.
    fn demultiplexer_thread(
        message_receiver: Receiver<(ReceiverEndpoint, In)>,
        register_receiver: Receiver<(ReceiverEndpoint, SyncSender<In>)>,
    ) {
        let mut known_receivers = HashMap::new();
        while let Ok((dest, message)) = message_receiver.recv() {
            // a message arrived to a not-yet-registered local receiver, wait for the missing
            // receiver
            while !known_receivers.contains_key(&dest) {
                let (dest, sender) = register_receiver.recv().unwrap();
                known_receivers.insert(dest, sender);
            }
            if let Err(e) = known_receivers[&dest].send(message) {
                warn!("Failed to send message to {}: {:?}", dest, e);
            }
        }
    }

    /// Handle the connection with a remote sender.
    ///
    /// This will receive every message, deserialize it and send it to the demultiplexer using the
    /// local channel.
    fn handle_remote_client(
        coord: BlockCoord,
        local_sender: SyncSender<(ReceiverEndpoint, In)>,
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

/// Serialize and send a message to a remote socket.
///
/// The network protocol works as follow:
/// - send a `MessageHeader` serialized with bincode with `FixintEncoding`
/// - send the message
pub(crate) fn remote_send<T: Data>(what: T, dest: ReceiverEndpoint, stream: &mut TcpStream) {
    let address = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let serialized = MESSAGE_CONFIG
        .serialize(&what)
        .unwrap_or_else(|e| panic!("Failed to serialize outgoing message to {}: {:?}", dest, e));
    let header = MessageHeader {
        size: serialized.len() as _,
        replica_id: dest.coord.replica_id,
        sender_block_id: dest.prev_block_id,
    };
    let serialized_header = HEADER_CONFIG.serialize(&header).unwrap();
    stream.write_all(&serialized_header).unwrap_or_else(|e| {
        panic!(
            "Failed to send size of message (was {} bytes) to {} at {}: {:?}",
            serialized.len(),
            dest,
            address,
            e
        )
    });

    stream.write_all(&serialized).unwrap_or_else(|e| {
        panic!(
            "Failed to send {} bytes to {} at {}: {:?}",
            serialized.len(),
            dest,
            address,
            e
        )
    });
}

/// Receive a message from the remote channel. Returns `None` if there was a failure receiving the
/// last message.
pub(crate) fn remote_recv<T: Data>(
    coord: BlockCoord,
    stream: &mut TcpStream,
) -> Option<(ReceiverEndpoint, T)> {
    let address = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let header_size = HEADER_CONFIG
        .serialized_size(&MessageHeader::default())
        .unwrap() as usize;
    let mut header = vec![0u8; header_size];
    match stream.read_exact(&mut header) {
        Ok(_) => {}
        Err(e) => {
            debug!(
                "Failed to receive {} bytes of header to {} from {}: {:?}",
                header_size, coord, address, e
            );
            return None;
        }
    }
    let header: MessageHeader = HEADER_CONFIG
        .deserialize(&header)
        .expect("Malformed header");
    let mut buf = vec![0u8; header.size as usize];
    stream.read_exact(&mut buf).unwrap_or_else(|e| {
        panic!(
            "Failed to receive {} bytes to {} from {}: {:?}",
            header.size, coord, address, e
        )
    });
    let receiver_endpoint = ReceiverEndpoint::new(
        Coord::new(coord.block_id, coord.host_id, header.replica_id),
        header.sender_block_id,
    );
    Some((
        receiver_endpoint,
        MESSAGE_CONFIG.deserialize(&buf).unwrap_or_else(|e| {
            panic!(
                "Failed to deserialize {} bytes to {} from {}: {:?}",
                header.size, coord, address, e
            )
        }),
    ))
}
