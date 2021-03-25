use std::collections::HashMap;
use std::marker::PhantomData;
use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender};
use std::thread::JoinHandle;

use anyhow::anyhow;
use derivative::Derivative;
use typemap::{Key, SendMap, TypeMap};

use crate::config::RemoteRuntimeConfig;
use crate::network::remote::{deserialize, remote_recv, SerializedMessage, CHANNEL_CAPACITY};
use crate::network::{BlockCoord, ReceiverEndpoint};
use crate::operator::Data;

/// This struct is used to index inside the `typemap` with the `NetworkReceiver`s.
struct TypedDemultiplexingReceiverKey<In: Data>(PhantomData<In>);

impl<In: Data> Key for TypedDemultiplexingReceiverKey<In> {
    type Value = TypedDemultiplexingReceiver<In>;
}

/// Like `NetworkReceiver`, but this should be used in a multiplexed channel (i.e. a remote one).
///
/// This receiver is handled in a separate thread that keeps track of the local registered receivers
/// and the open connections. The incoming messages are tagged with the receiver endpoint. Upon
/// arrival they are routed to the correct `TypedDemultiplexingReceiver` if it's registered,
/// otherwise the receiver waits for the registration of the local receiver.
///
/// This second redirection is required to perform the deserialization of the incoming messages that
/// may be of different types depending on the destination.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct DemultiplexingReceiver {
    /// The coordinate of the block this demultiplexer refers to.
    coord: BlockCoord,
    /// Tell the demultiplexer thread that a new receiver is present. The receiver refers to an
    /// instance of `TypedDemultiplexingReceiver` that internally will refer to the actual receiver
    /// of a replica.
    register_receiver: Sender<(
        ReceiverEndpoint,
        SyncSender<(ReceiverEndpoint, SerializedMessage)>,
    )>,
    /// The set of `TypedDemultiplexingReceiver`s, for all the registered types. Since those
    /// receivers have different types they cannot be simply stored inside a map.
    ///
    /// This `typemap` is indexed via `TypedDemultiplexingReceiverKey`.
    #[derivative(Debug = "ignore")]
    typed_demuxes: SendMap,
}

/// A demultiplexer that takes care of the deserialization of the messages and their forwarding to
/// the correct receiver based on the `ReceiverEndpoint` they're tagged with.
///
/// Internally this spawns a thread for handling the incoming messages and their deserialization.
struct TypedDemultiplexingReceiver<In: Data> {
    /// Tell the demultiplexer that a new receiver is present,
    register_receiver: Sender<(ReceiverEndpoint, SyncSender<In>)>,
    /// Tell the demultiplexer that a new message has arrived.
    serialized_sender: SyncSender<(ReceiverEndpoint, SerializedMessage)>,
}

impl DemultiplexingReceiver {
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
                coord,
                register_receiver: sender,
                typed_demuxes: TypeMap::custom(),
            },
            join_handle,
        )
    }

    /// Register a local receiver to this demultiplexer.
    ///
    /// This may spawn a new `TypedDemultiplexingReceiver`, in this case its `JoinHandle` is
    /// returned.
    pub fn register<In: Data>(
        &mut self,
        receiver_endpoint: ReceiverEndpoint,
        local_sender: SyncSender<In>,
    ) -> Option<JoinHandle<()>> {
        debug!(
            "Registering {} to the demultiplexer of {}",
            receiver_endpoint, self.coord
        );
        let join_handle = if !self
            .typed_demuxes
            .contains::<TypedDemultiplexingReceiverKey<In>>()
        {
            // spawn the `TypedDemultiplexingReceiver` for the `In` type.
            let (demux, join_handle) = TypedDemultiplexingReceiver::new(self.coord);
            self.typed_demuxes
                .insert::<TypedDemultiplexingReceiverKey<In>>(demux);
            Some(join_handle)
        } else {
            None
        };
        let typed_demux = self
            .typed_demuxes
            .get_mut::<TypedDemultiplexingReceiverKey<In>>()
            .unwrap();
        typed_demux
            .register_receiver
            .send((receiver_endpoint, local_sender))
            .unwrap();
        self.register_receiver
            .send((receiver_endpoint, typed_demux.serialized_sender.clone()))
            .unwrap();
        join_handle
    }

    /// Bind the socket of this demultiplexer.
    fn bind_remote(
        coord: BlockCoord,
        address: (String, u16),
        register_receiver: Receiver<(
            ReceiverEndpoint,
            SyncSender<(ReceiverEndpoint, SerializedMessage)>,
        )>,
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

    /// Demultiplexer thread that routes the incoming messages to the registered
    /// `TypedDemultiplexingReceiver` for the right receiver.
    fn demultiplexer_thread(
        coord: BlockCoord,
        message_receiver: Receiver<(ReceiverEndpoint, SerializedMessage)>,
        register_receiver: Receiver<(
            ReceiverEndpoint,
            SyncSender<(ReceiverEndpoint, SerializedMessage)>,
        )>,
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
            if let Err(e) = known_receivers[&dest].send((dest, message)) {
                warn!("Failed to send message to {}: {:?}", dest, e);
            }
        }
    }

    /// Handle the connection with a remote sender.
    ///
    /// This will receive every message and send it to the demultiplexer using the local channel.
    fn handle_remote_client(
        coord: BlockCoord,
        local_sender: SyncSender<(ReceiverEndpoint, SerializedMessage)>,
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

impl<In: Data> TypedDemultiplexingReceiver<In> {
    fn new(coord: BlockCoord) -> (Self, JoinHandle<()>) {
        let (register_receiver, register_receiver_rx) = channel();
        let (serialized_sender, serialized_receiver) = sync_channel(CHANNEL_CAPACITY);
        let join_handle = std::thread::Builder::new()
            .name(format!("TypDemux{}", coord))
            .spawn(move || {
                Self::demultiplexer_thread(coord, serialized_receiver, register_receiver_rx)
            })
            .unwrap();
        (
            Self {
                register_receiver,
                serialized_sender,
            },
            join_handle,
        )
    }

    /// Demultiplexer thread that routes the incoming messages to the registered receiver.
    fn demultiplexer_thread(
        coord: BlockCoord,
        serialized_receiver: Receiver<(ReceiverEndpoint, SerializedMessage)>,
        register_receiver: Receiver<(ReceiverEndpoint, SyncSender<In>)>,
    ) {
        debug!(
            "Starting demultiplexer for {} of type {}",
            coord,
            std::any::type_name::<In>()
        );
        let mut known_receivers = HashMap::new();
        while let Ok((dest, message)) = serialized_receiver.recv() {
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
}
