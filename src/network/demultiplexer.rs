use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::thread::JoinHandle;

use ahash::AHashMap;
use anyhow::anyhow;

use crate::channel::{Sender, UnboundedReceiver, UnboundedSender, self};
use crate::network::remote::{deserialize, header_size, remote_recv};
use crate::network::{DemuxCoord, NetworkMessage, ReceiverEndpoint};
use crate::operator::ExchangeData;
use crate::profiler::{get_profiler, Profiler};

/// Channel and its coordinate pointing to a local block.
type ReceiverEndpointMessageSender<In> =
    (ReceiverEndpoint, Sender<NetworkMessage<In>>);

/// Like `NetworkReceiver`, but this should be used in a multiplexed channel (i.e. a remote one).
///
/// This receiver is handled in a separate thread that keeps track of the local registered receivers
/// and the open connections. The incoming messages are tagged with the receiver endpoint. Upon
/// arrival they are routed to the correct receiver according to the `ReceiverEndpoint` the message
/// is tagged with.
#[derive(Debug)]
pub(crate) struct DemultiplexingReceiver<In: ExchangeData> {
    /// The coordinate of this demultiplexer.
    coord: DemuxCoord,
    /// Tell the demultiplexer that a new receiver is present,
    registry_tx: UnboundedSender<RegistryMessage<In>>,
}

/// Message sent to the demultiplexer thread.
///
/// This message will be sent to that thread by `register` (with `RegisterReceiverEndpoint`)
/// signaling that a new recipient is ready. When a remote multiplexer connects,
/// `RegisterRemoteClient` is sent with the sender to that thread.
#[derive(Debug, Clone)]
enum RegistryMessage<In: ExchangeData> {
    /// A new local replica has been registered, the demultiplexer will inform all the deserializing
    /// threads of this new `ReceiverEndpoint`.
    RegisterEndpoint(ReceiverEndpointMessageSender<In>),
    /// A new remote client has been connected, the demultiplexer will send all the registered
    /// replicas and all the replicas that will register from now on.
    RegisterRemoteClient(UnboundedSender<ReceiverEndpointMessageSender<In>>),
}

impl<In: ExchangeData> DemultiplexingReceiver<In> {
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
        let (registry_tx, registry_rx) = channel::unbounded();
        let reg_tx = registry_tx.clone();
        let join_handle = std::thread::Builder::new()
            .name(format!("Net{}", coord))
            .spawn(move || {
                Self::bind_remote(coord, address, reg_tx, registry_rx, num_clients)
            })
            .unwrap();
        (
            Self {
                coord,
                registry_tx,
            },
            join_handle,
        )
    }

    /// Register a local receiver to this demultiplexer.
    pub fn register(
        &mut self,
        receiver_endpoint: ReceiverEndpoint,
        local_sender: Sender<NetworkMessage<In>>,
    ) {
        debug!(
            "Registering {} to the demultiplexer of {}",
            receiver_endpoint, self.coord
        );
        self.registry_tx
            .send(RegistryMessage::RegisterEndpoint((
                receiver_endpoint,
                local_sender,
            )))
            .unwrap_or_else(|_| panic!("Register received for {:?} failed", self.coord))
    }

    /// Bind the socket of this demultiplexer.
    fn bind_remote(
        coord: DemuxCoord,
        address: (String, u16),
        registry_tx: UnboundedSender<RegistryMessage<In>>,
        registry_rx: UnboundedReceiver<RegistryMessage<In>>,
        num_clients: usize,
    ) {
        let address = (address.0.as_ref(), address.1);
        let address: Vec<_> = address
            .to_socket_addrs()
            .map_err(|e| format!("Failed to get the address for {}: {:?}", coord, e))
            .unwrap()
            .collect();

        tracing::debug!("demux binding {}", address[0]);
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

        // spawn an extra thread that keeps track of the connected clients and registered receivers
        let join_handle = std::thread::Builder::new()
            .name(format!("{}", coord))
            .spawn(move || Self::registry_thread(coord, registry_rx))
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

            let (client_tx_endpoints, client_rx_endpoints) =
                channel::unbounded();
            registry_tx
                .send(RegistryMessage::RegisterRemoteClient(
                    client_tx_endpoints,
                ))
                .unwrap();
            let join_handle = std::thread::Builder::new()
                .name(format!("Client{}", coord))
                .spawn(move || {
                    Self::demux_thread(coord, client_rx_endpoints, stream)
                })
                .unwrap();
            join_handles.push(join_handle);
        }
        debug!(
            "All connection to {} started, waiting for them to finish",
            coord
        );
        // make sure the demultiplexer thread can exit
        drop(registry_tx);
        for handle in join_handles {
            handle.join().unwrap();
        }
        debug!("Demultiplexer of {} finished", coord);
    }

    /// The body of the thread that will broadcast the sender of the local replicas to all the
    /// deserializing threads.
    fn registry_thread(
        coord: DemuxCoord,
        receiver: UnboundedReceiver<RegistryMessage<In>>,
    ) {
        debug!("Starting demultiplexer registry for {}", coord);
        let mut known_receivers: Vec<ReceiverEndpointMessageSender<In>> = Vec::new();
        let mut clients = Vec::new();
        while let Ok(message) = receiver.recv() {
            match message {
                RegistryMessage::RegisterRemoteClient(client) => {
                    for recv in &known_receivers {
                        client.send(recv.clone()).unwrap();
                    }
                    clients.push(client);
                }
                RegistryMessage::RegisterEndpoint(recv) => {
                    for client in &clients {
                        client.send(recv.clone()).unwrap();
                    }
                    known_receivers.push(recv);
                }
            }
        }
    }

    /// Handle the connection with a remote sender.
    ///
    /// Will deserialize the message upon arrival and send to the corresponding recipient the
    /// deserialized data. If the recipient is not yet known, it is waited until it registers.
    /// 
    /// # Upgrade path
    /// 
    /// Replace send with queue.
    /// 
    /// The queue uses a hierarchical queue:
    /// + First try to reserve and put the value in the fast queue
    /// + If the fast queue is full, put in the slow (unbounded?) queue
    /// + Return an enum, either Queued or Overflowed
    /// 
    /// if overflowed send a yield request through a second channel

    fn demux_thread(
        coord: DemuxCoord,
        register_receiver: UnboundedReceiver<(
            ReceiverEndpoint,
            Sender<NetworkMessage<In>>,
        )>,
        mut receiver: TcpStream,
    ) {
        let address = receiver
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        let mut known_receivers = AHashMap::new();
        while let Some((dest, message)) = remote_recv(coord, &mut receiver) {
            // a message arrived to a not-yet-registered local receiver, wait for the missing
            // receiver
            while !known_receivers.contains_key(&dest) {
                let (dest, sender) = register_receiver.recv().unwrap();
                known_receivers.insert(dest, sender);
            }
            let message_len = message.len();
            let message = deserialize::<NetworkMessage<In>>(message).unwrap();
            get_profiler().net_bytes_in(message.sender, dest.coord, header_size() + message_len);
            
            if let Err(e) = known_receivers[&dest].send(message) {
                warn!("Failed to send message to {}: {:?}", dest, e);
            }
        }

        let _ = receiver.shutdown(Shutdown::Both);
        debug!("Remote receiver for {} at {} exited", coord, address);
    }
}
