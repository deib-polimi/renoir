use std::collections::HashMap;
use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::thread::JoinHandle;

use anyhow::anyhow;

use crate::channel::{BoundedChannelSender, UnboundedChannelReceiver, UnboundedChannelSender};
use crate::network::remote::{deserialize, header_size, remote_recv};
use crate::network::{DemuxCoord, NetworkMessage, ReceiverEndpoint};
use crate::operator::Data;
use crate::profiler::{get_profiler, Profiler};

/// Channel and its coordinate pointing to a local block.
type ReceiverEndpointMessageSender<In> =
    (ReceiverEndpoint, BoundedChannelSender<NetworkMessage<In>>);

/// Like `NetworkReceiver`, but this should be used in a multiplexed channel (i.e. a remote one).
///
/// This receiver is handled in a separate thread that keeps track of the local registered receivers
/// and the open connections. The incoming messages are tagged with the receiver endpoint. Upon
/// arrival they are routed to the correct receiver according to the `ReceiverEndpoint` the message
/// is tagged with.
#[derive(Debug)]
pub(crate) struct DemultiplexingReceiver<In: Data> {
    /// The coordinate of this demultiplexer.
    coord: DemuxCoord,
    /// Tell the demultiplexer that a new receiver is present,
    register_receiver: UnboundedChannelSender<DemultiplexerMessage<In>>,
}

/// Message sent to the demultiplexer thread.
///
/// This message will be sent to that thread by `register` (with `RegisterReceiverEndpoint`)
/// signaling that a new recipient is ready. When a remote multiplexer connects,
/// `RegisterRemoteClient` is sent with the sender to that thread.
#[derive(Debug, Clone)]
enum DemultiplexerMessage<In: Data> {
    /// A new local replica has been registered, the demultiplexer will inform all the deserializing
    /// threads of this new `ReceiverEndpoint`.
    RegisterReceiverEndpoint(ReceiverEndpointMessageSender<In>),
    /// A new remote client has been connected, the demultiplexer will send all the registered
    /// replicas and all the replicas that will register from now on.
    RegisterRemoteClient(UnboundedChannelSender<ReceiverEndpointMessageSender<In>>),
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
        let (demux_sender, demux_receiver) = UnboundedChannelReceiver::new();
        let register_receiver = demux_sender.clone();
        let join_handle = std::thread::Builder::new()
            .name(format!("Net{}", coord))
            .spawn(move || {
                Self::bind_remote(coord, address, demux_sender, demux_receiver, num_clients)
            })
            .unwrap();
        (
            Self {
                coord,
                register_receiver,
            },
            join_handle,
        )
    }

    /// Register a local receiver to this demultiplexer.
    pub fn register(
        &mut self,
        receiver_endpoint: ReceiverEndpoint,
        local_sender: BoundedChannelSender<NetworkMessage<In>>,
    ) {
        debug!(
            "Registering {} to the demultiplexer of {}",
            receiver_endpoint, self.coord
        );
        self.register_receiver
            .send(DemultiplexerMessage::RegisterReceiverEndpoint((
                receiver_endpoint,
                local_sender,
            )))
            .unwrap();
    }

    /// Bind the socket of this demultiplexer.
    fn bind_remote(
        coord: DemuxCoord,
        address: (String, u16),
        demux_sender: UnboundedChannelSender<DemultiplexerMessage<In>>,
        demux_receiver: UnboundedChannelReceiver<DemultiplexerMessage<In>>,
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

        // spawn an extra thread that keeps track of the connected clients and registered receivers
        let join_handle = std::thread::Builder::new()
            .name(format!("{}", coord))
            .spawn(move || Self::demultiplexer_thread(coord, demux_receiver))
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

            let (register_receiver_sender, register_receiver_receiver) =
                UnboundedChannelReceiver::new();
            demux_sender
                .send(DemultiplexerMessage::RegisterRemoteClient(
                    register_receiver_sender,
                ))
                .unwrap();
            let join_handle = std::thread::Builder::new()
                .name(format!("Client{}", coord))
                .spawn(move || {
                    Self::handle_remote_client(coord, register_receiver_receiver, stream)
                })
                .unwrap();
            join_handles.push(join_handle);
        }
        debug!(
            "All connection to {} started, waiting for them to finish",
            coord
        );
        // make sure the demultiplexer thread can exit
        drop(demux_sender);
        for handle in join_handles {
            handle.join().unwrap();
        }
        debug!("Demultiplexer of {} finished", coord);
    }

    /// The body of the thread that will broadcast the sender of the local replicas to all the
    /// deserializing threads.
    fn demultiplexer_thread(
        coord: DemuxCoord,
        receiver: UnboundedChannelReceiver<DemultiplexerMessage<In>>,
    ) {
        debug!("Starting demultiplexer for {}", coord);
        let mut known_receivers: Vec<ReceiverEndpointMessageSender<In>> = Vec::new();
        let mut clients = Vec::new();
        while let Ok(message) = receiver.recv() {
            match message {
                DemultiplexerMessage::RegisterRemoteClient(client) => {
                    for recv in &known_receivers {
                        client.send(recv.clone()).unwrap();
                    }
                    clients.push(client);
                }
                DemultiplexerMessage::RegisterReceiverEndpoint(recv) => {
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
    fn handle_remote_client(
        coord: DemuxCoord,
        register_receiver: UnboundedChannelReceiver<(
            ReceiverEndpoint,
            BoundedChannelSender<NetworkMessage<In>>,
        )>,
        mut receiver: TcpStream,
    ) {
        let address = receiver
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        let mut known_receivers = HashMap::new();
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
