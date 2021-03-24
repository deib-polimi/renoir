use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::thread::JoinHandle;

use itertools::Itertools;
use typemap::{Key, SendMap};

use crate::config::{EnvironmentConfig, ExecutionRuntime};
use crate::network::remote::{DemultiplexingReceiver, MultiplexingSender};
use crate::network::{
    BlockCoord, Coord, NetworkMessage, NetworkReceiver, NetworkSender, ReceiverEndpoint,
};
use crate::operator::Data;

/// This struct is used to index inside the `typemap` with the `NetworkReceiver`s.
struct ReceiverKey<In: Data>(PhantomData<In>);
impl<In: Data> Key for ReceiverKey<In> {
    type Value = HashMap<ReceiverEndpoint, NetworkReceiver<NetworkMessage<In>>>;
}

/// This struct is used to index inside the `typemap` with the `NetworkSender`s.
struct SenderKey<In: Data>(PhantomData<In>);
impl<In: Data> Key for SenderKey<In> {
    type Value = HashMap<ReceiverEndpoint, NetworkSender<NetworkMessage<In>>>;
}

/// This struct is used to index inside the `typemap` with the `DemultiplexingReceiver`s.
struct DemultiplexingReceiverKey<In: Data>(PhantomData<In>);
impl<In: Data> Key for DemultiplexingReceiverKey<In> {
    type Value = HashMap<BlockCoord, DemultiplexingReceiver<NetworkMessage<In>>>;
}

/// This struct is used to index inside the `typemap` with the `MultiplexingSender`s.
struct MultiplexingSenderKey<In: Data>(PhantomData<In>);
impl<In: Data> Key for MultiplexingSenderKey<In> {
    type Value = HashMap<BlockCoord, MultiplexingSender<NetworkMessage<In>>>;
}

/// Metadata about a registered sender.
#[derive(Default, Derivative)]
#[derivative(Debug)]
struct SenderMetadata {
    /// This sender should connect to the remote recipient.
    is_remote: bool,
}

/// This struct keeps track of the network topology, all the registered replicas and their
/// connections.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct NetworkTopology {
    /// Configuration of the environment.
    config: EnvironmentConfig,
    /// All the registered receivers.
    ///
    /// Since the `NetworkReceiver` is generic over the element type we cannot simply store them
    /// (they have essentially different types). So instead of storing them in a
    /// `HashMap<Coord, NetworkReceiver<???>>` we store them indexed by element type inside a
    /// typemap: `TypeMap<ItemType -> HashMap<Coord, NetworkReceiver<ItemType>>`.
    ///
    /// This forces us to access the map only where we know the type of the element type, so some
    /// trickery is required to, for example, start the network component of all the receivers.
    /// At registration all the tasks start and immediately wait on a channel. We then store that
    /// channel (that does not depend of the element type) and later we use it to tell the task to
    /// actually start or exit.
    ///
    /// This map is indexed using `ReceiverKey`.
    #[derivative(Debug = "ignore")]
    receivers: SendMap,
    /// All the registered local senders.
    ///
    /// It works exactly like `self.receivers`.
    ///
    /// This map is indexed using `SenderKey`.
    #[derivative(Debug = "ignore")]
    senders: SendMap,

    /// All the registered demultiplexers.
    ///
    /// This map is indexed using `DemultiplexingReceiverKey`.
    #[derivative(Debug = "ignore")]
    demultiplexers: SendMap,
    /// All the registered multiplexers.
    ///
    /// This map is indexed using `MultiplexingSenderKey`.
    #[derivative(Debug = "ignore")]
    multiplexers: SendMap,

    /// The adjacency list of the execution graph that interests the local host.
    next: HashMap<Coord, Vec<Coord>>,
    /// The metadata about all the registered senders.
    senders_metadata: HashMap<ReceiverEndpoint, SenderMetadata>,

    /// The set of the used receivers.
    ///
    /// This set makes sure that a given receiver is not initialized twice. Bad things may happen if
    /// the same receiver is initialized twice (the same socket may be bound twice).
    used_receivers: HashSet<ReceiverEndpoint>,

    /// The set of join handles of the various threads spawned by the topology.
    join_handles: Vec<JoinHandle<()>>,
}

impl NetworkTopology {
    pub(crate) fn new(config: EnvironmentConfig) -> Self {
        NetworkTopology {
            config,
            receivers: SendMap::custom(),
            senders: SendMap::custom(),
            demultiplexers: SendMap::custom(),
            multiplexers: SendMap::custom(),
            next: Default::default(),
            senders_metadata: Default::default(),
            used_receivers: Default::default(),
            join_handles: Default::default(),
        }
    }

    /// Knowing that the computation ended, tear down the topology wait for all of its thread to
    /// exit.
    pub(crate) fn stop_and_wait(&mut self) {
        // drop all the senders/receivers making sure no dangling sender keep alive their network
        // receivers.
        // SAFETY: this is safe since we are not altering the content of the typemaps, just emptying
        // them.
        unsafe {
            self.receivers.data_mut().drain();
            self.senders.data_mut().drain();
            self.demultiplexers.data_mut().drain();
            self.multiplexers.data_mut().drain();
        }
        for handle in self.join_handles.drain(..) {
            handle.join().unwrap();
        }
    }

    /// Get all the outgoing senders from a replica.
    pub fn get_senders<T: Data>(
        &mut self,
        coord: Coord,
    ) -> HashMap<ReceiverEndpoint, NetworkSender<NetworkMessage<T>>> {
        match self.next.get(&coord) {
            None => Default::default(),
            Some(next) => {
                let next = next.clone();
                next.iter()
                    .map(|&c| {
                        let receiver_endpoint = ReceiverEndpoint::new(c, coord.block_id);
                        (receiver_endpoint, self.get_sender(receiver_endpoint))
                    })
                    .collect()
            }
        }
    }

    /// Get the sender associated with a given receiver endpoint. This may register a new channel if
    /// it was not registered before.
    fn get_sender<T: Data>(
        &mut self,
        receiver_endpoint: ReceiverEndpoint,
    ) -> NetworkSender<NetworkMessage<T>> {
        if !self.senders.contains::<SenderKey<T>>() {
            self.senders.insert::<SenderKey<T>>(Default::default());
        }
        let entry = self.senders.get_mut::<SenderKey<T>>().unwrap();
        if !entry.contains_key(&receiver_endpoint) {
            self.register_channel::<T>(receiver_endpoint);
        }
        self.senders
            .get::<SenderKey<T>>()
            .unwrap()
            .get(&receiver_endpoint)
            .unwrap()
            .clone()
    }

    /// Get the receiver of all the ingoing messages to a replica endpoint. This may register a new
    /// channel if it was not registered before.
    ///
    /// Calling this function twice with the same parameter will panic.
    pub fn get_receiver<T: Data>(
        &mut self,
        receiver_endpoint: ReceiverEndpoint,
    ) -> NetworkReceiver<NetworkMessage<T>> {
        if self.used_receivers.contains(&receiver_endpoint) {
            panic!(
                "The receiver for {} has already been got",
                receiver_endpoint
            );
        }
        self.used_receivers.insert(receiver_endpoint);

        if !self.receivers.contains::<ReceiverKey<T>>() {
            self.receivers.insert::<ReceiverKey<T>>(Default::default());
        }
        let entry = self.receivers.get_mut::<ReceiverKey<T>>().unwrap();
        // if the channel has not been registered yet, register it
        if !entry.contains_key(&receiver_endpoint) {
            self.register_channel::<T>(receiver_endpoint);
        }
        self.receivers
            .get_mut::<ReceiverKey<T>>()
            .unwrap()
            .remove(&receiver_endpoint)
            .unwrap()
    }

    /// Register the channel for the given receiver.
    ///
    /// This will initialize both the sender and the receiver to the receiver. If it's appropriate
    /// also the multiplexer and/or the demultiplexer are initialized and started.
    fn register_channel<T: Data>(&mut self, receiver_endpoint: ReceiverEndpoint) {
        debug!("Registering {}", receiver_endpoint);
        let sender_metadata = self.senders_metadata.get(&receiver_endpoint).unwrap();

        // create the receiver part of the channel
        let receiver = NetworkReceiver::<NetworkMessage<T>>::new(receiver_endpoint);
        let local_sender = receiver.sender();

        // if the receiver is local and the runtime is remote, register it to the demultiplexer
        if receiver_endpoint.coord.host_id == self.config.host_id.unwrap() {
            if let ExecutionRuntime::Remote(remote) = &self.config.runtime {
                let demuxes = self
                    .demultiplexers
                    .entry::<DemultiplexingReceiverKey<T>>()
                    .or_insert_with(Default::default);
                let block_coord = BlockCoord::from(receiver_endpoint.coord);
                #[allow(clippy::map_entry)]
                if !demuxes.contains_key(&block_coord) {
                    // find the set of all the previous blocks that have a MultiplexingSender that
                    // point to this DemultiplexingReceiver.
                    let mut prev = HashSet::new();
                    for (from, to) in &self.next {
                        // local blocks won't use the multiplexer
                        if from.host_id == block_coord.host_id {
                            continue;
                        }
                        for to in to {
                            if BlockCoord::from(*to) == block_coord {
                                prev.insert(BlockCoord::from(*from));
                            }
                        }
                    }
                    let (demux, join_handle) =
                        DemultiplexingReceiver::new(block_coord, remote, prev.len());
                    self.join_handles.push(join_handle);
                    demuxes.insert(block_coord, demux);
                }
                demuxes[&block_coord].register(receiver_endpoint, receiver.local_sender.clone());
            }
        }
        self.receivers
            .entry::<ReceiverKey<T>>()
            .or_insert_with(Default::default)
            .insert(receiver_endpoint, receiver);

        // create the sending part of the channel
        let sender = if sender_metadata.is_remote {
            // this channel ends to a remote host: connect it to the multiplexer
            if let ExecutionRuntime::Remote(remote) = &self.config.runtime {
                let muxers = self
                    .multiplexers
                    .entry::<MultiplexingSenderKey<T>>()
                    .or_insert_with(Default::default);
                let block_coord = BlockCoord::from(receiver_endpoint.coord);
                #[allow(clippy::map_entry)]
                if !muxers.contains_key(&block_coord) {
                    let (mux, join_handle) = MultiplexingSender::new(block_coord, remote);
                    self.join_handles.push(join_handle);
                    muxers.insert(block_coord, mux);
                }
                NetworkSender::remote(receiver_endpoint, muxers[&block_coord].clone())
            } else {
                panic!("Sender is marked as remote, but the runtime is not");
            }
        } else {
            local_sender
        };
        self.senders
            .entry::<SenderKey<T>>()
            .or_insert_with(Default::default)
            .insert(receiver_endpoint, sender);
    }

    /// Register the connection between two replicas.
    ///
    /// At least one of the 2 replicas must be local (we cannot connect 2 remote replicas).
    /// This will not actually bind/connect sockets, it will just mark them as bindable/connectable.
    pub fn connect(&mut self, from: Coord, to: Coord) {
        let host_id = self.config.host_id.unwrap();
        let from_remote = from.host_id != host_id;
        let to_remote = to.host_id != host_id;
        debug!(
            "New connection: {} (remote={}) -> {} (remote={})",
            from, from_remote, to, to_remote
        );
        self.next.entry(from).or_default().push(to);

        assert!(
            !(from_remote && to_remote),
            "Cannot connect two remote replicas: {} -> {}",
            from,
            to
        );

        let receiver_endpoint = ReceiverEndpoint::new(to, from.block_id);
        self.senders_metadata
            .entry(receiver_endpoint)
            .or_insert_with(Default::default);

        // we want to connect to a remote: this sender should be remote
        if to_remote {
            let metadata = self.senders_metadata.get_mut(&receiver_endpoint).unwrap();
            metadata.is_remote = true;
        }
    }

    pub fn log_topology(&self) {
        let mut topology = "Execution graph:".to_owned();
        for (coord, next) in self.next.iter().sorted() {
            topology += &format!("\n  {}:", coord);
            for next in next.iter().sorted() {
                topology += &format!(" {}", next);
            }
        }
        debug!("{}", topology);
    }
}
