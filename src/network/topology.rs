use std::collections::HashMap;
use std::marker::PhantomData;

use async_std::net::IpAddr;
use async_std::task::JoinHandle;
use itertools::Itertools;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use typemap::{Key, ShareMap};

use crate::config::{EnvironmentConfig, ExecutionRuntime};
use crate::network::{Coord, NetworkMessage, NetworkReceiver, NetworkSender, NetworkStarter};

/// This struct is used to index inside the `typemap` with the `NetworkReceiver`s.
struct ReceiverKey<In>(PhantomData<In>);
impl<In> Key for ReceiverKey<In>
where
    In: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    type Value = HashMap<Coord, NetworkReceiver<NetworkMessage<In>>>;
}

/// This struct is used to index inside the `typemap` with the `NetworkSender`s.
struct SenderKey<In>(PhantomData<In>);
impl<In> Key for SenderKey<In>
where
    In: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    type Value = HashMap<Coord, NetworkSender<NetworkMessage<In>>>;
}

/// Metadata about a registered receiver.
#[derive(Derivative)]
#[derivative(Debug)]
struct ReceiverMetadata {
    /// This receiver should also accept network connections and not just local ones.
    is_remote: bool,
    /// The starter that makes the receiver start binding the socket. `Option` because it will be
    /// moved out this structure.
    #[derivative(Debug = "ignore")]
    bind_socket: Option<NetworkStarter>,
    /// `JoinHandle` of the task that binds the remote socket. `Option` because it will be moved out
    /// this structure.
    #[derivative(Debug = "ignore")]
    join_handle: Option<JoinHandle<()>>,
}

/// Metadata about a registered sender.
#[derive(Derivative)]
#[derivative(Debug)]
struct SenderMetadata {
    /// This sender should connect to the remote recipient.
    is_remote: bool,
    /// The starter that makes the sender start connecting the socket. `Option` because it will be
    /// moved out this structure.
    #[derivative(Debug = "ignore")]
    connect_socket: Option<NetworkStarter>,
    /// `JoinHandle` of the task that connects the remote socket. `Option` because it will be moved
    /// out this structure.
    #[derivative(Debug = "ignore")]
    join_handle: Option<JoinHandle<()>>,
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
    #[derivative(Debug = "ignore")]
    receivers: ShareMap,
    /// All the registered local senders.
    ///
    /// It works exactly like `self.receivers`.
    #[derivative(Debug = "ignore")]
    local_senders: ShareMap,
    /// All the registered remote senders.
    ///
    /// It works exactly like `self.receivers`.
    #[derivative(Debug = "ignore")]
    remote_senders: ShareMap,

    /// The adjacency list of the execution graph that interests the local host.
    next: HashMap<Coord, Vec<Coord>>,

    /// The metadata about all the registered receivers.
    receivers_metadata: HashMap<Coord, ReceiverMetadata>,
    /// The metadata about all the registered senders.
    senders_metadata: HashMap<Coord, SenderMetadata>,
}

impl NetworkTopology {
    pub(crate) fn new(config: EnvironmentConfig) -> Self {
        NetworkTopology {
            config,
            receivers: ShareMap::custom(),
            local_senders: ShareMap::custom(),
            remote_senders: ShareMap::custom(),
            next: Default::default(),
            receivers_metadata: Default::default(),
            senders_metadata: Default::default(),
        }
    }

    /// Register a new replica to the network.
    ///
    /// This will create its sender and receiver and setup the remote counterparts. No socket is
    /// actually bound until `start_remote` is called and awaited. This method should not be called
    /// after `start_remote` has been called.
    pub(crate) fn register_replica<T>(&mut self, coord: Coord)
    where
        T: Clone + Serialize + DeserializeOwned + Send + 'static,
    {
        debug!("Registering {}", coord);
        let address = match &self.config.runtime {
            // this doesn't really matter, no socket will be bound
            ExecutionRuntime::Local(_) => (IpAddr::from([127, 0, 0, 1]), 0),
            ExecutionRuntime::Remote(remote) => {
                let host = &remote.hosts[coord.host_id];
                let address = host.address;
                // TODO: this will be wrong with the multiplexer
                let port = coord.block_id * host.num_cores + coord.replica_id;
                (address, port as u16 + host.base_port)
            }
        };
        let (receiver, bind_socket, receiver_join_handle) = NetworkReceiver::new(coord, address);
        let (sender, connect_socket, sender_join_handle) = NetworkSender::remote(coord, address);
        self.receivers_metadata
            .entry(coord)
            .or_insert_with(|| ReceiverMetadata {
                bind_socket: Some(bind_socket),
                is_remote: false,
                join_handle: Some(receiver_join_handle),
            });
        self.senders_metadata
            .entry(coord)
            .or_insert_with(|| SenderMetadata {
                connect_socket: Some(connect_socket),
                is_remote: false,
                join_handle: Some(sender_join_handle),
            });
        self.local_senders
            .entry::<SenderKey<T>>()
            .or_insert_with(|| Default::default())
            .insert(coord, receiver.sender());
        self.remote_senders
            .entry::<SenderKey<T>>()
            .or_insert_with(|| Default::default())
            .insert(coord, sender);
        self.receivers
            .entry::<ReceiverKey<T>>()
            .or_insert_with(|| Default::default())
            .insert(coord, receiver);
    }

    /// Get the sending channel to a replica.
    ///
    /// The replica must be registered and `start_remote` must have been called and awaited.
    pub fn get_sender<T>(&self, coord: Coord) -> NetworkSender<NetworkMessage<T>>
    where
        T: Clone + Serialize + DeserializeOwned + Send + 'static,
    {
        let metadata = self.senders_metadata.get(&coord).unwrap_or_else(|| {
            panic!("No sender registered for {}", coord);
        });
        let t_type_name = std::any::type_name::<T>();
        if metadata.is_remote {
            let map = self
                .remote_senders
                .get::<SenderKey<T>>()
                .unwrap_or_else(|| panic!("No remote sender registered for type: {}", t_type_name));
            map.get(&coord)
                .unwrap_or_else(|| {
                    panic!(
                        "Remote sender for ({}, {}) not registered",
                        t_type_name, coord
                    )
                })
                .clone()
        } else {
            let map = self
                .local_senders
                .get::<SenderKey<T>>()
                .unwrap_or_else(|| panic!("No local sender registered for type: {}", t_type_name));
            map.get(&coord)
                .unwrap_or_else(|| {
                    panic!(
                        "Local sender for ({}, {}) not registered",
                        t_type_name, coord
                    )
                })
                .clone()
        }
    }

    /// Get all the outgoing senders from a replica.
    pub fn get_senders<T>(&self, coord: Coord) -> HashMap<Coord, NetworkSender<NetworkMessage<T>>>
    where
        T: Clone + Serialize + DeserializeOwned + Send + 'static,
    {
        match self.next.get(&coord) {
            None => Default::default(),
            Some(next) => next.iter().map(|&c| (c, self.get_sender(c))).collect(),
        }
    }

    /// Get the receiver of all the ingoing messages to a replica.
    pub fn get_receiver<T>(&mut self, coord: Coord) -> NetworkReceiver<NetworkMessage<T>>
    where
        T: Clone + Serialize + DeserializeOwned + Send + 'static,
    {
        let t_type_name = std::any::type_name::<T>();
        let map = self
            .receivers
            .get_mut::<ReceiverKey<T>>()
            .unwrap_or_else(|| panic!("No receiver registered for type: {}", t_type_name));
        map.remove(&coord)
            .unwrap_or_else(|| panic!("Receiver for ({}, {:?}) not registered", t_type_name, coord))
    }

    /// Start all of the remote connections.
    ///
    /// First all the receivers are started and only after start also the senders, this will prevent
    /// deadlocks due to unfortunate orderings of bind/connect.
    pub async fn start_remote(&mut self) -> Vec<JoinHandle<()>> {
        info!("Starting remote connections");
        let mut join_handles = Vec::new();
        // first start all the receivers
        for (_coord, remote) in self.receivers_metadata.iter_mut() {
            let bind_socket = remote.bind_socket.take().unwrap();
            let join_handle = remote.join_handle.take().unwrap();
            if remote.is_remote {
                // tell the receiver to bind the socket
                bind_socket.send(true).await.unwrap();
                join_handles.push(join_handle);
            } else {
                // tell the receiver not to bind the socket and exit immediately
                bind_socket.send(false).await.unwrap();
                join_handle.await;
            }
        }
        // and later start all the senders
        for (_coord, remote) in self.senders_metadata.iter_mut() {
            let connect_socket = remote.connect_socket.take().unwrap();
            let join_handle = remote.join_handle.take().unwrap();
            if remote.is_remote {
                connect_socket.send(true).await.unwrap();
                join_handles.push(join_handle);
            } else {
                connect_socket.send(false).await.unwrap();
                join_handle.await;
            }
        }
        join_handles
    }

    /// Register the connection between two (already registered) replicas.
    ///
    /// At least one of the 2 replicas must be local (we cannot connect 2 remote replicas).
    /// This will not actually bind/connect sockets, it will just mark them as bindable/connectable,
    /// `start_remote` will start them.
    pub fn connect(&mut self, from: Coord, to: Coord) {
        let from_remote = from.host_id != self.config.host_id;
        let to_remote = to.host_id != self.config.host_id;
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

        // a remote client wants to connect: this receiver should be remote
        if from_remote {
            self.receivers_metadata.get_mut(&from).unwrap().is_remote = true;
        }
        // we want to connect to a remote: this sender should be remote
        if to_remote {
            self.senders_metadata.get_mut(&to).unwrap().is_remote = true;
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
