use std::collections::HashMap;

use async_std::channel::Sender;
use async_std::sync::{Arc, Mutex};
use async_std::task::JoinHandle;

use crate::block::InnerBlock;
use crate::config::{EnvironmentConfig, ExecutionRuntime, LocalRuntimeConfig};
use crate::network::{Coord, NetworkTopology};
use crate::operator::Operator;
use crate::stream::BlockId;
use crate::worker::spawn_worker;

/// The identifier of a replica of a block in the execution graph.
pub type ReplicaId = usize;

/// Metadata associated to a block in the execution graph.
#[derive(Clone, Debug)]
pub struct ExecutionMetadata {
    /// The coordinate of the block (it's id, replica id, ...).
    pub(crate) coord: Coord,
    /// The total number of replicas of the block.
    pub(crate) num_replicas: usize,
    /// The total number of previous blocks inside the execution graph.
    pub(crate) num_prev: usize,
    /// A reference to the `NetworkTopology` that keeps the state of the network.
    pub(crate) network: Arc<Mutex<NetworkTopology>>,
}

/// Handle that the scheduler uses to start the computation of a block.
pub(crate) struct StartHandle {
    /// Sender for the `ExecutionMetadata` sent to the worker.
    starter: Sender<ExecutionMetadata>,
    /// `JoinHandle` used to wait until a block has finished working.
    join_handle: JoinHandle<()>,
}

/// Information about a block in the job graph.
#[derive(Debug, Clone)]
struct SchedulerBlockInfo {
    /// The number of replicas of the block.
    num_replicas: usize,
}

/// The `Scheduler` is the entity that keeps track of all the blocks of the job graph and when the
/// execution starts it builds the execution graph and actually start the workers.
pub(crate) struct Scheduler {
    /// The configuration of the environment.
    config: EnvironmentConfig,
    /// Adjacency list of the job graph.
    next_blocks: HashMap<BlockId, Vec<BlockId>>,
    /// Reverse adjacency list of the job graph.
    prev_blocks: HashMap<BlockId, Vec<BlockId>>,
    /// Information about the blocks known to the scheduler.
    block_info: HashMap<BlockId, SchedulerBlockInfo>,
    /// The list of handles of each block in the execution graph.
    start_handles: Vec<(Coord, StartHandle)>,
    /// The network topology that keeps track of all the connections inside the execution graph.
    network: NetworkTopology,
}

impl Scheduler {
    pub fn new(config: EnvironmentConfig) -> Self {
        Self {
            config,
            next_blocks: Default::default(),
            prev_blocks: Default::default(),
            block_info: Default::default(),
            start_handles: Default::default(),
            network: NetworkTopology::new(),
        }
    }

    /// Register a new block inside the scheduler.
    ///
    /// This spawns a worker for each replica of the block in the execution graph and saves its
    /// start handle. The handle will be later used to actually start the worker when the
    /// computation is asked to begin.
    pub(crate) fn add_block<In, Out, OperatorChain>(
        &mut self,
        block: InnerBlock<In, Out, OperatorChain>,
    ) where
        In: Clone + Send + 'static,
        Out: Clone + Send + 'static,
        OperatorChain: Operator<Out> + Send + 'static,
    {
        let block_id = block.id;
        let info = self.local_block_info(&block);
        info!("Adding new block id={}: {:?}", block_id, info);

        // duplicate the block in the execution graph
        let mut blocks = vec![];
        blocks.reserve(info.num_replicas);
        if info.num_replicas >= 1 {
            let replica_id = 0;
            let coord = Coord::new(block_id, replica_id);
            blocks.push((coord, block));
        }
        // not all blocks can be cloned: clone only when necessary
        for replica_id in 1..info.num_replicas {
            let coord = Coord::new(block_id, replica_id);
            blocks.push((coord, blocks[0].1.clone()));
        }
        self.block_info.insert(block_id, info);

        for (coord, block) in blocks {
            // register this block in the network
            self.network.register_local::<In>(coord);
            // spawn the actual worker
            let start_handle = spawn_worker(block);
            self.start_handles.push((coord, start_handle));
        }
    }

    /// Connect a pair of blocks inside the job graph.
    pub(crate) fn connect_blocks(&mut self, from: BlockId, to: BlockId) {
        info!("Connecting blocks: {} -> {}", from, to);
        self.next_blocks.entry(from).or_default().push(to);
        self.prev_blocks.entry(to).or_default().push(from);
    }

    /// Start the computation returning the list of handles used to join the workers.
    pub(crate) async fn start(self) -> Vec<JoinHandle<()>> {
        match self.config.runtime {
            ExecutionRuntime::Local(local) => self.start_local(local).await,
        }
    }

    /// Start the computation locally without spawning any other process.
    async fn start_local(mut self, config: LocalRuntimeConfig) -> Vec<JoinHandle<()>> {
        info!("Starting local environment: {:?}", config);
        self.setup_topology();
        self.log_topology();
        self.network.log_topology();

        let mut join = Vec::new();
        let num_prev = self.num_prev();
        let network = Arc::new(Mutex::new(self.network));
        // let start_handles: Vec<_> = self.start_handles.drain().collect();
        // start the execution
        for (coord, handle) in self.start_handles {
            let num_replicas = self.block_info[&coord.block_id].num_replicas;
            let metadata = ExecutionMetadata {
                coord,
                num_replicas,
                num_prev: num_prev[&coord.block_id],
                network: network.clone(),
            };
            handle.starter.send(metadata).await.unwrap();
            join.push(handle.join_handle);
        }
        join
    }

    /// Compute the number of predecessors of each block inside the execution graph.
    fn num_prev(&self) -> HashMap<BlockId, usize> {
        self.block_info
            .keys()
            .map(|&block_id| {
                (
                    block_id,
                    if let Some(prev_blocks) = self.prev_blocks.get(&block_id) {
                        prev_blocks
                            .iter()
                            .map(|b| self.block_info[b].num_replicas)
                            .sum()
                    } else {
                        0
                    },
                )
            })
            .collect()
    }

    /// Build the execution graph for the network topology.
    fn setup_topology(&mut self) {
        for (from_block_id, next) in self.next_blocks.iter() {
            let from_info = &self.block_info[from_block_id];
            for to_block_id in next.iter() {
                let to_info = &self.block_info[to_block_id];
                for from_replica_id in 0..from_info.num_replicas {
                    let from_coord = Coord::new(*from_block_id, from_replica_id);
                    for to_replica_id in 0..to_info.num_replicas {
                        let to_coord = Coord::new(*to_block_id, to_replica_id);
                        self.network.connect(from_coord, to_coord);
                    }
                }
            }
        }
    }

    fn log_topology(&self) {
        debug!("Job graph:");
        for (id, next) in self.next_blocks.iter() {
            debug!("  {}: {:?}", id, next);
        }
    }

    /// Extract the `SchedulerBlockInfo` of a block that will be run locally.
    fn local_block_info<In, Out, OperatorChain>(
        &self,
        block: &InnerBlock<In, Out, OperatorChain>,
    ) -> SchedulerBlockInfo
    where
        In: Clone + Send + 'static,
        Out: Clone + Send + 'static,
        OperatorChain: Operator<Out>,
    {
        match self.config.runtime {
            ExecutionRuntime::Local(local) => SchedulerBlockInfo {
                num_replicas: block.max_parallelism.unwrap_or(local.num_cores),
            },
        }
    }
}

impl StartHandle {
    pub(crate) fn new(starter: Sender<ExecutionMetadata>, join_handle: JoinHandle<()>) -> Self {
        Self {
            starter,
            join_handle,
        }
    }
}
