use std::any::TypeId;
use std::collections::HashMap;
use std::thread::JoinHandle;

use itertools::Itertools;

use crate::block::{BatchMode, BlockStructure, InnerBlock, JobGraphGenerator};
use crate::config::{EnvironmentConfig, ExecutionRuntime, LocalRuntimeConfig, RemoteRuntimeConfig};
use crate::network::{Coord, NetworkTopology};
use crate::operator::{Data, Operator};
use crate::profiler::{wait_profiler, ProfilerResult};
use crate::worker::spawn_worker;
use crate::TracingData;
use crate::CoordUInt;

/// Identifier of a block in the job graph.
pub type BlockId = CoordUInt;
/// The identifier of an host.
pub type HostId = CoordUInt;
/// The identifier of a replica of a block in the execution graph.
pub type ReplicaId = CoordUInt;

/// Metadata associated to a block in the execution graph.
#[derive(Debug)]
pub struct ExecutionMetadata<'a> {
    /// The coordinate of the block (it's id, replica id, ...).
    pub coord: Coord,
    /// The list of replicas of this block.
    pub replicas: Vec<Coord>,
    /// The global identifier of the replica (from 0 to `replicas.len()-1`)
    pub global_id: CoordUInt,
    /// The total number of previous blocks inside the execution graph.
    pub prev: Vec<(Coord, TypeId)>,
    /// A reference to the `NetworkTopology` that keeps the state of the network.
    pub(crate) network: &'a mut NetworkTopology,
    /// The batching mode to use inside this block.
    pub batch_mode: BatchMode,
}

/// Information about a block in the job graph.
#[derive(Debug, Clone)]
struct SchedulerBlockInfo {
    /// String representation of the block.
    repr: String,
    /// All the replicas, grouped by host.
    replicas: HashMap<HostId, Vec<Coord>, std::collections::hash_map::RandomState>,
    /// All the global ids, grouped by coordinate.
    global_ids: HashMap<Coord, CoordUInt, std::collections::hash_map::RandomState>,
    /// The batching mode to use inside this block.
    batch_mode: BatchMode,
    /// Whether this block has `NextStrategy::OnlyOne`.
    is_only_one_strategy: bool,
}

/// The `Scheduler` is the entity that keeps track of all the blocks of the job graph and when the
/// execution starts it builds the execution graph and actually start the workers.
pub(crate) struct Scheduler {
    /// The configuration of the environment.
    config: EnvironmentConfig,
    /// Adjacency list of the job graph.
    next_blocks:
        HashMap<BlockId, Vec<(BlockId, TypeId, bool)>, std::collections::hash_map::RandomState>,
    /// Reverse adjacency list of the job graph.
    prev_blocks: HashMap<BlockId, Vec<(BlockId, TypeId)>, std::collections::hash_map::RandomState>,
    /// Information about the blocks known to the scheduler.
    block_info: HashMap<BlockId, SchedulerBlockInfo, std::collections::hash_map::RandomState>,
    /// The list of handles of each block in the execution graph.
    // start_handles: Vec<(Coord, StartHandle)>,
    block_init: Vec<(
        Coord,
        Box<dyn FnOnce(&mut ExecutionMetadata) -> (JoinHandle<()>, BlockStructure) + Send>,
    )>,
    /// The network topology that keeps track of all the connections inside the execution graph.
    network: NetworkTopology,
}

impl Scheduler {
    pub fn new(config: EnvironmentConfig) -> Self {
        Self {
            next_blocks: Default::default(),
            prev_blocks: Default::default(),
            block_info: Default::default(),
            // start_handles: Default::default(),
            block_init: Default::default(),
            network: NetworkTopology::new(config.clone()),
            config,
        }
    }

    /// Register a new block inside the scheduler.
    ///
    /// This spawns a worker for each replica of the block in the execution graph and saves its
    /// start handle. The handle will be later used to actually start the worker when the
    /// computation is asked to begin.
    pub(crate) fn add_block<Out: Data, OperatorChain>(
        &mut self,
        block: InnerBlock<Out, OperatorChain>,
    ) where
        OperatorChain: Operator<Out> + 'static,
    {
        let block_id = block.id;
        let info = self.block_info(&block);
        info!(
            "Adding new block id={}: {}",
            block_id,
            block.to_string(),
            // info
        );

        // duplicate the block in the execution graph
        let mut blocks = vec![];
        let local_replicas = info.replicas(self.config.host_id.unwrap());
        blocks.reserve(local_replicas.len());
        if !local_replicas.is_empty() {
            let coord = local_replicas[0];
            blocks.push((coord, block));
        }
        // not all blocks can be cloned: clone only when necessary
        if local_replicas.len() > 1 {
            for coord in &local_replicas[1..] {
                blocks.push((*coord, blocks[0].1.clone()));
            }
        }

        self.block_info.insert(block_id, info);

        for (coord, block) in blocks {
            // spawn the actual worker
            self.block_init.push((
                coord,
                Box::new(move |metadata| spawn_worker(block, metadata)),
            ));
        }
    }

    /// Connect a pair of blocks inside the job graph.
    pub(crate) fn connect_blocks(&mut self, from: BlockId, to: BlockId, typ: TypeId) {
        info!("Connecting blocks: {} -> {}", from, to);
        self.next_blocks
            .entry(from)
            .or_default()
            .push((to, typ, false));
        self.prev_blocks.entry(to).or_default().push((from, typ));
    }

    /// Connect a pair of blocks inside the job graph, marking the connection as fragile.
    pub(crate) fn connect_blocks_fragile(&mut self, from: BlockId, to: BlockId, typ: TypeId) {
        info!("Connecting blocks: {} -> {} (fragile)", from, to);
        self.next_blocks
            .entry(from)
            .or_default()
            .push((to, typ, true));
        self.prev_blocks.entry(to).or_default().push((from, typ));
    }

    #[cfg(feature = "async-tokio")]
    /// Start the computation returning the list of handles used to join the workers.
    pub(crate) async fn start(mut self, num_blocks: usize) {
        info!("Starting scheduler: {:#?}", self.config);
        self.log_topology();

        assert_eq!(
            self.block_info.len(),
            num_blocks,
            "Some streams do not have a sink attached: {} streams created, but only {} registered",
            num_blocks,
            self.block_info.len(),
        );

        self.build_execution_graph();
        self.network.build();
        self.network.log();

        let mut join = vec![];
        let mut block_structures = vec![];
        let mut job_graph_generator = JobGraphGenerator::new();

        for (coord, init_fn) in self.block_init {
            let block_info = &self.block_info[&coord.block_id];
            let replicas = block_info.replicas.values().flatten().cloned().collect();
            let global_id = block_info.global_ids[&coord];
            let mut metadata = ExecutionMetadata {
                coord,
                replicas,
                global_id,
                prev: self.network.prev(coord),
                network: &mut self.network,
                batch_mode: block_info.batch_mode,
            };
            let (handle, structure) = init_fn(&mut metadata);
            join.push(handle);
            block_structures.push((coord, structure.clone()));
            job_graph_generator.add_block(coord.block_id, structure);
        }

        let job_graph = job_graph_generator.finalize();
        debug!("Job graph in dot format:\n{}", job_graph);

        self.network.finalize();

        self.network.stop_and_wait().await;

        for handle in join {
            handle.join().unwrap();
        }

        let profiler_results = wait_profiler();

        Self::log_tracing_data(block_structures, profiler_results);
    }

    #[cfg(not(feature = "async-tokio"))]
    /// Start the computation returning the list of handles used to join the workers.
    pub(crate) fn start(mut self, num_blocks: CoordUInt) {
        info!("Starting scheduler: {:#?}", self.config);
        self.log_topology();

        assert_eq!(
            self.block_info.len(),
            num_blocks as usize,
            "Some streams do not have a sink attached: {} streams created, but only {} registered",
            num_blocks as usize,
            self.block_info.len(),
        );

        self.build_execution_graph();
        self.network.build();
        self.network.log();

        let mut join = vec![];
        let mut block_structures = vec![];
        let mut job_graph_generator = JobGraphGenerator::new();

        for (coord, init_fn) in self.block_init {
            let block_info = &self.block_info[&coord.block_id];
            let replicas = block_info.replicas.values().flatten().cloned().collect();
            let global_id = block_info.global_ids[&coord];
            let mut metadata = ExecutionMetadata {
                coord,
                replicas,
                global_id,
                prev: self.network.prev(coord),
                network: &mut self.network,
                batch_mode: block_info.batch_mode,
            };
            let (handle, structure) = init_fn(&mut metadata);
            join.push(handle);
            block_structures.push((coord, structure.clone()));
            job_graph_generator.add_block(coord.block_id, structure);
        }

        let job_graph = job_graph_generator.finalize();
        debug!("Job graph in dot format:\n{}", job_graph);

        self.network.finalize();

        for handle in join {
            handle.join().unwrap();
        }

        self.network.stop_and_wait();

        let profiler_results = wait_profiler();

        Self::log_tracing_data(block_structures, profiler_results);
    }

    /// Get the ids of the previous blocks of a given block in the job graph
    pub(crate) fn prev_blocks(&self, block_id: BlockId) -> Option<Vec<(BlockId, TypeId)>> {
        self.prev_blocks.get(&block_id).cloned()
    }

    /// Build the execution graph for the network topology, multiplying each block of the job graph
    /// into all its replicas.
    fn build_execution_graph(&mut self) {
        for (from_block_id, next) in self.next_blocks.iter() {
            let from = &self.block_info[from_block_id];
            for &(to_block_id, typ, fragile) in next.iter() {
                let to = &self.block_info[&to_block_id];
                // for each pair (from -> to) inside the job graph, connect all the corresponding
                // jobs of the execution graph
                for &from_coord in from.replicas.values().flatten() {
                    let to: Vec<_> = to.replicas.values().flatten().collect();
                    for &to_coord in &to {
                        if from.is_only_one_strategy || fragile {
                            if to.len() == 1
                                || (to_coord.host_id == from_coord.host_id
                                    && to_coord.replica_id == from_coord.replica_id)
                            {
                                self.network.connect(from_coord, *to_coord, typ, fragile);
                            }
                        } else {
                            self.network.connect(from_coord, *to_coord, typ, fragile);
                        }
                    }
                }
            }
        }
    }

    fn log_tracing_data(structures: Vec<(Coord, BlockStructure)>, profilers: Vec<ProfilerResult>) {
        let data = TracingData {
            structures,
            profilers,
        };
        debug!(
            "__noir2_TRACING_DATA__ {}",
            serde_json::to_string(&data).unwrap()
        );
    }

    fn log_topology(&self) {
        let mut topology = "Job graph:".to_string();
        for (block_id, block) in self.block_info.iter() {
            topology += &format!("\n  {}: {}", block_id, block.repr);
            if let Some(next) = &self.next_blocks.get(block_id) {
                let sorted = next
                    .iter()
                    .map(|(x, _, fragile)| format!("{}{}", x, if *fragile { "*" } else { "" }))
                    .sorted()
                    .collect_vec();
                topology += &format!("\n    -> {:?}", sorted);
            }
        }
        debug!("{}", topology);
        let mut assignments = "Replicas:".to_string();
        for (block_id, block) in self.block_info.iter() {
            assignments += &format!("\n  {}:", block_id);
            let replicas = block.replicas.values().flatten().sorted();
            for &coord in replicas {
                assignments += &format!(" {}", coord);
            }
        }
        debug!("{}", assignments);
    }

    /// Extract the `SchedulerBlockInfo` of a block.
    fn block_info<Out: Data, OperatorChain>(
        &self,
        block: &InnerBlock<Out, OperatorChain>,
    ) -> SchedulerBlockInfo
    where
        OperatorChain: Operator<Out>,
    {
        match &self.config.runtime {
            ExecutionRuntime::Local(local) => self.local_block_info(block, local),
            ExecutionRuntime::Remote(remote) => self.remote_block_info(block, remote),
        }
    }

    /// Extract the `SchedulerBlockInfo` of a block that runs only locally.
    ///
    /// The number of replicas will be the minimum between:
    ///
    ///  - the number of logical cores.
    ///  - the `max_parallelism` of the block.
    fn local_block_info<Out: Data, OperatorChain>(
        &self,
        block: &InnerBlock<Out, OperatorChain>,
        local: &LocalRuntimeConfig,
    ) -> SchedulerBlockInfo
    where
        OperatorChain: Operator<Out>,
    {
        let max_parallelism = block.scheduler_requirements.max_parallelism;
        let num_replicas = local.num_cores.min(max_parallelism.unwrap_or(CoordUInt::MAX));
        debug!(
            "Block {} will have {} local replicas (max_parallelism={:?}), is_only_one_strategy={}",
            block.id, num_replicas, max_parallelism, block.is_only_one_strategy
        );
        let host_id = self.config.host_id.unwrap();
        let replicas = (0..num_replicas).map(|r| Coord::new(block.id, host_id, r));
        let global_ids = (0..num_replicas).map(|r| (Coord::new(block.id, host_id, r), r));
        SchedulerBlockInfo {
            repr: block.to_string(),
            replicas: vec![(host_id, replicas.collect())].into_iter().collect(),
            global_ids: global_ids.into_iter().collect(),
            batch_mode: block.batch_mode,
            is_only_one_strategy: block.is_only_one_strategy,
        }
    }

    /// Extract the `SchedulerBlockInfo` of a block that runs remotely.
    ///
    /// The block can be replicated at most `max_parallelism` times (if specified). Assign the
    /// replicas starting from the first host giving as much replicas as possible..
    fn remote_block_info<Out: Data, OperatorChain>(
        &self,
        block: &InnerBlock<Out, OperatorChain>,
        remote: &RemoteRuntimeConfig,
    ) -> SchedulerBlockInfo
    where
        OperatorChain: Operator<Out>,
    {
        let max_parallelism = block.scheduler_requirements.max_parallelism;
        debug!("Allocating block {} on remote runtime", block.id);
        // number of replicas we can assign at most
        let mut remaining_replicas = max_parallelism.unwrap_or(CoordUInt::MAX);
        let mut num_replicas = 0;
        let mut replicas: HashMap<_, Vec<_>, std::collections::hash_map::RandomState> =
            HashMap::default();
        let mut global_ids = HashMap::default();
        // FIXME: if the next_strategy of the previous blocks are OnlyOne the replicas of this block
        //        must be in the same hosts are the previous blocks.
        for (host_id, host_info) in remote.hosts.iter().enumerate() {
            let host_id: HostId = host_id.try_into().expect("host_id > max id");
            let num_host_replicas = host_info.num_cores.min(remaining_replicas);
            debug!(
                "Block {} will have {} replicas on {} (max_parallelism={:?}, num_cores={})",
                block.id,
                num_host_replicas,
                host_info.to_string(),
                max_parallelism,
                host_info.num_cores
            );
            remaining_replicas -= num_host_replicas;
            let host_replicas = replicas.entry(host_id).or_default();
            for replica_id in 0..num_host_replicas {
                let coord = Coord::new(block.id, host_id, replica_id);
                host_replicas.push(coord);
                global_ids.insert(coord, num_replicas + replica_id);
            }
            num_replicas += num_host_replicas;
        }
        SchedulerBlockInfo {
            repr: block.to_string(),
            replicas,
            global_ids,
            batch_mode: block.batch_mode,
            is_only_one_strategy: block.is_only_one_strategy,
        }
    }
}

impl SchedulerBlockInfo {
    /// The list of replicas of the block inside a given host.
    fn replicas(&self, host_id: HostId) -> Vec<Coord> {
        self.replicas.get(&host_id).cloned().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source::IteratorSource;

    #[test]
    #[should_panic(expected = "Some streams do not have a sink attached")]
    fn test_scheduler_panic_on_missing_sink() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = IteratorSource::new(vec![1, 2, 3].into_iter());
        let _stream = env.stream(source);
        env.execute();
    }

    #[test]
    #[should_panic(expected = "Some streams do not have a sink attached")]
    fn test_scheduler_panic_on_missing_sink_shuffle() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = IteratorSource::new(vec![1, 2, 3].into_iter());
        let _stream = env.stream(source).shuffle();
        env.execute();
    }
}
