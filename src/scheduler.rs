use std::collections::HashMap;

use async_std::channel::Sender;
use async_std::task::JoinHandle;

use crate::block::InnerBlock;
use crate::config::{EnvironmentConfig, ExecutionRuntime, LocalRuntimeConfig};
use crate::environment::ReplicaId;
use crate::network::{NetworkReceiver, NetworkSender};
use crate::operator::Operator;
use crate::stream::BlockId;
use crate::worker::spawn_worker;

#[derive(Debug)]
pub struct ExecutionMetadata {
    pub next: Vec<Vec<NetworkSender>>,
    pub prev: Vec<NetworkReceiver>,
    pub replica_id: ReplicaId,
}

pub struct StartHandle {
    pub starter: Sender<ExecutionMetadata>,
    pub join_handle: JoinHandle<()>,
}

pub struct Scheduler {
    pub config: EnvironmentConfig,
    pub next_blocks: HashMap<BlockId, Vec<BlockId>>,
    pub start_handles: HashMap<BlockId, StartHandle>,
}

impl Scheduler {
    pub fn new(config: EnvironmentConfig) -> Self {
        Self {
            config,
            next_blocks: Default::default(),
            start_handles: Default::default(),
        }
    }

    pub fn add_block<In, Out, OperatorChain>(&mut self, block: InnerBlock<In, Out, OperatorChain>)
    where
        In: Send + 'static,
        Out: Send + 'static,
        OperatorChain: Operator<Out> + Send + 'static,
    {
        let block_id = block.id;
        info!("Adding new block, id={}", block_id);
        // TODO: spawn many blocks
        let start_handle = spawn_worker(block);
        self.start_handles.insert(block_id, start_handle);
    }

    pub fn connect_blocks(&mut self, from: BlockId, to: BlockId) {
        info!("Connecting blocks: {} -> {}", from, to);
        if !self.start_handles.contains_key(&from) {
            panic!("Connecting from an unknown block: {}", from);
        }
        self.next_blocks.entry(from).or_default().push(to);
    }

    pub async fn start(&mut self) -> Vec<JoinHandle<()>> {
        self.log_topology();
        match self.config.runtime {
            ExecutionRuntime::Local(local) => self.start_local(local).await,
        }
    }

    async fn start_local(&mut self, config: LocalRuntimeConfig) -> Vec<JoinHandle<()>> {
        info!("Starting local environment: {:?}", config);
        let mut join = Vec::new();
        // start the execution
        for (id, handle) in self.start_handles.drain() {
            // TODO: build metadata
            let metadata = ExecutionMetadata {
                next: vec![],
                prev: vec![],
                replica_id: 0,
            };
            handle.starter.send(metadata).await.unwrap();
            join.push(handle.join_handle);
        }
        join
    }

    fn log_topology(&self) {
        debug!("Job graph:");
        for (id, next) in self.next_blocks.iter() {
            debug!("  {}: {:?}", id, next);
        }
    }
}
