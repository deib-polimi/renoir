use std::collections::HashMap;

use async_std::channel::Sender;
use async_std::task::JoinHandle;

use crate::block::InnerBlock;
use crate::config::{EnvironmentConfig, ExecutionRuntime, LocalRuntimeConfig};
use crate::operator::Operator;
use crate::stream::BlockId;
use crate::worker::spawn_worker;

pub type ReplicaId = usize;

#[derive(Debug)]
pub struct ExecutionMetadata {
    pub replica_id: ReplicaId,
}

pub struct StartHandle {
    pub starter: Sender<ExecutionMetadata>,
    pub join_handle: JoinHandle<()>,
}

pub struct Scheduler {
    pub config: EnvironmentConfig,
    pub next_blocks: HashMap<BlockId, Vec<BlockId>>,
    pub start_handles: HashMap<BlockId, Vec<StartHandle>>,
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
        let parallelism = self.local_block_parallelism(&block);
        info!(
            "Adding new block id={} with {} copies",
            block_id, parallelism
        );
        let mut blocks = vec![block];
        blocks.reserve(parallelism);
        for _ in 0..parallelism - 1 {
            // avoid an extra clone
            blocks.push(blocks[0].clone());
        }
        for mut block in blocks {
            block.operators.init(block.execution_metadata.clone());
            let start_handle = spawn_worker(block);
            self.start_handles
                .entry(block_id)
                .or_default()
                .push(start_handle);
        }
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
        for (_id, handles) in self.start_handles.drain() {
            for (replica_id, handle) in handles.into_iter().enumerate() {
                // TODO: build metadata
                let metadata = ExecutionMetadata { replica_id };
                handle.starter.send(metadata).await.unwrap();
                join.push(handle.join_handle);
            }
        }
        join
    }

    fn log_topology(&self) {
        debug!("Job graph:");
        for (id, next) in self.next_blocks.iter() {
            debug!("  {}: {:?}", id, next);
        }
    }

    fn local_block_parallelism<In, Out, OperatorChain>(
        &self,
        _block: &InnerBlock<In, Out, OperatorChain>,
    ) -> usize
    where
        OperatorChain: Operator<Out>,
    {
        match self.config.runtime {
            ExecutionRuntime::Local(local) => local.num_cores,
        }
    }
}
