use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use async_std::channel::Sender;
use async_std::task::JoinHandle;

use crate::block::{ExecutionMetadataRef, InnerBlock};
use crate::config::EnvironmentConfig;
use crate::operator::source::Source;
use crate::scheduler;
use crate::scheduler::ExecutionMetadata;
use crate::stream::{BlockId, Stream};
use std::ops::DerefMut;

pub struct StartHandle {
    pub starter: Sender<ExecutionMetadata>,
    pub join_handle: JoinHandle<()>,
}

pub struct StreamEnvironmentInner {
    pub config: EnvironmentConfig,
    pub block_count: BlockId,
    pub next_blocks: HashMap<BlockId, Vec<BlockId>>,
    pub start_handles: HashMap<BlockId, StartHandle>,
}

pub struct StreamEnvironment {
    inner: Rc<RefCell<StreamEnvironmentInner>>,
}

impl StreamEnvironment {
    pub fn new(config: EnvironmentConfig) -> Self {
        info!("Constructing environment");
        StreamEnvironment {
            inner: Rc::new(RefCell::new(StreamEnvironmentInner {
                config,
                block_count: 0,
                next_blocks: Default::default(),
                start_handles: Default::default(),
            })),
        }
    }

    pub fn stream<Out, S>(&mut self, source: S) -> Stream<Out, Out, S>
    where
        S: Source<Out> + 'static,
    {
        let block_id = self.inner.borrow().block_count;
        self.inner.borrow_mut().block_count += 1;
        info!("Creating a new stream, block_id={}", block_id);
        Stream {
            block_id,
            block: InnerBlock::new(block_id, source, ExecutionMetadataRef::default()),
            env: self.inner.clone(),
        }
    }

    pub async fn execute(self) {
        let mut env = self.inner.borrow_mut();
        info!("Starting execution of {} blocks", env.block_count);
        StreamEnvironment::log_topology(&env);
        let join = scheduler::start(env.deref_mut()).await;
        // wait till the computation ends
        for join_handle in join {
            join_handle.await;
        }
    }

    fn log_topology(env: &StreamEnvironmentInner) {
        debug!("Job graph:");
        for (id, next) in env.next_blocks.iter() {
            debug!("  {}: {:?}", id, next);
        }
    }
}
