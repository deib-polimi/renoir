use std::cell::RefCell;
use std::rc::Rc;

use crate::block::{ExecutionMetadataRef, InnerBlock};
use crate::config::EnvironmentConfig;
use crate::operator::source::Source;
use crate::scheduler::Scheduler;
use crate::stream::{BlockId, Stream};


pub struct StreamEnvironmentInner {
    pub config: EnvironmentConfig,
    pub block_count: BlockId,
    pub scheduler: Scheduler,
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
                scheduler: Scheduler::new(config),
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
        let join = env.scheduler.start().await;
        // wait till the computation ends
        for join_handle in join {
            join_handle.await;
        }
    }
}
