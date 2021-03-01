use std::cell::RefCell;
use std::rc::Rc;

use crate::block::InnerBlock;
use crate::config::EnvironmentConfig;
use crate::operator::source::Source;
use crate::scheduler::Scheduler;
use crate::stream::{BlockId, Stream};

pub struct StreamEnvironmentInner {
    pub config: EnvironmentConfig,
    pub block_count: BlockId,
    scheduler: Option<Scheduler>,
}

pub struct StreamEnvironment {
    inner: Rc<RefCell<StreamEnvironmentInner>>,
}

impl StreamEnvironment {
    pub fn new(config: EnvironmentConfig) -> Self {
        info!("Constructing environment");
        StreamEnvironment {
            inner: Rc::new(RefCell::new(StreamEnvironmentInner::new(config))),
        }
    }

    pub fn stream<Out, S>(&mut self, source: S) -> Stream<Out, Out, S>
    where
        Out: Clone + Send + 'static,
        S: Source<Out> + 'static,
    {
        let block_id = self.inner.borrow().block_count;
        self.inner.borrow_mut().block_count += 1;
        info!("Creating a new stream, block_id={}", block_id);
        Stream {
            block_id,
            block: InnerBlock::new(block_id, source),
            env: self.inner.clone(),
        }
    }

    pub async fn execute(self) {
        let mut env = self.inner.borrow_mut();
        info!("Starting execution of {} blocks", env.block_count);
        let join = env.scheduler.take().unwrap().start().await;
        // wait till the computation ends
        for join_handle in join {
            join_handle.await;
        }
    }
}

impl StreamEnvironmentInner {
    pub fn new(config: EnvironmentConfig) -> Self {
        Self {
            config,
            block_count: 0,
            scheduler: Some(Scheduler::new(config)),
        }
    }

    pub fn scheduler_mut(&mut self) -> &mut Scheduler {
        self.scheduler
            .as_mut()
            .expect("The environment has already been started, cannot access the scheduler")
    }
}
