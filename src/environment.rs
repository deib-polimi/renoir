use std::cell::RefCell;
use std::rc::Rc;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::block::InnerBlock;
use crate::config::EnvironmentConfig;
use crate::operator::source::Source;
use crate::scheduler::Scheduler;
use crate::stream::{BlockId, Stream};

/// Actual content of the StreamEnvironment. This is stored inside a `Rc` and it's shared among all
/// the blocks.
pub(crate) struct StreamEnvironmentInner {
    /// The configuration of the environment.
    pub(crate) config: EnvironmentConfig,
    /// The number of blocks in the job graph, it's used to assign new ids to the blocks.
    block_count: BlockId,
    /// The scheduler that will start the computation. It's an option because it will be moved out
    /// of this struct when the computation starts.
    scheduler: Option<Scheduler>,
}

/// Streaming environment from which it's possible to register new streams and start the
/// computation.
pub struct StreamEnvironment {
    /// Reference to the actual content of the environment.
    inner: Rc<RefCell<StreamEnvironmentInner>>,
}

impl StreamEnvironment {
    /// Construct a new environment from the config.
    pub fn new(config: EnvironmentConfig) -> Self {
        info!("Constructing environment");
        StreamEnvironment {
            inner: Rc::new(RefCell::new(StreamEnvironmentInner::new(config))),
        }
    }

    /// Construct a new stream bound to this environment starting with the specified source.
    pub fn stream<Out, S>(&mut self, source: S) -> Stream<Out, Out, S>
    where
        Out: Clone + Serialize + DeserializeOwned + Send + 'static,
        S: Source<Out> + Send + 'static,
    {
        let block_id = self.inner.borrow_mut().new_block();
        let source_max_parallelism = source.get_max_parallelism();
        info!(
            "Creating a new stream, block_id={} with max_parallelism {:?}",
            block_id, source_max_parallelism
        );
        let mut block = InnerBlock::new(block_id, source);
        if let Some(p) = source_max_parallelism {
            block.scheduler_requirements.max_parallelism(p);
        }
        Stream {
            block,
            env: self.inner.clone(),
        }
    }

    /// Start the computation. Await on the returned future to actually start the computation.
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
    fn new(config: EnvironmentConfig) -> Self {
        Self {
            config: config.clone(),
            block_count: 0,
            scheduler: Some(Scheduler::new(config)),
        }
    }

    /// Allocate a new BlockId inside the environment.
    pub(crate) fn new_block(&mut self) -> BlockId {
        let new_id = self.block_count;
        self.block_count += 1;
        info!("Creating a new block, id={}", new_id);
        new_id
    }

    /// Return a mutable reference to the scheduler. This method will panic if the computation has
    /// already been started.
    pub(crate) fn scheduler_mut(&mut self) -> &mut Scheduler {
        self.scheduler
            .as_mut()
            .expect("The environment has already been started, cannot access the scheduler")
    }
}
