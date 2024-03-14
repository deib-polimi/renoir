use parking_lot::Mutex;
use std::any::TypeId;
use std::sync::Arc;
use std::thread::available_parallelism;

use crate::block::{Block, Scheduling};
use crate::config::RuntimeConfig;
use crate::operator::iteration::IterationStateLock;
use crate::operator::source::Source;
use crate::operator::{Data, Operator};
#[cfg(feature = "ssh")]
use crate::scheduler::{BlockId, Scheduler};
use crate::stream::Stream;
use crate::{BatchMode, CoordUInt};

// static LAST_REMOTE_CONFIG: Lazy<Mutex<Option<RemoteConfig>>> = Lazy::new(|| Mutex::new(None));

/// Actual content of the StreamContext. This is stored inside a `Rc` and it's shared among all
/// the blocks.
pub(crate) struct StreamContextInner {
    /// The configuration of the environment.
    pub(crate) config: RuntimeConfig,
    /// The number of blocks in the job graph, it's used to assign new ids to the blocks.
    block_count: CoordUInt,
    /// The scheduler that will start the computation. It's an option because it will be moved out
    /// of this struct when the computation starts.
    scheduler: Option<Scheduler>,
}

/// Streaming environment from which it's possible to register new streams and start the
/// computation.
///
/// This is the entrypoint for the library: construct an environment providing an
/// [`RuntimeConfig`], then you can ask new streams providing the source from where to read from.
///
/// If you want to use a distributed environment (i.e. using remote workers) you have to spawn them
/// using [`spawn_remote_workers`](StreamContext::spawn_remote_workers) before asking for some stream.
///
/// When all the stream have been registered you have to call [`execute`](StreamContext::execute_blocking) that will consume the
/// environment and start the computation. This function will return when the computation ends.
///
/// TODO: example usage
pub struct StreamContext {
    /// Reference to the actual content of the environment.
    inner: Arc<Mutex<StreamContextInner>>,
}

impl Default for StreamContext {
    fn default() -> Self {
        Self::new(RuntimeConfig::local(
            available_parallelism().map(|q| q.get()).unwrap_or(1) as u64,
        ))
    }
}

impl StreamContext {
    /// Construct a new environment from the config.
    pub fn new(config: RuntimeConfig) -> Self {
        debug!("new environment");
        StreamContext {
            inner: Arc::new(Mutex::new(StreamContextInner::new(config))),
        }
    }

    /// Construct a new stream bound to this environment starting with the specified source.
    pub fn stream<S>(&self, source: S) -> Stream<S>
    where
        S: Source + Send + 'static,
    {
        let mut inner = self.inner.lock();
        if let RuntimeConfig::Remote(remote) = &inner.config {
            assert!(remote.host_id.is_some(), "remote config must be started using RuntimeConfig::spawn_remote_workers(). (Or initialize `host_id` correctly)");
        }

        let block = inner.new_block(source, Default::default(), Default::default());
        Stream::new(self.inner.clone(), block)
    }

    /// Start the computation. Await on the returned future to actually start the computation.
    #[cfg(feature = "async-tokio")]
    pub async fn execute(self) {
        let mut env = self.inner.lock();
        info!("starting execution ({} blocks)", env.block_count);
        let scheduler = env.scheduler.take().unwrap();
        let block_count = env.block_count;
        drop(env);
        scheduler.start(block_count).await;
        info!("finished execution");
    }

    /// Start the computation. Blocks until the computation is complete.
    ///
    /// Execute on a thread or use the async version [`execute`]
    /// for non-blocking alternatives
    pub fn execute_blocking(self) {
        let mut env = self.inner.lock();
        info!("starting execution ({} blocks)", env.block_count);
        let scheduler = env.scheduler.take().unwrap();
        scheduler.start_blocking(env.block_count);
        info!("finished execution");
    }

    /// Get the total number of processing cores in the cluster.
    pub fn parallelism(&self) -> CoordUInt {
        match &self.inner.lock().config {
            RuntimeConfig::Local(local) => local.num_cores,
            RuntimeConfig::Remote(remote) => remote.hosts.iter().map(|h| h.num_cores).sum(),
        }
    }
}

impl StreamContextInner {
    fn new(config: RuntimeConfig) -> Self {
        Self {
            config: config.clone(),
            block_count: 0,
            scheduler: Some(Scheduler::new(config)),
        }
    }

    pub(crate) fn new_block<S: Source>(
        &mut self,
        source: S,
        batch_mode: BatchMode,
        iteration_ctx: Vec<Arc<IterationStateLock>>,
    ) -> Block<S> {
        let new_id = self.new_block_id();
        let replication = source.replication();
        let scheduling = Scheduling { replication };
        info!("new block (b{new_id:02}), replication {replication:?}",);
        Block::new(new_id, source, batch_mode, iteration_ctx, scheduling)
    }

    pub(crate) fn close_block<Out: Data, Op: Operator<Out = Out> + 'static>(
        &mut self,
        block: Block<Op>,
    ) -> BlockId {
        let id = block.id;
        let scheduler = self.scheduler_mut();
        scheduler.schedule_block(block);
        id
    }

    pub(crate) fn connect_blocks<Out: 'static>(&mut self, from: BlockId, to: BlockId) {
        let scheduler = self.scheduler_mut();
        scheduler.connect_blocks(from, to, TypeId::of::<Out>());
    }

    pub(crate) fn clone_block<Op: Operator>(&mut self, block: &Block<Op>) -> Block<Op> {
        let mut new_block = block.clone();
        new_block.id = self.new_block_id();

        let prev_nodes = self.scheduler_mut().prev_blocks(block.id).unwrap();
        for (prev_node, typ) in prev_nodes.into_iter() {
            self.scheduler_mut()
                .connect_blocks(prev_node, new_block.id, typ);
        }

        new_block
    }

    /// Allocate a new BlockId inside the environment.
    fn new_block_id(&mut self) -> BlockId {
        let new_id = self.block_count;
        self.block_count += 1;
        debug!("new block_id (b{new_id:02})");
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
