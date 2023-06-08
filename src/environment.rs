use micrometer::Span;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::any::TypeId;
use std::sync::Arc;
use std::thread::available_parallelism;

use crate::block::Block;
use crate::config::{EnvironmentConfig, ExecutionRuntime, RemoteRuntimeConfig};
use crate::operator::iteration::IterationStateLock;
use crate::operator::source::Source;
use crate::operator::{Data, Operator};
#[cfg(feature = "ssh")]
use crate::runner::spawn_remote_workers;
use crate::scheduler::{BlockId, Scheduler};
use crate::stream::Stream;
use crate::{BatchMode, CoordUInt};

static LAST_REMOTE_CONFIG: Lazy<Mutex<Option<RemoteRuntimeConfig>>> =
    Lazy::new(|| Mutex::new(None));

/// Actual content of the StreamEnvironment. This is stored inside a `Rc` and it's shared among all
/// the blocks.
pub(crate) struct StreamEnvironmentInner {
    /// The configuration of the environment.
    pub(crate) config: EnvironmentConfig,
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
/// [`EnvironmentConfig`], then you can ask new streams providing the source from where to read from.
///
/// If you want to use a distributed environment (i.e. using remote workers) you have to spawn them
/// using [`spawn_remote_workers`](StreamEnvironment::spawn_remote_workers) before asking for some stream.
///
/// When all the stream have been registered you have to call [`execute`](StreamEnvironment::execute) that will consume the
/// environment and start the computation. This function will return when the computation ends.
///
/// TODO: example usage
pub struct StreamEnvironment {
    /// Reference to the actual content of the environment.
    inner: Arc<Mutex<StreamEnvironmentInner>>,
    /// Measure the time for building the graph.
    build_time: Span<'static>,
}

impl Default for StreamEnvironment {
    fn default() -> Self {
        Self::new(EnvironmentConfig::local(
            available_parallelism().map(|q| q.get()).unwrap_or(1) as u64,
        ))
    }
}

impl StreamEnvironment {
    /// Construct a new environment from the config.
    pub fn new(config: EnvironmentConfig) -> Self {
        debug!("new environment");
        if !config.skip_single_remote_check {
            Self::single_remote_environment_check(&config);
        }
        micrometer::span!(noir_build_time);
        StreamEnvironment {
            inner: Arc::new(Mutex::new(StreamEnvironmentInner::new(config))),
            build_time: noir_build_time,
        }
    }

    /// Construct a new stream bound to this environment starting with the specified source.
    pub fn stream<Out: Data, S>(&mut self, source: S) -> Stream<Out, S>
    where
        S: Source<Out> + Send + 'static,
    {
        let inner = self.inner.lock();
        let config = &inner.config;
        if config.host_id.is_none() {
            match config.runtime {
                ExecutionRuntime::Remote(_) => {
                    panic!(
                        "Call StreamEnvironment::spawn_remote_workers() before calling ::stream()"
                    );
                }
                ExecutionRuntime::Local(_) => {
                    unreachable!("Local environments do not need an host_id");
                }
            }
        }
        drop(inner);
        StreamEnvironmentInner::stream(self.inner.clone(), source)
    }

    /// Spawn the remote workers via SSH and exit if this is the process that should spawn. If this
    /// is already a spawned process nothing is done.
    pub fn spawn_remote_workers(&self) {
        match &self.inner.lock().config.runtime {
            ExecutionRuntime::Local(_) => {}
            #[cfg(feature = "ssh")]
            ExecutionRuntime::Remote(remote) => {
                spawn_remote_workers(remote.clone());
            }
            #[cfg(not(feature = "ssh"))]
            ExecutionRuntime::Remote(_) => {
                panic!("spawn_remote_workers() requires the `ssh` feature for remote configs.");
            }
        }
    }

    /// Start the computation. Await on the returned future to actually start the computation.
    #[cfg(feature = "async-tokio")]
    pub async fn execute(self) {
        drop(self.build_time);
        micrometer::span!("noir_execution");
        let mut env = self.inner.lock();
        info!("starting execution ({} blocks)", env.block_count);
        let scheduler = env.scheduler.take().unwrap();
        scheduler.start(env.block_count).await;
    }

    /// Start the computation. Await on the returned future to actually start the computation.
    #[cfg(not(feature = "async-tokio"))]
    pub fn execute(self) {
        drop(self.build_time);
        micrometer::span!("noir_execution");
        let mut env = self.inner.lock();
        info!("starting execution ({} blocks)", env.block_count);
        let scheduler = env.scheduler.take().unwrap();
        scheduler.start(env.block_count);
    }

    /// Get the total number of processing cores in the cluster.
    pub fn parallelism(&self) -> CoordUInt {
        match &self.inner.lock().config.runtime {
            ExecutionRuntime::Local(local) => local.num_cores,
            ExecutionRuntime::Remote(remote) => remote.hosts.iter().map(|h| h.num_cores).sum(),
        }
    }

    /// Make sure that, if an environment with a remote configuration is built, all the following
    /// remote environments use the same config.
    fn single_remote_environment_check(config: &EnvironmentConfig) {
        if let ExecutionRuntime::Remote(config) = &config.runtime {
            let mut prev = LAST_REMOTE_CONFIG.lock();
            match &*prev {
                Some(prev) => {
                    if prev != config {
                        panic!("Spawning remote runtimes with different configurations is not supported");
                    }
                }
                None => {
                    *prev = Some(config.clone());
                }
            }
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

    pub fn stream<Out: Data, S>(
        env_rc: Arc<Mutex<StreamEnvironmentInner>>,
        source: S,
    ) -> Stream<Out, S>
    where
        S: Source<Out> + Send + 'static,
    {
        let mut env = env_rc.lock();
        if matches!(env.config.runtime, ExecutionRuntime::Remote(_)) {
            // calling .spawn_remote_workers() will exit so it wont reach this point
            if env.config.host_id.is_none() {
                panic!("Call `StreamEnvironment::spawn_remote_workers` before calling stream!");
            }
        }

        let source_replication = source.replication();
        let mut block = env.new_block(source, Default::default(), Default::default());

        block.scheduler_requirements.replication(source_replication);
        drop(env);
        Stream { block, env: env_rc }
    }

    pub(crate) fn new_block<Out: Data, S: Source<Out>>(
        &mut self,
        source: S,
        batch_mode: BatchMode,
        iteration_ctx: Vec<Arc<IterationStateLock>>,
    ) -> Block<Out, S> {
        let new_id = self.new_block_id();
        let parallelism = source.replication();
        info!("new block (b{new_id:02}), replication {parallelism:?}",);
        Block::new(new_id, source, batch_mode, iteration_ctx)
    }

    pub(crate) fn close_block<Out: Data, Op: Operator<Out> + 'static>(
        &mut self,
        block: Block<Out, Op>,
    ) -> BlockId {
        let id = block.id;
        let scheduler = self.scheduler_mut();
        scheduler.schedule_block(block);
        id
    }

    pub(crate) fn connect_blocks<Out: Data>(&mut self, from: BlockId, to: BlockId) {
        let scheduler = self.scheduler_mut();
        scheduler.connect_blocks(from, to, TypeId::of::<Out>());
    }

    /// Allocate a new BlockId inside the environment.
    pub(crate) fn new_block_id(&mut self) -> BlockId {
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
