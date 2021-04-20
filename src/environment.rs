use std::cell::RefCell;
use std::rc::Rc;

use crate::block::InnerBlock;
use crate::config::{EnvironmentConfig, ExecutionRuntime};
use crate::operator::source::Source;
use crate::operator::Data;
use crate::runner::spawn_remote_workers;
use crate::scheduler::Scheduler;
use crate::stream::{BlockId, Stream};

// See: check_no_double_remote_init
//
// /// Whether a remote execution has already been started. Currently you cannot start more than one
// /// remote environments.
// static mut ALREADY_INIT_REMOTE: bool = false;
// /// `Once` used to set safely `ALREADY_INIT_REMOTE`.
// static INIT_REMOTE: Once = Once::new();

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
    pub fn stream<Out: Data, S>(&mut self, source: S) -> Stream<Out, S>
    where
        S: Source<Out> + Send + 'static,
    {
        StreamEnvironmentInner::stream(self.inner.clone(), source)
    }

    /// Spawn the remote workers via SSH and exit if this is the process that should spawn. If this
    /// is already a spawned process nothing is done.
    pub fn spawn_remote_workers(&self) {
        match &self.inner.borrow().config.runtime {
            ExecutionRuntime::Local(_) => {}
            ExecutionRuntime::Remote(remote) => {
                spawn_remote_workers(remote.clone());
            }
        }
    }

    /// Start the computation. Await on the returned future to actually start the computation.
    pub fn execute(self) {
        let mut env = self.inner.borrow_mut();
        info!("Starting execution of {} blocks", env.block_count);
        env.scheduler.take().unwrap().start();
    }
}

impl StreamEnvironmentInner {
    fn new(config: EnvironmentConfig) -> Self {
        if matches!(config.runtime, ExecutionRuntime::Remote(_)) {
            StreamEnvironmentInner::check_no_double_remote_init();
        }
        Self {
            config: config.clone(),
            block_count: 0,
            scheduler: Some(Scheduler::new(config)),
        }
    }

    pub fn stream<Out: Data, S>(
        env_rc: Rc<RefCell<StreamEnvironmentInner>>,
        source: S,
    ) -> Stream<Out, S>
    where
        S: Source<Out> + Send + 'static,
    {
        let mut env = env_rc.borrow_mut();
        if matches!(env.config.runtime, ExecutionRuntime::Remote(_)) {
            // calling .spawn_remote_workers() will exit so it wont reach this point
            if env.config.host_id.is_none() {
                panic!("Call `StreamEnvironment::spawn_remote_workers` before calling stream!");
            }
        }
        let block_id = env.new_block();
        let source_max_parallelism = source.get_max_parallelism();
        info!(
            "Creating a new stream, block_id={} with max_parallelism {:?}",
            block_id, source_max_parallelism
        );
        let mut block = InnerBlock::new(block_id, source, Default::default());
        if let Some(p) = source_max_parallelism {
            block.scheduler_requirements.max_parallelism(p);
        }
        drop(env);
        Stream { block, env: env_rc }
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

    /// Make sure the environment is not construct twice with remote environments. This is currently
    /// unsupported since the two configurations may be different.
    fn check_no_double_remote_init() {
        // FIXME: for now this check is disabled because the integration tests spawn threads for
        //        the remote runtime.

        // all of these is safe since ALREADY_INIT_REMOTE will be written only once:
        //   https://doc.rust-lang.org/std/sync/struct.Once.html#examples-1
        // unsafe {
        //     assert!(
        //         !ALREADY_INIT_REMOTE,
        //         "Having multiple remote environments in the same program is not supported yet"
        //     );
        //     INIT_REMOTE.call_once(|| {
        //         ALREADY_INIT_REMOTE = true;
        //     });
        // }
    }
}
