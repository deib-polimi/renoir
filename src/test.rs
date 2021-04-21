use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::block::{BlockStructure, OperatorStructure};
use crate::config::{EnvironmentConfig, ExecutionRuntime, RemoteHostConfig, RemoteRuntimeConfig};
use crate::environment::StreamEnvironment;
use crate::operator::source::Source;
use crate::operator::{Data, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;
use itertools::{process_results, Itertools};
use std::str::FromStr;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

/// Port from which the integration tests start allocating sockets for the remote runtime.
const TEST_BASE_PORT: u16 = 17666;
/// How many ports to allocate for each host.
const PORTS_PER_HOST: u16 = 100;

lazy_static! {
    /// The first available port for the next host.
    static ref PORT_INDEX: AtomicU16 = AtomicU16::new(TEST_BASE_PORT);
}

/// Helper functions for running the integration tests.
///
/// For now this is in the public undocumented API and not in the integration test crate because it
/// has to craft the configuration as they come from the network.
pub struct TestHelper;

/// A fake operator that can be used to unit-test the operators.
#[derive(Debug, Clone)]
pub struct FakeOperator<Out: Data> {
    /// The data to return from `next()`.
    buffer: VecDeque<StreamElement<Out>>,
}

/// A fake operator that makes sure the watermarks are consistent with the data.
///
/// This will check that no data has a timestamp lower or equal than a previous watermark. This also
/// keeps track of the number of watermarks received.
#[derive(Clone)]
pub struct WatermarkChecker<Out: Data, PreviousOperator>
where
    PreviousOperator: Operator<Out>,
{
    last_watermark: Option<Timestamp>,
    prev: PreviousOperator,
    received_watermarks: Arc<AtomicUsize>,
    _out: PhantomData<Out>,
}

impl TestHelper {
    fn setup() {
        let _ = env_logger::Builder::new()
            .filter(None, log::LevelFilter::Debug)
            .is_test(true)
            .try_init();
    }

    /// Crate an environment with the specified config and run the test build by `body`.
    pub fn env_with_config(
        config: EnvironmentConfig,
        body: Arc<dyn Fn(StreamEnvironment) + Send + Sync>,
    ) {
        let timeout_sec = Self::parse_int_from_env("RSTREAM_TEST_TIMEOUT").unwrap_or(10);
        let timeout = Duration::from_secs(timeout_sec);
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("Worker".into())
            .spawn(move || {
                let env = StreamEnvironment::new(config);
                body(env);
                sender.send(()).unwrap();
            })
            .unwrap();
        match receiver.recv_timeout(timeout) {
            Ok(_) => {}
            Err(RecvTimeoutError::Timeout) => {
                panic!(
                    "Worker thread didn't complete before the timeout of {:?}",
                    timeout
                );
            }
            Err(RecvTimeoutError::Disconnected) => {
                panic!("Worker thread has panicked!");
            }
        }
        worker.join().expect("Worker thread has panicked!");
    }

    /// Run the test body under a local environment.
    pub fn local_env(body: Arc<dyn Fn(StreamEnvironment) + Send + Sync>, num_cores: usize) {
        Self::setup();
        let config = EnvironmentConfig::local(num_cores);
        debug!("Running test with env: {:?}", config);
        Self::env_with_config(config, body)
    }

    /// Run the test body under a simulated remote environment.
    pub fn remote_env(
        body: Arc<dyn Fn(StreamEnvironment) + Send + Sync>,
        num_hosts: usize,
        cores_per_host: usize,
    ) {
        Self::setup();
        let mut hosts = vec![];
        for _ in 0..num_hosts {
            let base_port = PORT_INDEX.fetch_add(PORTS_PER_HOST, Ordering::SeqCst);
            hosts.push(RemoteHostConfig {
                address: "localhost".to_string(),
                base_port,
                num_cores: cores_per_host,
                ssh: Default::default(),
                perf_path: None,
            });
        }
        let runtime = ExecutionRuntime::Remote(RemoteRuntimeConfig { hosts });
        debug!("Running with remote configuration: {:?}", runtime);

        let mut join_handles = vec![];
        for host_id in 0..num_hosts {
            let config = EnvironmentConfig {
                runtime: runtime.clone(),
                host_id: Some(host_id),
            };
            let body = body.clone();
            join_handles.push(
                std::thread::Builder::new()
                    .name(format!("Test host{}", host_id))
                    .spawn(move || Self::env_with_config(config, body))
                    .unwrap(),
            )
        }
        for (host_id, handle) in join_handles.into_iter().enumerate() {
            handle
                .join()
                .unwrap_or_else(|e| panic!("Remote worker for host {} crashed: {:?}", host_id, e));
        }
    }

    /// Run the test body under a local environment and later under a simulated remote environment.
    pub fn local_remote_env<F>(body: F)
    where
        F: Fn(StreamEnvironment) + Send + Sync + 'static,
    {
        let body = Arc::new(body);

        let local_cores =
            Self::parse_list_from_env("RSTREAM_TEST_LOCAL_CORES").unwrap_or_else(|| vec![4]);
        for num_cores in local_cores {
            Self::local_env(body.clone(), num_cores);
        }

        let remote_hosts =
            Self::parse_list_from_env("RSTREAM_TEST_REMOTE_HOSTS").unwrap_or_else(|| vec![4]);
        let remote_cores =
            Self::parse_list_from_env("RSTREAM_TEST_REMOTE_CORES").unwrap_or_else(|| vec![4]);
        for num_hosts in remote_hosts {
            for &num_cores in &remote_cores {
                Self::remote_env(body.clone(), num_hosts, num_cores);
            }
        }
    }

    /// Parse a list of arguments from an environment variable.
    ///
    /// The list should be comma separated without spaces.
    fn parse_list_from_env(var_name: &str) -> Option<Vec<usize>> {
        let content = std::env::var(var_name).ok()?;
        let values = content.split(',').map(|s| usize::from_str(s)).collect_vec();
        process_results(values.into_iter(), |values| values.collect_vec()).ok()
    }

    fn parse_int_from_env(var_name: &str) -> Option<u64> {
        let content = std::env::var(var_name).ok()?;
        u64::from_str(&content).ok()
    }
}

impl<Out: Data> FakeOperator<Out> {
    /// Create an empty `FakeOperator`.
    pub fn empty() -> Self {
        Self {
            buffer: Default::default(),
        }
    }

    /// Create a `FakeOperator` with the specified data.
    pub fn new<I: Iterator<Item = Out>>(data: I) -> Self {
        Self {
            buffer: data.map(StreamElement::Item).collect(),
        }
    }

    /// Add an element to the end of the list of elements to return from `next`.
    pub fn push(&mut self, item: StreamElement<Out>) {
        self.buffer.push_back(item);
    }
}

impl<Out: Data> Operator<Out> for FakeOperator<Out> {
    fn setup(&mut self, _metadata: ExecutionMetadata) {}

    fn next(&mut self) -> StreamElement<Out> {
        if let Some(item) = self.buffer.pop_front() {
            item
        } else {
            StreamElement::Terminate
        }
    }

    fn to_string(&self) -> String {
        format!("FakeOperator<{}>", std::any::type_name::<Out>())
    }

    fn structure(&self) -> BlockStructure {
        BlockStructure::default().add_operator(OperatorStructure::new::<Out, _>("FakeOperator"))
    }
}

impl<Out: Data> Source<Out> for FakeOperator<Out> {
    fn get_max_parallelism(&self) -> Option<usize> {
        None
    }
}

impl<Out: Data, PreviousOperator: Operator<Out>> WatermarkChecker<Out, PreviousOperator> {
    pub fn new(prev: PreviousOperator, received_watermarks: Arc<AtomicUsize>) -> Self {
        Self {
            last_watermark: None,
            prev,
            received_watermarks,
            _out: Default::default(),
        }
    }
}

impl<Out: Data, PreviousOperator: Operator<Out>> Operator<Out>
    for WatermarkChecker<Out, PreviousOperator>
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Out> {
        let item = self.prev.next();
        match &item {
            StreamElement::Timestamped(_, ts) => {
                if let Some(w) = &self.last_watermark {
                    assert!(ts > w);
                }
            }
            StreamElement::Watermark(ts) => {
                if let Some(w) = &self.last_watermark {
                    assert!(ts > w);
                }
                self.last_watermark = Some(*ts);
                self.received_watermarks.fetch_add(1, Ordering::Release);
            }
            _ => {}
        }
        item
    }

    fn to_string(&self) -> String {
        String::from("WatermarkController")
    }

    fn structure(&self) -> BlockStructure {
        Default::default()
    }
}
