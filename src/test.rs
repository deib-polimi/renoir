use std::collections::VecDeque;

use rand::Rng;

use crate::block::{BlockStructure, OperatorStructure};
use crate::config::{EnvironmentConfig, ExecutionRuntime, RemoteHostConfig, RemoteRuntimeConfig};
use crate::environment::StreamEnvironment;
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use std::sync::Arc;

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

impl TestHelper {
    fn setup() {
        let _ = env_logger::try_init();
    }

    /// Crate an environment with the specified config and run the test build by `body`.
    pub fn env_with_config<F>(config: EnvironmentConfig, body: F)
    where
        F: Fn(StreamEnvironment),
    {
        let env = StreamEnvironment::new(config);
        body(env);
    }

    /// Run the test body under a local environment.
    pub fn local_env<F>(body: F)
    where
        F: Fn(StreamEnvironment),
    {
        Self::setup();
        let config = EnvironmentConfig::local(4);
        debug!("Running test with env: {:?}", config);
        Self::env_with_config(config, body)
    }

    /// Run the test body under a simulated remote environment.
    pub fn remote_env<F>(body: F)
    where
        F: Fn(StreamEnvironment) + Send + Sync + 'static,
    {
        Self::setup();
        let num_hosts = 4;
        let mut hosts = vec![];
        for _ in 0..num_hosts {
            let base_port = rand::thread_rng().gen_range(16384..=65535);
            let num_cores = 2;
            hosts.push(RemoteHostConfig {
                address: "localhost".to_string(),
                base_port,
                num_cores,
                ssh: Default::default(),
                perf_path: None,
            });
        }
        let runtime = ExecutionRuntime::Remote(RemoteRuntimeConfig { hosts });
        debug!("Running with remote configuration: {:?}", runtime);

        let mut join_handles = vec![];
        let body = Arc::new(body);
        for host_id in 0..num_hosts {
            let config = EnvironmentConfig {
                runtime: runtime.clone(),
                host_id: Some(host_id),
            };
            let body = body.clone();
            join_handles.push(
                std::thread::Builder::new()
                    .name(format!("Test host{}", host_id))
                    .spawn(move || Self::env_with_config(config, &*body))
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
        Self::local_env(&body);
        Self::remote_env(body);
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
            StreamElement::End
        }
    }

    fn to_string(&self) -> String {
        format!("FakeOperator<{}>", std::any::type_name::<Out>())
    }

    fn structure(&self) -> BlockStructure {
        BlockStructure::default().add_operator(OperatorStructure::new::<Out, _>("FakeOperator"))
    }
}
