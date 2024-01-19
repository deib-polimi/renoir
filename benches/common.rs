#![allow(unused)]

use criterion::{black_box, Bencher};
use noir_compute::config::{ExecutionRuntime, RemoteHostConfig, RemoteRuntimeConfig};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use noir_compute::*;

pub const SAMPLES: usize = 50;

static NONCE: AtomicU16 = AtomicU16::new(1);
const PORT_BASE: u16 = 9090;

pub fn remote_loopback_deploy(
    num_hosts: CoordUInt,
    cores_per_host: CoordUInt,
    body: impl Fn(&mut StreamEnvironment) + Send + Sync + 'static,
) {
    let mut hosts = vec![];
    for host_id in 0..num_hosts {
        let test_id = NONCE.fetch_add(1, Ordering::SeqCst);
        let [hi, lo] = test_id.to_be_bytes();
        let address = format!("127.{hi}.{lo}.{host_id}");
        hosts.push(RemoteHostConfig {
            address,
            base_port: PORT_BASE,
            num_cores: cores_per_host,
            ssh: Default::default(),
            perf_path: None,
        });
    }

    let runtime = ExecutionRuntime::Remote(RemoteRuntimeConfig {
        hosts,
        tracing_dir: None,
        cleanup_executable: false,
    });

    let mut join_handles = vec![];
    let body = Arc::new(body);
    for host_id in 0..num_hosts {
        let config = EnvironmentConfig {
            runtime: runtime.clone(),
            host_id: Some(host_id),
            skip_single_remote_check: true,
        };
        let body = body.clone();
        join_handles.push(
            std::thread::Builder::new()
                .name(format!("lohost-{host_id:02}"))
                .spawn(move || {
                    let mut env = StreamEnvironment::new(config);
                    body(&mut env);
                    env.execute_blocking();
                })
                .unwrap(),
        )
    }
    for (host_id, handle) in join_handles.into_iter().enumerate() {
        handle
            .join()
            .unwrap_or_else(|e| panic!("Remote worker for host {host_id} crashed: {e:?}"));
    }
}

pub struct NoirBenchBuilder<F, G, R>
where
    F: Fn() -> StreamEnvironment,
    G: Fn(&mut StreamEnvironment) -> R,
{
    make_env: F,
    make_network: G,
    _result: PhantomData<R>,
}

impl<F, G, R> NoirBenchBuilder<F, G, R>
where
    F: Fn() -> StreamEnvironment,
    G: Fn(&mut StreamEnvironment) -> R,
{
    pub fn new(make_env: F, make_network: G) -> Self {
        Self {
            make_env,
            make_network,
            _result: Default::default(),
        }
    }

    pub fn bench(&self, n: u64) -> Duration {
        let mut time = Duration::default();
        for _ in 0..n {
            let mut env = (self.make_env)();
            let _result = (self.make_network)(&mut env);
            let start = Instant::now();
            env.execute_blocking();
            time += start.elapsed();
            black_box(_result);
        }
        time
    }
}

pub fn noir_bench_default(b: &mut Bencher, logic: impl Fn(&mut StreamEnvironment)) {
    let builder = NoirBenchBuilder::new(StreamEnvironment::default, logic);
    b.iter_custom(|n| builder.bench(n));
}
