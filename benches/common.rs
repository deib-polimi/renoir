#![allow(unused)]

use criterion::{black_box, Bencher};
use noir::config::{ExecutionRuntime, RemoteHostConfig, RemoteRuntimeConfig};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use noir::*;

pub const SAMPLES: usize = 20;
pub const WARM_UP: Duration = Duration::from_secs(3);
pub const DURATION: Duration = Duration::from_secs(10);

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
