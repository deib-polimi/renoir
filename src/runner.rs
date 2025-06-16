use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};

use sha2::Digest;

use crate::config::RemoteConfig;
use crate::config::CONFIG_ENV_VAR;
use crate::config::HOST_ID_ENV_VAR;
use crate::profiler::TracingData;
use crate::scheduler::HostId;

#[cfg(feature = "ssh")]
mod ssh2;
#[cfg(feature = "ssh")]
use ssh2::remote_worker;

/// Execution results returned by a remote worker.
struct HostExecutionResult {
    /// Tracing data if renoir is compiled with tracing enabled.
    tracing: Option<TracingData>,
    /// Time spent for sending the binary file to the remote worker.
    sync_time: Duration,
    /// Execution time excluding the sync.
    execution_time: Duration,
    /// Worker process exit code.
    exit_code: i32,
}

/// Compute a cryptographic hash digest of the current executable and return it as a string.
/// Intended as a discrimintaor for file changes
fn executable_hash() -> String {
    let mut hasher = sha2::Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    let mut f = File::open(std::env::current_exe().unwrap()).unwrap();

    loop {
        match f.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => hasher.update(&buf[..n]),
            Err(e) => panic!("Error reading the current executable! {e}"),
        }
    }

    let digest = hasher.finalize();
    B64.encode(digest)
}

/// Spawn all the remote workers via ssh and wait until all of them complete, after that exit from
/// the process,
///
/// If this was already a spawned process to nothing.
pub(crate) fn spawn_remote_workers(config: RemoteConfig) {
    // if this process already comes from a the spawner do not spawn again!
    if is_spawned_process() {
        return;
    }

    // from now we are sure this is the process that should spawn the remote workers
    info!("starting {} remote workers", config.hosts.len());

    let start = Instant::now();
    let exe_hash = executable_hash();
    let mut join_handles = Vec::new();
    let mut host_dup: HashMap<String, usize> = HashMap::new(); // Used to detect deployments with replicated host
    for (host_id, host) in config.hosts.iter().enumerate() {
        let mut exe_uid = exe_hash.clone();
        let ctr = host_dup.entry(host.address.clone()).or_default();
        if *ctr > 0 {
            write!(&mut exe_uid, "-{:02}", *ctr).unwrap();
        }
        *ctr += 1;

        let config = config.clone();
        let host = host.clone();
        let join_handle = std::thread::Builder::new()
            .name(format!("remote-{host_id:02}",))
            .spawn(move || remote_worker(host_id as _, host, config, exe_uid))
            .unwrap();
        join_handles.push(join_handle);
    }
    let mut tracing_data = TracingData::default();
    let mut max_execution_time = Duration::default();
    let mut max_sync_time = Duration::default();
    let mut exit_code_or = 0;
    for join_handle in join_handles {
        let result = join_handle.join().unwrap();
        max_execution_time = max_execution_time.max(result.execution_time);
        max_sync_time = max_sync_time.max(result.sync_time);
        exit_code_or |= result.exit_code;
        if let Some(mut data) = result.tracing {
            tracing_data.structures.append(&mut data.structures);
            tracing_data.profilers.append(&mut data.profilers);
        }
    }
    if let Some(path) = config.tracing_dir {
        std::fs::create_dir_all(&path).expect("Cannot create tracing directory");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        let file_name = format!("renoir-trace-{}.json", now.as_secs());
        let target = path.join(file_name);
        let mut target = std::fs::File::create(target).expect("Cannot create tracing json file");
        serde_json::to_writer(&mut target, &tracing_data)
            .expect("Failed to write tracing json file");
    }

    log::info!("total time: {:?}", start.elapsed());
    log::info!("max execution time: {max_execution_time:?}");
    log::info!("max sync time: {max_sync_time:?}");

    // all the remote processes have finished, exit to avoid running the environment inside the
    // spawner process
    std::process::exit(exit_code_or);
}

/// Check if this is a spawned process.
fn is_spawned_process() -> bool {
    std::env::var_os(HOST_ID_ENV_VAR).is_some()
}

/// Build the command for running the remote worker.
///
/// This will export all the required variables before executing the binary.
fn build_remote_command(
    host_id: HostId,
    config: &RemoteConfig,
    binary_path: &Path,
    perf_path: &Option<PathBuf>,
) -> String {
    let config_toml = toml::to_string(config).unwrap();
    let config_str = shell_escape::escape(config_toml.into());
    let args = std::env::args()
        .skip(1)
        .map(|arg| shell_escape::escape(arg.into()))
        .collect::<Vec<_>>()
        .join(" ");
    let perf_cmd = if let Some(path) = perf_path.as_ref() {
        warn!("Running remote process on host {} with perf enabled. This may cause performance regressions.", host_id);
        format!(
            "perf record --call-graph dwarf -o {} -- ",
            shell_escape::escape(path.to_str().expect("non UTF-8 perf path").into())
        )
    } else {
        "".to_string()
    };
    format!(
        "export {host_id_env}={host_id};
export {config_env}={config};
export RUST_LOG={rust_log};
export RUST_BACKTRACE={rust_backtrace};
export RUST_LOG_STYLE=always;
{perf_cmd}{binary_path} {args}",
        host_id_env = HOST_ID_ENV_VAR,
        host_id = host_id,
        config_env = CONFIG_ENV_VAR,
        config = config_str,
        perf_cmd = perf_cmd,
        binary_path = binary_path.to_str().expect("non UTF-8 executable path"),
        args = args,
        rust_log = std::env::var("RUST_LOG").unwrap_or_default(),
        rust_backtrace = std::env::var("RUST_BACKTRACE").unwrap_or_default(),
    )
}
