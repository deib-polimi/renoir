use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};

use itertools::Itertools;
use sha2::Digest;
#[cfg(feature = "ssh")]
use ssh2::Session;

use crate::config::CONFIG_ENV_VAR;
use crate::config::HOST_ID_ENV_VAR;
use crate::config::{RemoteHostConfig, RemoteRuntimeConfig};
use crate::scheduler::HostId;
use crate::TracingData;

/// Size of the buffer usedahash to send the executable file via SCP.
pub(crate) const SCP_BUFFER_SIZE: usize = 512 * 1024;

/// Execution results returned by a remote worker.
struct HostExecutionResult {
    /// Tracing data if noir is compiled with tracing enabled.
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
pub(crate) fn spawn_remote_workers(config: RemoteRuntimeConfig) {
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
        if let Some(data) = result.tracing {
            tracing_data += data;
        }
    }
    if let Some(path) = config.tracing_dir {
        std::fs::create_dir_all(&path).expect("Cannot create tracing directory");
        let now = chrono::Local::now();
        let file_name = format!("{}.json", now.format("%Y-%m-%d-%H%M%S"));
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

/// Spawn the remote worker.
///
/// - Connect via SSH to the remote host
/// - Ask for a temporary file with `mktemp`
/// - Send the local executable using SCP
/// - Make it executable using `chmod`
/// - Spawn the worker setting the correct environment variables
/// - Redirect the remote stderr to the local one
/// - Remove the remote file on exit
///
/// This function is allowed to block (i.e. not be asynchronous) since it will be run inside a
/// `spawn_blocking`.
fn remote_worker(
    host_id: HostId,
    mut host: RemoteHostConfig,
    config: RemoteRuntimeConfig,
    executable_uid: String,
) -> HostExecutionResult {
    if host.ssh.username.is_none() {
        host.ssh.username = Some(whoami::username());
    }
    info!("starting remote worker for host {}: {:#?}", host_id, host);

    // connect to the ssh server
    let address = (host.address.as_str(), host.ssh.ssh_port);
    let stream = TcpStream::connect(address).unwrap_or_else(|e| {
        panic!(
            "Failed to connect to remote SSH for host {} at {} port {}: {:?}",
            host_id, host.address, host.ssh.ssh_port, e
        )
    });
    let mut session = Session::new().unwrap();
    session.set_tcp_stream(stream);
    session.handshake().unwrap();
    log::debug!(
        "connected to ssh server for host {}: {:?}",
        host_id,
        address
    );

    // try to authenticate
    let username = host.ssh.username.as_ref().unwrap().as_str();
    match (host.ssh.password.as_ref(), host.ssh.key_file.as_ref()) {
        (None, None) => {
            session.userauth_agent(username).unwrap();
        }
        (Some(password), None) => {
            session
                .userauth_password(username, password.as_str())
                .unwrap();
        }
        (None, Some(key_file)) => session
            .userauth_pubkey_file(
                username,
                None,
                key_file.as_path(),
                host.ssh.key_passphrase.as_deref(),
            )
            .unwrap(),
        (Some(_), Some(_)) => unreachable!("Cannot use both password and key"),
    }
    assert!(
        session.authenticated(),
        "Failed to authenticate to remote host {host_id} at {address:?}"
    );
    log::debug!("authentication succeeded to host {}", host_id);

    let sync_start = Instant::now();

    let current_exe = std::env::current_exe().unwrap();
    log::debug!("executable located at {}", current_exe.display());

    // generate a temporary file on remote host
    let remote_path = Path::new("/tmp/noir/").join(format!(
        "{}-{}",
        current_exe.file_name().unwrap().to_string_lossy(),
        executable_uid
    ));
    log::debug!(
        "executable destination for host {}: {}",
        host_id,
        remote_path.display()
    );

    send_executable(
        host_id,
        &mut session,
        &current_exe,
        Path::new(&remote_path),
        0o500,
    );
    let sync_time = sync_start.elapsed();

    // build the remote command
    let command = build_remote_command(host_id, &config, &remote_path, &host.perf_path);
    log::debug!("executing on host {}:\n{}", host_id, command);

    let execution_start = Instant::now();
    let mut channel = session.channel_session().unwrap();
    channel.exec(&command).unwrap();

    let stderr_reader = BufReader::new(channel.stderr());
    let stdout_reader = BufReader::new(&mut channel);

    let mut tracing_data = None;

    std::thread::scope(|s| {
        s.spawn(|| {
            for l in stdout_reader.lines() {
                println!(
                    "{}|{}",
                    host_id,
                    l.unwrap_or_else(|e| format!("ERROR: {e}"))
                );
            }
        });
        s.spawn(|| {
            // copy to stderr the output of the remote process
            for line in stderr_reader.lines().flatten() {
                if let Some(pos) = line.find("__noir2_TRACING_DATA__") {
                    let json_data = &line[(pos + "__noir2_TRACING_DATA__ ".len())..];
                    match serde_json::from_str(json_data) {
                        Ok(data) => tracing_data = Some(data),
                        Err(err) => {
                            error!("Corrupted tracing data from host {}: {:?}", host_id, err);
                        }
                    }
                } else {
                    eprintln!("{host_id}|{line}");
                }
            }
        });
    });

    channel.wait_close().unwrap();
    let exit_code = channel.exit_status().unwrap();
    info!("{}|Exit status: {}", host_id, exit_code);

    let execution_time = execution_start.elapsed();

    if config.cleanup_executable {
        log::debug!(
            "Removing temporary binary file at host {}: {}",
            host_id,
            remote_path.display()
        );
        let remove_binary = format!(
            "rm -f {}",
            shell_escape::escape(Cow::Borrowed(
                remote_path.to_str().expect("non UTF-8 executable path")
            ))
        );
        let (_, exit_code) = run_remote_command(&mut session, &remove_binary);
        assert_eq!(
            exit_code,
            0,
            "Failed to remove remote executable on host {} at {}",
            host_id,
            remote_path.display()
        );
    }

    HostExecutionResult {
        tracing: tracing_data,
        execution_time,
        sync_time,
        exit_code,
    }
}

/// Execute a command remotely and return the standard output and the exit code.
fn run_remote_command(session: &mut Session, command: &str) -> (String, i32) {
    log::debug!("remote command: {}", command);
    let mut channel = session.channel_session().unwrap();
    channel.exec(command).unwrap();
    let mut stdout = String::new();
    channel.read_to_string(&mut stdout).unwrap();
    channel.wait_close().unwrap();
    let exit_code = channel.exit_status().unwrap();
    (stdout, exit_code)
}

/// Send a file remotely via SCP and change its mode.
fn send_executable(
    host_id: HostId,
    session: &mut Session,
    local_path: &Path,
    remote_path: &Path,
    mode: i32,
) {
    let remote_path_str = remote_path.to_str().expect("non UTF-8 executable path");
    let metadata = local_path.metadata().unwrap();
    log::debug!(
        "sending executable to host {}: {} -> {}, {} bytes",
        host_id,
        local_path.display(),
        remote_path.display(),
        metadata.len()
    );

    let (_, result) = run_remote_command(session, &format!("ls {remote_path_str}",));
    if result == 0 {
        debug!(
            "remote file with matching hash `{}` already exists, skipping transfer.",
            remote_path_str
        );
        return;
    }

    let (msg, result) = run_remote_command(session, "mkdir -p /tmp/noir");
    if result != 0 {
        warn!("failed to create /tmp/noir directory [{result}]: {msg}");
    }

    let mut local_file = File::open(local_path).unwrap();
    let mut remote_file = session
        .scp_send(remote_path, mode, metadata.len(), None)
        .unwrap();

    let mut buffer = [0u8; SCP_BUFFER_SIZE];
    while let Ok(n) = local_file.read(&mut buffer) {
        if n == 0 {
            break;
        }
        remote_file.write_all(&buffer[0..n]).unwrap();
    }
    remote_file.send_eof().unwrap();
    remote_file.wait_eof().unwrap();
    remote_file.close().unwrap();
    remote_file.wait_close().unwrap();

    log::info!("sent executable to host {}", host_id,);

    // setting the file mode using scp_send seems unreliable
    let chmod = format!(
        "chmod {:03o} {}",
        mode,
        shell_escape::escape(remote_path_str.into())
    );
    run_remote_command(session, &chmod);
}

/// Build the command for running the remote worker.
///
/// This will export all the required variables before executing the binary.
fn build_remote_command(
    host_id: HostId,
    config: &RemoteRuntimeConfig,
    binary_path: &Path,
    perf_path: &Option<PathBuf>,
) -> String {
    let config_yaml = serde_yaml::to_string(config).unwrap();
    let config_str = shell_escape::escape(config_yaml.into());
    let args = std::env::args()
        .skip(1)
        .map(|arg| shell_escape::escape(arg.into()))
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
