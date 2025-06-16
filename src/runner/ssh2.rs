use std::borrow::Cow;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::net::TcpStream;
use std::path::Path;
use std::time::Instant;

use ssh2::Session;

use super::{build_remote_command, HostExecutionResult};
use crate::config::{HostConfig, RemoteConfig};
use crate::profiler::try_parse_trace;
use crate::scheduler::HostId;

/// Size of the buffer usedahash to send the executable file via SCP.
pub(crate) const SCP_BUFFER_SIZE: usize = 512 * 1024;

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
pub(super) fn remote_worker(
    host_id: HostId,
    mut host: HostConfig,
    config: RemoteConfig,
    executable_uid: String,
) -> HostExecutionResult {
    if host.ssh.username.is_none() {
        host.ssh.username = Some(whoami::username());
    }
    info!("starting remote worker for host {}: {:?}", host_id, host);

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
    let remote_path = Path::new("/tmp/renoir/").join(format!(
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

    for line in stdout_reader.lines().map_while(Result::ok) {
        println!("{host_id}|{line}");
    }

    // copy to stderr the output of the remote process
    for line in stderr_reader.lines().map_while(Result::ok) {
        if let Some(trace) = try_parse_trace(&line) {
            tracing_data = Some(trace);
        } else {
            eprintln!("{host_id}|{line}");
        }
    }

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

    let (msg, result) = run_remote_command(session, "mkdir -p /tmp/renoir");
    if result != 0 {
        warn!("failed to create /tmp/renoir directory [{result}]: {msg}");
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
