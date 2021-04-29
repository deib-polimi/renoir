use std::fmt::{Display, Formatter};
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

use crate::runner::{CONFIG_ENV_VAR, HOST_ID_ENV_VAR};
use crate::scheduler::HostId;

#[derive(Debug, Clone)]
pub struct EnvironmentConfig {
    pub(crate) runtime: ExecutionRuntime,
    /// In a remote execution this field represents the identifier of the host, i.e. the index
    /// inside the host list in the config.
    pub(crate) host_id: Option<HostId>,
}

#[derive(Debug, Clone)]
pub(crate) enum ExecutionRuntime {
    Local(LocalRuntimeConfig),
    Remote(RemoteRuntimeConfig),
}

#[derive(Debug, Clone)]
pub(crate) struct LocalRuntimeConfig {
    pub(crate) num_cores: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RemoteRuntimeConfig {
    pub(crate) hosts: Vec<RemoteHostConfig>,
    pub(crate) tracing_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RemoteHostConfig {
    pub(crate) address: String,
    pub(crate) base_port: u16,
    pub(crate) num_cores: usize,
    #[serde(default)]
    pub(crate) ssh: RemoteHostSSHConfig,
    pub(crate) perf_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Derivative)]
#[derivative(Default)]
#[allow(clippy::upper_case_acronyms)]
pub(crate) struct RemoteHostSSHConfig {
    #[derivative(Default(value = "22"))]
    #[serde(default = "ssh_default_port")]
    pub(crate) ssh_port: u16,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) key_file: Option<PathBuf>,
    pub(crate) key_passphrase: Option<String>,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "rstream2", about = "RStream on steroids!")]
struct CommandLineOptions {
    /// Enable verbose output.
    #[structopt(short, long)]
    verbose: bool,

    /// Path to the configuration file for remote execution.
    ///
    /// When this is specified the execution will be remote. This conflicts with `--threads`.
    #[structopt(short, long)]
    remote: Option<PathBuf>,

    /// Number of cores in the local execution.
    ///
    /// When this is specified the execution will be local. This conflicts with `--remote`.
    #[structopt(short, long)]
    local: Option<usize>,

    /// The rest of the arguments.
    args: Vec<String>,
}

impl EnvironmentConfig {
    /// Build the configuration from the specified args list.
    pub fn from_args() -> (EnvironmentConfig, Vec<String>) {
        let opt: CommandLineOptions = CommandLineOptions::from_args();
        opt.validate();
        if opt.verbose {
            std::env::set_var("RUST_LOG", "debug");
            let _ = env_logger::try_init();
        }
        if let Some(num_cores) = opt.local {
            (Self::local(num_cores), opt.args)
        } else if let Some(remote) = opt.remote {
            (Self::remote(remote).unwrap(), opt.args)
        } else {
            unreachable!("Invalid configuration")
        }
    }

    /// Local environment that avoid using the network and runs concurrently using only threads.
    pub fn local(num_cores: usize) -> EnvironmentConfig {
        EnvironmentConfig {
            runtime: ExecutionRuntime::Local(LocalRuntimeConfig { num_cores }),
            host_id: Some(0),
        }
    }

    /// Remote environment based on the provided configuration file.
    ///
    /// The behaviour of this changes if this process is the "runner" process (ie the one that will
    /// execute via ssh the other workers) or a worker process.
    /// If it's the runner, the configuration file is read. If it's a worker, the configuration is
    /// read directly from the environment variable and not from the file (remote hosts may not have
    /// the configuration file).
    pub fn remote<P: AsRef<Path>>(config: P) -> Result<EnvironmentConfig> {
        let config = if let Some(config) = EnvironmentConfig::config_from_env() {
            config
        } else {
            debug!("Reading remote config from: {}", config.as_ref().display());
            let content = std::fs::read_to_string(config)?;
            serde_yaml::from_str(&content)?
        };

        // validate the configuration
        for (host_id, host) in config.hosts.iter().enumerate() {
            if host.ssh.password.is_some() && host.ssh.key_file.is_some() {
                bail!("Malformed configuration: cannot specify both password and key file on host {}: {}", host_id, host.address);
            }
        }

        let host_id = EnvironmentConfig::host_id(config.hosts.len());
        debug!("Detected host id: {:?}", host_id);
        debug!("Remote runtime configuration: {:#?}", config);
        Ok(EnvironmentConfig {
            runtime: ExecutionRuntime::Remote(config),
            host_id,
        })
    }

    /// Extract the host id from the environment variable, if present.
    fn host_id(num_hosts: usize) -> Option<HostId> {
        let host_id = match std::env::var(HOST_ID_ENV_VAR) {
            Ok(host_id) => host_id,
            Err(_) => return None,
        };
        let host_id = match HostId::from_str(&host_id) {
            Ok(host_id) => host_id,
            Err(e) => panic!("Invalid value for environment {}: {:?}", HOST_ID_ENV_VAR, e),
        };
        if host_id >= num_hosts {
            panic!(
                "Invalid value for environment {}: value too large, max possible is {}",
                HOST_ID_ENV_VAR,
                num_hosts - 1
            );
        }
        Some(host_id)
    }

    /// Extract the configuration from the environment, if it's present.
    fn config_from_env() -> Option<RemoteRuntimeConfig> {
        match std::env::var(CONFIG_ENV_VAR) {
            Ok(config) => {
                info!("Reading remote config from env {}", CONFIG_ENV_VAR);
                let config: RemoteRuntimeConfig =
                    serde_yaml::from_str(&config).expect("Invalid configuration from environment");
                Some(config)
            }
            Err(_) => None,
        }
    }
}

impl Display for RemoteHostConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}:{}-]", self.address, self.base_port)
    }
}

impl CommandLineOptions {
    /// Check that the configuration provided is valid.
    fn validate(&self) {
        if !(self.remote.is_some() ^ self.local.is_some()) {
            panic!("Use one of --remote or --threads");
        }
        if let Some(threads) = self.local {
            if threads == 0 {
                panic!("The number of cores should be positive");
            }
        }
    }
}

/// Default port for ssh, used by the serde default value.
fn ssh_default_port() -> u16 {
    22
}
