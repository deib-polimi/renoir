use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{bail, Result};
use async_std::path::Path;
use serde::{Deserialize, Serialize};

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RemoteHostConfig {
    pub(crate) address: String,
    pub(crate) base_port: u16,
    pub(crate) num_cores: usize,
    #[serde(default)]
    pub(crate) ssh: RemoteHostSSHConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Derivative)]
#[derivative(Default)]
pub(crate) struct RemoteHostSSHConfig {
    #[derivative(Default(value = "22"))]
    #[serde(default = "ssh_default_port")]
    pub(crate) ssh_port: u16,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) key_file: Option<PathBuf>,
    pub(crate) key_passphrase: Option<String>,
}

impl EnvironmentConfig {
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
    pub async fn remote<P: AsRef<Path>>(config: P) -> Result<EnvironmentConfig> {
        let config = if let Some(config) = EnvironmentConfig::config_from_env() {
            config
        } else {
            debug!("Reading remote config from: {}", config.as_ref().display());
            let content = async_std::fs::read_to_string(config).await?;
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

/// Default port for ssh, used by the serde default value.
fn ssh_default_port() -> u16 {
    22
}
