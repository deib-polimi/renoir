use std::fmt::{Display, Formatter};
use std::str::FromStr;

use anyhow::Result;
use async_std::path::Path;
use serde::Deserialize;

use crate::scheduler::HostId;

const HOST_ID_ENV_VAR: &str = "RSTREAM_HOST_ID";

#[derive(Debug, Clone)]
pub struct EnvironmentConfig {
    pub runtime: ExecutionRuntime,
    /// In a remote execution this field represents the identifier of the host, i.e. the index
    /// inside the host list in the config.
    pub host_id: HostId,
}

#[derive(Debug, Clone)]
pub enum ExecutionRuntime {
    Local(LocalRuntimeConfig),
    Remote(RemoteRuntimeConfig),
}

#[derive(Debug, Clone)]
pub struct LocalRuntimeConfig {
    pub num_cores: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RemoteRuntimeConfig {
    pub hosts: Vec<RemoteHostConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RemoteHostConfig {
    pub address: String,
    pub base_port: u16,
    pub num_cores: usize,
    // TODO: extra ssh information for spawning
}

impl EnvironmentConfig {
    pub fn local(num_cores: usize) -> EnvironmentConfig {
        EnvironmentConfig {
            runtime: ExecutionRuntime::Local(LocalRuntimeConfig { num_cores }),
            host_id: 0,
        }
    }

    pub async fn remote<P: AsRef<Path>>(config: P) -> Result<EnvironmentConfig> {
        debug!("Reading remote config from: {}", config.as_ref().display());
        let content = async_std::fs::read_to_string(config).await?;
        let config: RemoteRuntimeConfig = serde_yaml::from_str(&content)?;
        let host_id = EnvironmentConfig::host_id();
        if host_id >= config.hosts.len() {
            panic!(
                "Invalid value for environment {}: value too large, max possible is {}",
                HOST_ID_ENV_VAR,
                config.hosts.len() - 1
            );
        }
        debug!("Detected host id: {}", host_id);
        debug!("Remote runtime configuration: {:#?}", config);
        Ok(EnvironmentConfig {
            runtime: ExecutionRuntime::Remote(config),
            host_id: EnvironmentConfig::host_id(),
        })
    }

    fn host_id() -> HostId {
        let host_id = match std::env::var(HOST_ID_ENV_VAR) {
            Ok(host_id) => host_id,
            Err(_) => panic!(
                "Remote execution without environment {} set",
                HOST_ID_ENV_VAR
            ),
        };
        match HostId::from_str(&host_id) {
            Ok(host_id) => host_id,
            Err(e) => panic!("Invalid value for environment {}: {:?}", HOST_ID_ENV_VAR, e),
        }
    }
}

impl Display for RemoteHostConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}:{}-]", self.address, self.base_port)
    }
}
