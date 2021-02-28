#[derive(Debug, Clone, Copy)]
pub struct EnvironmentConfig {
    pub runtime: ExecutionRuntime,
}

#[derive(Debug, Clone, Copy)]
pub enum ExecutionRuntime {
    Local(LocalRuntimeConfig),
}

#[derive(Debug, Clone, Copy)]
pub struct LocalRuntimeConfig {
    pub num_cores: usize,
}

impl EnvironmentConfig {
    pub fn local(num_cores: usize) -> EnvironmentConfig {
        EnvironmentConfig {
            runtime: ExecutionRuntime::Local(LocalRuntimeConfig { num_cores }),
        }
    }
}
