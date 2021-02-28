use async_std::task::JoinHandle;

use crate::config::{ExecutionRuntime, LocalRuntimeConfig};
use crate::environment::StreamEnvironmentInner;
use std::borrow::Borrow;

#[derive(Debug)]
pub struct ExecutionMetadata {
    // TODO: previous nodes, next nodes, index
}

pub async fn start(env: &mut StreamEnvironmentInner) -> Vec<JoinHandle<()>> {
    match env.config.runtime {
        ExecutionRuntime::Local(local) => start_local(env, local).await,
    }
}

async fn start_local(
    env: &mut StreamEnvironmentInner,
    config: LocalRuntimeConfig,
) -> Vec<JoinHandle<()>> {
    info!("Starting local environment: {:?}", config);
    let mut join = Vec::new();
    // start the execution
    for (id, handle) in env.start_handles.drain() {
        // TODO: build metadata
        let metadata = ExecutionMetadata {};
        handle.starter.send(metadata).await.unwrap();
        join.push(handle.join_handle);
    }
    join
}
