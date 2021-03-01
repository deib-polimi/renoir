#[macro_use]
extern crate log;

use async_std::stream::from_iter;

use operator::source;

use crate::config::EnvironmentConfig;
use crate::environment::StreamEnvironment;

mod block;
mod config;
mod environment;
mod network;
mod operator;
mod scheduler;
mod stream;
mod worker;

#[async_std::main]
async fn main() {
    pretty_env_logger::init();

    let config = EnvironmentConfig::local(4);
    let mut env = StreamEnvironment::new(config);
    let source = source::StreamSource::new(from_iter(0..10));
    let stream = env
        .stream(source)
        .map(|x| x.to_string())
        .shuffle()
        .map(|s| s.len());
    let result = stream.collect_vec();
    env.execute().await;
    println!("Output: {:?}", result.get());
}
