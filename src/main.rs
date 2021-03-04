#[macro_use]
extern crate derivative;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;

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

    // let config = EnvironmentConfig::local(4);
    let config = EnvironmentConfig::remote("config.yml").await.unwrap();
    let mut env = StreamEnvironment::new(config);
    let source = source::StreamSource::new(from_iter(90..110));
    let stream = env
        .stream(source)
        .map(|x| x.to_string())
        .shuffle()
        .group_by(|s| s.len())
        .map(|(_k, s)| s.len())
        .unkey();
    let result = stream.collect_vec();
    env.execute().await;
    println!("Output: {:?}", result.get());
}
