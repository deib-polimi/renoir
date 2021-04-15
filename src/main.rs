#[macro_use]
extern crate log;

use rstream::config::EnvironmentConfig;
use rstream::environment::StreamEnvironment;
use rstream::operator::source;

fn main() {
    env_logger::init();

    let (config, _args) = EnvironmentConfig::from_args();
    let mut env = StreamEnvironment::new(config);

    env.spawn_remote_workers();

    let source = source::IteratorSource::new(90..110u8);
    let stream = env
        .stream(source)
        .map(|x| x.to_string())
        .shuffle()
        .group_by(|s| s.len())
        .map(|(_k, s)| s.len())
        .unkey();
    let result = stream.collect_vec();

    env.execute();

    info!("Output: {:?}", result.get());
}
