use noir::prelude::*;

fn main() {
    let (config, _args) = EnvironmentConfig::from_args();

    let mut env = StreamEnvironment::new(config);

    env.spawn_remote_workers();

    let source = IteratorSource::new(0..100);
    let result = env
        .stream(source)
        .group_by(|i| i % 5)
        .fold(Vec::new(), Vec::push)
        .collect_vec();
    env.execute_blocking();
    if let Some(result) = result.get() {
        println!("{result:?}");
    }
}
