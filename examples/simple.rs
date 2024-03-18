use renoir::prelude::*;

fn main() {
    let (config, _args) = RuntimeConfig::from_args();
    config.spawn_remote_workers();
    let env = StreamContext::new(config);

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
