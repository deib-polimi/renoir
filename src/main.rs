#[macro_use]
extern crate log;

use rstream::config::EnvironmentConfig;
use rstream::environment::StreamEnvironment;
use rstream::operator::source::IteratorSource;

fn main() {
    env_logger::init();

    let (config, _args) = EnvironmentConfig::from_args();
    let mut env = StreamEnvironment::new(config);

    env.spawn_remote_workers();

    let n = 5u64;
    let n_iter = 5;

    let source = IteratorSource::new(0..n);
    let (state, res) = env.stream(source).shuffle().iterate(
        n_iter,
        0u64,
        |s, state| s.map(move |x| x + *state.get()),
        |delta: &mut u64, x| *delta += x,
        |old_state, delta| *old_state += delta,
        |_state| true,
    );
    let state = state.collect_vec();
    let res = res.collect_vec();
    env.execute();

    info!("Output: {:?}", res.get());
    info!("Output: {:?}", state.get());
}
