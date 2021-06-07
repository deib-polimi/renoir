#[macro_use]
extern crate log;

use rstream::operator::source::IteratorSource;
use rstream::EnvironmentConfig;
use rstream::StreamEnvironment;

fn main() {
    env_logger::init();

    let (config, _args) = EnvironmentConfig::from_args();
    let mut env = StreamEnvironment::new(config);

    env.spawn_remote_workers();

    let s1 = env.stream(IteratorSource::new(0..5u64));
    let s2 = env.stream(IteratorSource::new(0..5i32));
    let res = s1
        .join_with(s2, |n| (*n % 2) as u8, |n| (*n % 2) as u8)
        .ship_hash()
        .local_hash()
        .inner()
        .collect_vec();

    env.execute();

    if let Some(res) = res.get() {
        for (k, (lhs, rhs)) in res {
            info!("key {} -> {:?} {:?}", k, lhs, rhs);
        }
    }
}
