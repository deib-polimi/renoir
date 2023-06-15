use itertools::Itertools;

use noir::operator::source::IteratorSource;
use utils::TestHelper;

mod utils;

#[test]
fn filter_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env.stream(source).filter(|x| x % 2 == 1).collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            assert_eq!(res, &[1, 3, 5, 7, 9]);
        }
    });
}

#[test]
fn filter_keyed_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|n| n % 2)
            .filter(|(_key, x)| *x < 6)
            .drop_key()
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(res, (0..6u8).collect_vec());
        }
    });
}
