use itertools::Itertools;

use noir::operator::source::IteratorSource;
use utils::TestHelper;

mod utils;

#[test]
fn group_by_reduce_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u32);
        let res = env
            .stream(source)
            .map(|x| x.to_string())
            .group_by_reduce(|n| n.parse::<u32>().unwrap() % 2, |acc, y| *acc += &y)
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            let res = res.into_iter().sorted().collect_vec();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0], (0, "02468".into()));
            assert_eq!(res[1], (1, "13579".into()));
        }
    });
}

#[test]
fn reduce_keyed_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u32);
        let res = env
            .stream(source)
            .group_by(|n| n % 2)
            .map(|(_, x)| x.to_string())
            .reduce(|acc, y| *acc += &y)
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            let res = res.into_iter().sorted().collect_vec();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0], (0, "02468".into()));
            assert_eq!(res[1], (1, "13579".into()));
        }
    });
}
