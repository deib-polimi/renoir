use std::str::FromStr;

use itertools::Itertools;

use noir::operator::source::IteratorSource;
use utils::TestHelper;

mod utils;

#[test]
fn map_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .map(|n| n.to_string())
            .map(|n| n + "000")
            .map(|n| u32::from_str(&n).unwrap())
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            let expected = (0..10u32).map(|n| 1000 * n).collect_vec();
            assert_eq!(res, expected);
        }
    });
}

#[test]
fn map_keyed_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|n| n % 2)
            .map(|(k, v)| 100 * k + v)
            .drop_key()
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            let res = res.into_iter().sorted().collect_vec();
            let expected = (0..10u8).map(|n| (n % 2) * 100 + n).sorted().collect_vec();
            assert_eq!(res, expected);
        }
    });
}
