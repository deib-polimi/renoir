use itertools::Itertools;

use noir::operator::source::IteratorSource;
use utils::TestHelper;

mod utils;

#[test]
fn rich_map_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .map(|_| 1)
            .rich_map({
                let mut count = 0;
                move |n| {
                    count += n;
                    count
                }
            })
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            let expected = (1..=10u32).collect_vec();
            assert_eq!(res, expected);
        }
    });
}

#[test]
fn rich_map_keyed_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|n| n % 2)
            .map(|_| 1)
            .rich_map({
                let mut count = 0;
                move |(_k, v)| {
                    count += v;
                    count
                }
            })
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            let res = res.into_iter().sorted().collect_vec();
            let expected = (0..=1)
                .flat_map(|k| (1..=5).map(move |v| (k, v)))
                .sorted()
                .collect_vec();
            assert_eq!(res, expected);
        }
    });
}
