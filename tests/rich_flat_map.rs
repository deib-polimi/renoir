use itertools::{repeat_n, Itertools};

use noir::operator::source::IteratorSource;
use utils::TestHelper;

mod utils;

#[test]
fn rich_flat_map_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .rich_flat_map({
                let mut v = Vec::new();
                move |x| {
                    v.push(x);
                    v.clone()
                }
            })
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            let expected = (0..10u8)
                .flat_map(|x| repeat_n(x, (10 - x) as usize))
                .sorted()
                .collect_vec();
            assert_eq!(expected, res);
        }
    });
}

#[test]
fn rich_flat_map_keyed_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10i32);
        let res = env
            .stream(source)
            .group_by(|v| v % 2)
            .rich_flat_map({
                let mut values = Vec::new();
                move |(_k, v)| {
                    values.push(v);
                    if values.len() == 5 {
                        values.clone()
                    } else {
                        vec![-1]
                    }
                }
            })
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            let res = res.into_iter().sorted().collect_vec();
            let expected = (0..10i32)
                .map(|x| (x % 2, x))
                .chain((0..2i32).flat_map(|k| repeat_n((k, -1), 4)))
                .sorted()
                .collect_vec();
            assert_eq!(expected, res);
        }
    });
}
