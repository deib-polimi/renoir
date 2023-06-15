use noir::operator::source::IteratorSource;
use utils::TestHelper;

mod utils;

#[test]
fn interval_join_keyed_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10);
        let source2 = IteratorSource::new(0..10);

        let right = env
            .stream(source2)
            .add_timestamps(|&x| x, |&x, &ts| if x % 2 == 0 { Some(ts) } else { None })
            .group_by(|x| x % 2);
        let res = env
            .stream(source)
            .add_timestamps(|&x| x, |&x, &ts| if x % 2 == 0 { Some(ts) } else { None })
            .group_by(|x| x % 2)
            .interval_join(right, 1, 2)
            .collect_vec();

        env.execute_blocking();

        if let Some(mut res) = res.get() {
            let mut expected = Vec::new();
            for l in 0..10 {
                for r in l - 1..=l + 2 {
                    if !(0..10).contains(&r) || l % 2 != r % 2 {
                        continue;
                    }
                    expected.push((l % 2, (l, r)));
                }
            }
            expected.sort_unstable();
            res.sort_unstable();
            assert_eq!(res, expected);
        }
    });
}

#[test]
fn interval_join_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10);
        let source2 = IteratorSource::new(0..10);

        let right = env
            .stream(source2)
            .add_timestamps(|&x| x, |&x, &ts| if x % 2 == 0 { Some(ts) } else { None });
        let res = env
            .stream(source)
            .add_timestamps(|&x| x, |&x, &ts| if x % 2 == 0 { Some(ts) } else { None })
            .interval_join(right, 1, 2)
            .collect_vec();

        env.execute_blocking();

        if let Some(mut res) = res.get() {
            let mut expected = Vec::new();
            for l in 0..10 {
                for r in l - 1..=l + 2 {
                    if !(0..10).contains(&r) {
                        continue;
                    }
                    expected.push((l, r));
                }
            }
            expected.sort_unstable();
            res.sort_unstable();
            assert_eq!(res, expected);
        }
    });
}
