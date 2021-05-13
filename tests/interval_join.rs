use rstream::operator::source::EventTimeIteratorSource;
use rstream::operator::Timestamp;
use rstream::test::TestHelper;
use std::time::Duration;

#[test]
fn interval_join_keyed_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = EventTimeIteratorSource::new(
            (0..10).map(|x| (x, Timestamp::from_nanos(x as u64))),
            |x, ts| {
                if x % 2 == 0 {
                    Some(*ts)
                } else {
                    None
                }
            },
        );

        let source2 = EventTimeIteratorSource::new(
            (0..10).map(|x| (x, Timestamp::from_nanos(x as u64))),
            |x, ts| {
                if x % 2 == 0 {
                    Some(*ts)
                } else {
                    None
                }
            },
        );
        let right = env.stream(source2).group_by(|x| x % 2);
        let res = env
            .stream(source)
            .group_by(|x| x % 2)
            .interval_join(right, Duration::from_nanos(1), Duration::from_nanos(2))
            .unkey()
            .collect_vec();

        env.execute();

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
        let source = EventTimeIteratorSource::new(
            (0..10).map(|x| (x, Timestamp::from_nanos(x as u64))),
            |x, ts| {
                if x % 2 == 0 {
                    Some(*ts)
                } else {
                    None
                }
            },
        );

        let source2 = EventTimeIteratorSource::new(
            (0..10).map(|x| (x, Timestamp::from_nanos(x as u64))),
            |x, ts| {
                if x % 2 == 0 {
                    Some(*ts)
                } else {
                    None
                }
            },
        );
        let right = env.stream(source2);
        let res = env
            .stream(source)
            .interval_join(right, Duration::from_nanos(1), Duration::from_nanos(2))
            .collect_vec();

        env.execute();

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
