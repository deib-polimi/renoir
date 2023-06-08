use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use itertools::Itertools;

use noir::operator::source::IteratorSource;
use noir::Replication;
use utils::{TestHelper, WatermarkChecker};

mod utils;

#[test]
fn merge_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source1 = IteratorSource::new(0..10000u16);
        let source2 = IteratorSource::new(10000..20000u16);

        let stream1 = env.stream(source1);
        let stream2 = env.stream(source2);

        let res = stream1.merge(stream2).collect_vec();
        env.execute();
        if let Some(res) = res.get() {
            let res_sorted = res.into_iter().sorted().collect_vec();
            let expected = (0..20000u16).collect_vec();
            assert_eq!(res_sorted, expected);
        }
    });
}

#[test]
fn merge_stream_with_empty() {
    TestHelper::local_remote_env(|mut env| {
        let source1 = IteratorSource::new(0..10000u16);
        let source2 = IteratorSource::new(0..0u16);

        let stream1 = env.stream(source1);
        let stream2 = env.stream(source2);

        let res = stream1.merge(stream2).collect_vec();
        env.execute();
        if let Some(res) = res.get() {
            let res_sorted = res.into_iter().sorted().collect_vec();
            let expected = (0..10000u16).collect_vec();
            assert_eq!(res_sorted, expected);
        }
    });
}

#[test]
fn merge_stream_with_empty_other_way() {
    TestHelper::local_remote_env(|mut env| {
        let source1 = IteratorSource::new(0..0u16);
        let source2 = IteratorSource::new(0..10000u16);

        let stream1 = env.stream(source1);
        let stream2 = env.stream(source2);

        let res = stream1.merge(stream2).collect_vec();
        env.execute();
        if let Some(res) = res.get() {
            let res_sorted = res.into_iter().sorted().collect_vec();
            let expected = (0..10000u16).collect_vec();
            assert_eq!(res_sorted, expected);
        }
    });
}

#[test]
fn merge_empty_with_empty() {
    TestHelper::local_remote_env(|mut env| {
        let source1 = IteratorSource::new(0..0u16);
        let source2 = IteratorSource::new(0..0u16);

        let stream1 = env.stream(source1);
        let stream2 = env.stream(source2);

        let res = stream1.merge(stream2).collect_vec();
        env.execute();
        if let Some(res) = res.get() {
            assert!(res.is_empty());
        }
    });
}

#[test]
fn merge_with_timestamps() {
    TestHelper::local_remote_env(|mut env| {
        let source1 = IteratorSource::new(0..10u64);
        let source2 = IteratorSource::new(100..110u64);

        let stream1 = env
            .stream(source1)
            .add_timestamps(
                |&x| x as i64,
                |&x, &ts| if x % 2 == 1 { Some(ts) } else { None },
            )
            .shuffle();
        let stream2 = env
            .stream(source2)
            .add_timestamps(
                |&x| x as i64 % 10,
                |&x, &ts| if x % 2 == 1 { Some(ts) } else { None },
            )
            .shuffle();

        let num_watermarks = Arc::new(AtomicUsize::new(0));
        let stream = stream1
            .merge(stream2)
            .shuffle()
            .replication(Replication::One)
            .add_operator(|prev| WatermarkChecker::new(prev, num_watermarks.clone()));
        let res = stream.collect_vec();

        env.execute();
        if let Some(res) = res.get() {
            assert_eq!(res.len(), 20);
            assert_eq!(num_watermarks.load(Ordering::Acquire), 5);
        }
    });
}

#[test]
fn merge_keyed_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source1 = IteratorSource::new(0..100u64);
        let source2 = IteratorSource::new(100..200u64);

        let stream1 = env.stream(source1).group_by(|x| x % 3);
        let stream2 = env.stream(source2).group_by(|x| x % 3);

        let res = stream1.merge(stream2).reduce(|x, y| *x += y).collect_vec();
        env.execute();

        if let Some(mut res) = res.get() {
            res.sort_unstable();

            let expected = (0..3)
                .map(|k| (k, (0..200).filter(|x| x % 3 == k).sum()))
                .collect_vec();
            assert_eq!(res, expected);
        }
    });
}
