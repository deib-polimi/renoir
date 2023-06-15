use itertools::Itertools;

use noir::operator::source::IteratorSource;
use utils::TestHelper;

mod utils;

#[test]
fn flatten_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(
            vec![
                vec![],
                vec![1u8, 2, 3],
                vec![4, 5],
                vec![],
                vec![6, 7, 8],
                vec![],
            ]
            .into_iter(),
        );
        let res = env.stream(source).flatten().collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            assert_eq!(res, (1..=8).collect_vec());
        }
    });
}

#[test]
fn flatten_keyed_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|v| v % 2)
            .map(|(_k, v)| vec![v, v, v])
            .flatten()
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            let res = res.into_iter().sorted().collect_vec();
            let expected = (0..10u8)
                .flat_map(|x| vec![(x % 2, x), (x % 2, x), (x % 2, x)])
                .sorted()
                .collect_vec();
            assert_eq!(expected, res);
        }
    });
}

#[test]
fn flat_map_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .flat_map(|x| vec![x, 10 * x, 20 * x])
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            let res = res.into_iter().sorted().collect_vec();
            let expected = (0..10u8)
                .flat_map(|x| vec![x, 10 * x, 20 * x])
                .sorted()
                .collect_vec();
            assert_eq!(expected, res);
        }
    });
}

#[test]
fn flat_map_keyed_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|v| v % 2)
            .flat_map(|(_k, v)| vec![v, v, v])
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            let res = res.into_iter().sorted().collect_vec();
            let expected = (0..10u8)
                .flat_map(|x| vec![(x % 2, x), (x % 2, x), (x % 2, x)])
                .sorted()
                .collect_vec();
            assert_eq!(expected, res);
        }
    });
}

#[test]
fn flatten_not_serializable_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(
            vec![
                vec![],
                vec![1u8, 2, 3],
                vec![4, 5],
                vec![],
                vec![6, 7, 8],
                vec![],
            ]
            .into_iter(),
        );
        let res = env
            .stream(source)
            .map(|x| x.into_iter())
            .flatten()
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            assert_eq!(res, (1..=8).collect_vec());
        }
    });
}

#[test]
fn flat_map_not_serializable_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .flat_map(|x| vec![x, 10 * x, 20 * x].into_iter())
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            let res = res.into_iter().sorted().collect_vec();
            let expected = (0..10u8)
                .flat_map(|x| vec![x, 10 * x, 20 * x])
                .sorted()
                .collect_vec();
            assert_eq!(expected, res);
        }
    });
}
