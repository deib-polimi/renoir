use rstream::operator::source::IteratorSource;
use utils::TestHelper;

mod utils;

#[test]
fn group_by_min_element() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by_min_element(|&x| x % 2, |&x| x)
            .collect_vec();
        env.execute();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(res, &[(0, 0), (1, 1)]);
        }
    });
}

#[test]
fn group_by_max_element() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by_max_element(|&x| x % 2, |&x| x)
            .collect_vec();
        env.execute();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(res, &[(0, 8), (1, 9)]);
        }
    });
}

#[test]
#[allow(clippy::identity_op)]
fn group_by_sum() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by_sum(|&x| x % 2, |&x| x)
            .collect_vec();
        env.execute();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(res, &[(0, 0 + 2 + 4 + 6 + 8), (1, 1 + 3 + 5 + 7 + 9)]);
        }
    });
}

#[test]
#[allow(clippy::identity_op)]
fn group_by_avg() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by_avg(|&x| x % 2, |&x| x as f64)
            .collect_vec();
        env.execute();
        if let Some(mut res) = res.get() {
            res.sort_by_key(|(m, _)| *m);
            assert_eq!(
                res,
                &[
                    (0, (0 + 2 + 4 + 6 + 8) as f64 / 5f64),
                    (1, (1 + 3 + 5 + 7 + 9) as f64 / 5f64)
                ]
            );
        }
    });
}

#[test]
fn group_by_count() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env.stream(source).group_by_count(|&x| x % 2).collect_vec();
        env.execute();
        if let Some(mut res) = res.get() {
            res.sort_by_key(|(m, _)| *m);
            assert_eq!(res, &[(0, 5), (1, 5)]);
        }
    });
}
