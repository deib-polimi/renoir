use noir::operator::source::IteratorSource;
use utils::TestHelper;

mod utils;

#[test]
fn group_by_fold_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by_fold(
                |n| n % 2,
                "".to_string(),
                |s, n| *s += &n.to_string(),
                |s1, s2| *s1 += &s2,
            )
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].1, "02468");
            assert_eq!(res[1].1, "13579");
        }
    });
}

#[test]
fn fold_keyed_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|n| n % 2)
            .fold("".to_string(), |s, n| *s += &n.to_string())
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].1, "02468");
            assert_eq!(res[1].1, "13579");
        }
    });
}

#[test]
fn group_by_fold_shuffled_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .shuffle()
            .group_by_fold(
                |n| n % 2,
                Vec::new(),
                |v, n| v.push(n),
                |v1, mut v2| v1.append(&mut v2),
            )
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            res[0].1.sort_unstable();
            res[1].1.sort_unstable();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].1, &[0, 2, 4, 6, 8]);
            assert_eq!(res[1].1, &[1, 3, 5, 7, 9]);
        }
    });
}

#[test]
fn fold_shuffled_keyed_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .shuffle()
            .group_by(|n| n % 2)
            .fold(Vec::new(), |v, n| v.push(n))
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            res[0].1.sort_unstable();
            res[1].1.sort_unstable();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].1, &[0, 2, 4, 6, 8]);
            assert_eq!(res[1].1, &[1, 3, 5, 7, 9]);
        }
    });
}
