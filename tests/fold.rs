use itertools::Itertools;

use noir::operator::source::IteratorSource;
use utils::TestHelper;

mod utils;

#[test]
fn fold_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .fold("".to_string(), |s, n| *s += &n.to_string())
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            assert_eq!(res.len(), 1);
            assert_eq!(res[0], "0123456789");
        }
    });
}

#[test]
fn fold_assoc_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .fold_assoc(
                "".to_string(),
                |s, n| *s += &n.to_string(),
                |s1, s2| *s1 += &s2,
            )
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            assert_eq!(res.len(), 1);
            assert_eq!(res[0], "0123456789");
        }
    });
}

#[test]
fn fold_shuffled_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .shuffle()
            .fold(Vec::new(), |v, n| v.push(n))
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            assert_eq!(res.len(), 1);
            res[0].sort_unstable();
            assert_eq!(res[0], (0..10u8).collect_vec());
        }
    });
}

#[test]
fn fold_assoc_shuffled_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .shuffle()
            .fold_assoc(
                Vec::new(),
                |v, n| v.push(n),
                |v1, mut v2| v1.append(&mut v2),
            )
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            assert_eq!(res.len(), 1);
            res[0].sort_unstable();
            assert_eq!(res[0], (0..10u8).collect_vec());
        }
    });
}
