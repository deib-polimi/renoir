use itertools::Itertools;
use rstream::operator::source::IteratorSource;
use rstream::test::TestHelper;

#[test]
fn fold_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .fold("".to_string(), |s, n| s + &n.to_string())
            .collect_vec();
        env.execute();
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
            .fold_assoc("".to_string(), |s, n| s + &n.to_string(), |s1, s2| s1 + &s2)
            .collect_vec();
        env.execute();
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
            .fold(Vec::new(), |mut v, n| {
                v.push(n);
                v
            })
            .collect_vec();
        env.execute();
        if let Some(mut res) = res.get() {
            assert_eq!(res.len(), 1);
            res[0].sort_unstable();
            assert_eq!(res[0], (0..10u8).into_iter().collect_vec());
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
                |mut v, n| {
                    v.push(n);
                    v
                },
                |mut v1, mut v2| {
                    v2.append(&mut v1);
                    v2
                },
            )
            .collect_vec();
        env.execute();
        if let Some(mut res) = res.get() {
            assert_eq!(res.len(), 1);
            res[0].sort_unstable();
            assert_eq!(res[0], (0..10u8).into_iter().collect_vec());
        }
    });
}
