use noir::operator::source::IteratorSource;
use utils::TestHelper;

mod utils;

#[test]
fn reduce_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env.stream(source).reduce(|acc, v| *acc += v).collect_vec();
        env.execute();
        if let Some(res) = res.get() {
            assert_eq!(res.len(), 1);
            assert_eq!(res[0], 45);
        }
    });
}

#[test]
fn reduce_assoc_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .reduce_assoc(|acc, v| *acc += v)
            .collect_vec();
        env.execute();
        if let Some(res) = res.get() {
            assert_eq!(res.len(), 1);
            assert_eq!(res[0], 45);
        }
    });
}

#[test]
fn reduce_shuffled_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .shuffle()
            .reduce(|acc, v| *acc += v)
            .collect_vec();
        env.execute();
        if let Some(res) = res.get() {
            assert_eq!(res.len(), 1);
            assert_eq!(res[0], 45);
        }
    });
}

#[test]
fn reduce_assoc_shuffled_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .shuffle()
            .reduce_assoc(|acc, v| *acc += v)
            .collect_vec();
        env.execute();
        if let Some(res) = res.get() {
            assert_eq!(res.len(), 1);
            assert_eq!(res[0], 45);
        }
    });
}
