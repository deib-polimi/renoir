use noir::operator::source::IteratorSource;
use utils::TestHelper;

mod utils;

#[test]
fn reduce_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env.stream(source).reduce(|a, b| a + b).collect_vec();
        env.execute_blocking();
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
        let res = env.stream(source).reduce_assoc(|a, b| a + b).collect_vec();
        env.execute_blocking();
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
            .reduce(|a, b| a + b)
            .collect_vec();
        env.execute_blocking();
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
            .reduce_assoc(|a, b| a + b)
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            assert_eq!(res.len(), 1);
            assert_eq!(res[0], 45);
        }
    });
}
