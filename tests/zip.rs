use itertools::Itertools;

use noir::operator::source::IteratorSource;
use utils::TestHelper;

mod utils;

#[test]
fn test_zip_no_shuffle() {
    TestHelper::local_remote_env(|mut env| {
        let items1 = 0..3u8;
        let items2 = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        let source1 = IteratorSource::new(items1.clone());
        let source2 = IteratorSource::new(items2.clone().into_iter());
        let stream1 = env.stream(source1);
        let stream2 = env.stream(source2);
        let res = stream1.zip(stream2).collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            let expected = items1.into_iter().zip(items2.into_iter()).collect_vec();
            assert_eq!(res, expected);
        }
    });
}

#[test]
fn test_zip_balanced() {
    TestHelper::local_remote_env(|mut env| {
        let items1 = 0..3u8;
        let items2 = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        let source1 = IteratorSource::new(items1.clone());
        let source2 = IteratorSource::new(items2.clone().into_iter());
        let stream1 = env.stream(source1).shuffle();
        let stream2 = env.stream(source2).shuffle();
        let res = stream1.zip(stream2).collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            let (n, s): (Vec<_>, Vec<_>) = res.into_iter().unzip();
            let n = n.into_iter().sorted().collect_vec();
            let s = s.into_iter().sorted().collect_vec();

            assert_eq!(n, items1.collect_vec());
            assert_eq!(s, items2);
        }
    });
}

#[test]
fn test_zip_unbalanced() {
    TestHelper::local_remote_env(|mut env| {
        let items1 = 0..10u8;
        let items2 = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        let source1 = IteratorSource::new(items1);
        let source2 = IteratorSource::new(items2.clone().into_iter());
        let stream1 = env.stream(source1).shuffle();
        let stream2 = env.stream(source2).shuffle();
        let res = stream1.zip(stream2).collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            let (_n, s): (Vec<_>, Vec<_>) = res.into_iter().unzip();
            let s = s.into_iter().sorted().collect_vec();

            assert_eq!(s, items2);
        }
    });
}
