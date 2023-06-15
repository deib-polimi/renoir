use itertools::Itertools;

use noir::operator::source::IteratorSource;
use utils::TestHelper;

mod utils;

#[test]
fn split_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..5u8);
        let mut splits = env.stream(source).shuffle().map(|n| n.to_string()).split(2);
        let v1 = splits.pop().unwrap().map(|x| x.clone() + &x).collect_vec();
        let v2 = splits.pop().unwrap().map(|x| x + "a").collect_vec();
        env.execute_blocking();

        if let Some(v1) = v1.get() {
            assert_eq!(
                v1.into_iter().sorted().collect_vec(),
                &["00", "11", "22", "33", "44"]
            );
        }
        if let Some(v2) = v2.get() {
            assert_eq!(
                v2.into_iter().sorted().collect_vec(),
                &["0a", "1a", "2a", "3a", "4a"]
            );
        }
    });
}

#[test]
fn double_split_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..5u8);
        let mut v1234 = env.stream(source).shuffle().split(2);
        let mut v12 = v1234.pop().unwrap().split(2);
        let mut v34 = v1234.pop().unwrap().split(2);
        let res = vec![
            v12.pop().unwrap().collect_vec(),
            v12.pop().unwrap().collect_vec(),
            v34.pop().unwrap().collect_vec(),
            v34.pop().unwrap().collect_vec(),
        ];

        env.execute_blocking();

        for res in res {
            if let Some(mut res) = res.get() {
                res.sort_unstable();
                assert_eq!(res, (0..5u8).collect_vec());
            }
        }
    });
}
