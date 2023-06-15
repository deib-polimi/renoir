use noir::operator::source::IteratorSource;
use utils::TestHelper;

mod utils;

#[test]
fn rich_filter_map_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new((0..6u8).map(|_| "a".to_owned()));
        let res = env
            .stream(source)
            .rich_filter_map({
                let mut s = String::new();
                move |x| {
                    s.push_str(&x);
                    if s.len() >= 4 {
                        Some(s.clone())
                    } else {
                        None
                    }
                }
            })
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            assert_eq!(res, &["aaaa", "aaaaa", "aaaaaa"]);
        }
    });
}

#[test]
fn rich_filter_map_keyed_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new((0..10u8).map(|x| if x % 2 == 0 { 'a' } else { 'b' }));
        let res = env
            .stream(source)
            .group_by(|c| *c)
            .rich_filter_map({
                let mut s = String::new();
                move |(_key, x)| {
                    s.push(x);
                    if s.len() >= 4 {
                        Some(s.clone())
                    } else {
                        None
                    }
                }
            })
            .drop_key()
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(res, &["aaaa", "aaaaa", "bbbb", "bbbbb"]);
        }
    });
}
