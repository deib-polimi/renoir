use itertools::Itertools;

use rstream::operator::source::IteratorSource;
use rstream::test::TestHelper;

#[test]
fn group_by_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..100u8);
        let res = env
            .stream(source)
            .group_by(|&n| n.to_string().chars().next().unwrap())
            .unkey()
            .collect_vec();
        env.execute();
        if let Some(res) = res.get() {
            let res = res.into_iter().sorted().collect_vec();
            let expected = (0..100u8)
                .map(|n| (n.to_string().chars().next().unwrap(), n))
                .sorted()
                .collect_vec();
            assert_eq!(res, expected);
        }
    });
}
