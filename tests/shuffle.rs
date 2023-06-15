use itertools::Itertools;

use noir::operator::source::IteratorSource;
use utils::TestHelper;

mod utils;

#[test]
fn shuffle_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..1000u16);
        let parallelism = env.parallelism();
        let res = env
            .stream(source)
            .shuffle()
            .shuffle()
            .shuffle()
            .shuffle()
            .shuffle()
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            let res_sorted = res.clone().into_iter().sorted().collect_vec();
            let expected = (0..1000u16).collect_vec();
            assert_eq!(res_sorted, expected);
            if parallelism > 1 {
                assert_ne!(
                    res, expected,
                    "It's very improbable that going through the shuffles the result is sorted"
                );
            }
        }
    });
}
