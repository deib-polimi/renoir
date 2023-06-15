use noir::operator::source::IteratorSource;
use utils::TestHelper;

mod utils;

#[test]
fn broadcast_test() {
    TestHelper::local_remote_env(|mut env| {
        let parallelism = env.parallelism() as u32;
        let source = IteratorSource::new(0..100u32);

        let res = env
            .stream(source)
            .shuffle()
            .broadcast()
            .reduce(|x, y| x + y)
            .collect_vec();

        env.execute_blocking();
        if let Some(res) = res.get() {
            assert_eq!(res.len(), 1);
            assert_eq!(res[0], (0..100).sum::<u32>() * parallelism);
        }
    });
}
