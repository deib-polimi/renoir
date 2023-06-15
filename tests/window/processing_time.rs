use std::time::Duration;

use noir::operator::source::IteratorSource;
use noir::operator::window::ProcessingTimeWindow;

use super::utils::TestHelper;

#[test]
fn tumbling_processing_time() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(1..=1000);

        let res = env
            .stream(source)
            .group_by(|x| {
                std::thread::sleep(Duration::from_millis(1));
                x % 2
            })
            .window(ProcessingTimeWindow::tumbling(Duration::from_millis(100)))
            .fold(0, |acc, x| *acc += x)
            .drop_key()
            .collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            eprintln!("{res:?}");
            let sum: i32 = res.into_iter().sum();
            assert_eq!(sum, (1..=1000).sum::<i32>());
        }
    });
}
