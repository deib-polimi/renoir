use noir::operator::source::IteratorSource;
use noir::operator::window::EventTimeWindow;

use super::utils::TestHelper;

#[test]
fn sliding_event_time() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10);

        let res = env
            .stream(source)
            .add_timestamps(
                |&x| x * 1000,
                |&x, &ts| if x % 2 == 1 { Some(ts) } else { None },
            )
            .group_by(|x| x % 2)
            .window(EventTimeWindow::sliding(3000, 2500))
            .first()
            .drop_key()
            .collect_vec();
        env.execute_blocking();

        if let Some(mut res) = res.get() {
            // Windows and elements
            // 0.0 -> 3.0  [0, 2] and [1]
            // 2.5 -> 5.5  [4] and [3, 5]
            // 5.0 -> 8.0  [6] and [5, 7]
            // 7.5 -> 10.5 [8] and [9]
            res.sort_unstable();
            assert_eq!(res, vec![0, 1, 3, 4, 5, 6, 8, 9]);
        }
    });
}

#[test]
fn tumbling_event_time() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10);

        let res = env
            .stream(source)
            .add_timestamps(|&x| x, |&x, &ts| if x % 2 == 1 { Some(ts) } else { None })
            .group_by(|x| x % 2)
            .window(EventTimeWindow::tumbling(3))
            .first()
            .drop_key()
            .collect_vec();
        env.execute_blocking();

        if let Some(mut res) = res.get() {
            // Windows and elements
            // 0.0 -> 3.0  [0, 2] and [1]
            // 3.0 -> 6.0  [4] and [3, 5]
            // 6.0 -> 9.0  [6, 8] and [7]
            // 9.0 -> 12.0 [] and [9]
            res.sort_unstable();
            assert_eq!(res, vec![0, 1, 3, 4, 6, 7, 9]);
        }
    });
}
