use noir::operator::source::IteratorSource;
use noir::operator::window::EventTimeWindow;

use super::utils::TestHelper;

#[test]
fn window_join() {
    TestHelper::local_remote_env(|mut env| {
        let source1 = IteratorSource::new(0..10);
        let source2 = IteratorSource::new(0..10);

        let stream1 = env
            .stream(source1)
            .add_timestamps(|&x| x, |&x, &ts| if x % 2 == 1 { Some(ts) } else { None })
            .shuffle()
            .group_by(|x| x % 2);
        let stream2 = env
            .stream(source2)
            .add_timestamps(|&x| x, |&x, &ts| if x % 2 == 0 { Some(ts) } else { None })
            .shuffle()
            .group_by(|x| x % 2)
            .map(|(_, x)| ('a'..='z').nth(x as usize).unwrap());

        let res = stream1
            .window(EventTimeWindow::tumbling(3))
            .join(stream2)
            .collect_vec();
        env.execute_blocking();

        if let Some(mut res) = res.get() {
            res.sort_unstable();

            let windows = vec![
                // key == 0
                vec![0, 2],
                vec![4],
                vec![6, 8],
                // key == 1
                vec![1],
                vec![3, 5],
                vec![7],
                vec![9],
            ];

            let mut expected = Vec::new();
            for window in windows.into_iter() {
                for &x in window.iter() {
                    for &y in window.iter() {
                        expected.push((x % 2, (x, ('a'..='z').nth(y as usize).unwrap())));
                    }
                }
            }
            expected.sort_unstable();

            assert_eq!(res, expected);
        }
    });
}

#[test]
fn window_all_join() {
    TestHelper::local_remote_env(|mut env| {
        let source1 = IteratorSource::new(0..10);
        let source2 = IteratorSource::new(0..10);

        let stream1 = env
            .stream(source1)
            .add_timestamps(|&x| x, |&x, &ts| if x % 2 == 1 { Some(ts) } else { None })
            .shuffle();
        let stream2 = env
            .stream(source2)
            .add_timestamps(|&x| x, |&x, &ts| if x % 2 == 0 { Some(ts) } else { None })
            .shuffle()
            .map(|x| ('a'..='z').nth(x as usize).unwrap());

        let res = stream1
            .window_all(EventTimeWindow::tumbling(3))
            .join(stream2)
            .collect_vec();
        env.execute_blocking();

        if let Some(mut res) = res.get() {
            res.sort_unstable();

            let windows = vec![vec![0, 1, 2], vec![3, 4, 5], vec![6, 7, 8], vec![9]];

            let mut expected = Vec::new();
            for window in windows.into_iter() {
                for &x in window.iter() {
                    for &y in window.iter() {
                        expected.push((x, ('a'..='z').nth(y as usize).unwrap()));
                    }
                }
            }
            expected.sort_unstable();

            assert_eq!(res, expected);
        }
    });
}

#[test]
fn session_window_join() {
    TestHelper::local_remote_env(|mut env| {
        let source1 = IteratorSource::new(vec![0, 1, 2, 6, 7, 8].into_iter());
        let source2 = IteratorSource::new(vec![1, 3, 6, 9, 10, 11].into_iter());

        let stream1 = env
            .stream(source1)
            .add_timestamps(|&x| x, |&x, &ts| if x % 2 == 1 { Some(ts) } else { None })
            .shuffle()
            .group_by(|x| x % 2);
        let stream2 = env
            .stream(source2)
            .add_timestamps(|&x| x, |&x, &ts| if x % 2 == 0 { Some(ts) } else { None })
            .shuffle()
            .group_by(|x| x % 2);

        let res = stream1
            .window(EventTimeWindow::session(3))
            .join(stream2)
            .collect_vec();
        env.execute_blocking();

        if let Some(mut res) = res.get() {
            let mut expected = vec![
                // key 0
                (0, (6, 6)),
                (0, (6, 10)),
                (0, (8, 6)),
                (0, (8, 10)),
                // key 1
                (1, (1, 1)),
                (1, (1, 3)),
                (1, (7, 9)),
                (1, (7, 11)),
            ];
            expected.sort_unstable();
            res.sort_unstable();

            assert_eq!(res, expected);
        }
    });
}
