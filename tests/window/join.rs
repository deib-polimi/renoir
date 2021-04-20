use rstream::operator::source::EventTimeIteratorSource;
use rstream::operator::{EventTimeWindow, Timestamp};
use rstream::test::TestHelper;

#[test]
fn window_join() {
    TestHelper::local_remote_env(|mut env| {
        let source1 =
            EventTimeIteratorSource::new((0..10).map(|x| (x, Timestamp::from_secs(x))), |x, ts| {
                if x % 2 == 1 {
                    Some(*ts)
                } else {
                    None
                }
            });

        let source2 =
            EventTimeIteratorSource::new((0..10).map(|x| (x, Timestamp::from_secs(x))), |x, ts| {
                if x % 2 == 0 {
                    Some(*ts)
                } else {
                    None
                }
            });

        let stream1 = env.stream(source1).shuffle().group_by(|x| x % 2);
        let stream2 = env
            .stream(source2)
            .shuffle()
            .group_by(|x| x % 2)
            .map(|(_, x)| ('a'..'z').nth(x as usize).unwrap());

        let res = stream1
            .window_join(stream2, EventTimeWindow::tumbling(Timestamp::from_secs(3)))
            .unkey()
            .collect_vec();
        env.execute();

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
                        expected.push((x % 2, (x, ('a'..'z').nth(y as usize).unwrap())));
                    }
                }
            }
            expected.sort_unstable();

            assert_eq!(res, expected);
        }
    });
}
