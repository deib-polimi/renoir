use itertools::Itertools;
use rstream::operator::source::IteratorSource;
use rstream::operator::CountWindow;
use rstream::test::TestHelper;

#[test]
fn test_first_window_keyed() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|x| x % 2)
            .window(CountWindow::sliding(3, 2))
            .first()
            .unkey()
            .collect_vec();
        env.execute();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(
                res,
                vec![
                    (0, 0), // [0, 2, 4]
                    (0, 4), // [4, 6, 8]
                    (0, 8), // [8]
                    (1, 1), // [1, 3, 5]
                    (1, 5), // [5, 7, 9]
                    (1, 9), // [9]
                ]
            );
        }
    });
}

#[test]
#[allow(clippy::identity_op)]
fn test_fold_window_keyed() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|x| x % 2)
            .window(CountWindow::sliding(3, 2))
            .fold(0, |acc, x| acc + x)
            .unkey()
            .collect_vec();
        env.execute();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(
                res,
                vec![
                    (0, 0 + 2 + 4), // [0, 2, 4]
                    (0, 4 + 6 + 8), // [4, 6, 8]
                    (0, 8),         // [8]
                    (1, 1 + 3 + 5), // [1, 3, 5]
                    (1, 5 + 7 + 9), // [5, 7, 9]
                    (1, 9),         // [9]
                ]
                .into_iter()
                .sorted()
                .collect_vec()
            );
        }
    });
}

#[test]
#[allow(clippy::identity_op)]
fn test_sum_window_keyed() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|x| x % 2)
            .window(CountWindow::sliding(3, 2))
            .sum()
            .unkey()
            .collect_vec();
        env.execute();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(
                res,
                vec![
                    (0, 0 + 2 + 4), // [0, 2, 4]
                    (0, 4 + 6 + 8), // [4, 6, 8]
                    (0, 8),         // [8]
                    (1, 1 + 3 + 5), // [1, 3, 5]
                    (1, 5 + 7 + 9), // [5, 7, 9]
                    (1, 9),         // [9]
                ]
                .into_iter()
                .sorted()
                .collect_vec()
            );
        }
    });
}

#[test]
fn test_min_window_keyed() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|x| x % 2)
            .window(CountWindow::sliding(3, 2))
            .min()
            .unkey()
            .collect_vec();
        env.execute();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(
                res,
                vec![
                    (0, 0), // [0, 2, 4]
                    (0, 4), // [4, 6, 8]
                    (0, 8), // [8]
                    (1, 1), // [1, 3, 5]
                    (1, 5), // [5, 7, 9]
                    (1, 9), // [9]
                ]
            );
        }
    });
}

#[test]
fn test_max_window_keyed() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|x| x % 2)
            .window(CountWindow::sliding(3, 2))
            .max()
            .unkey()
            .collect_vec();
        env.execute();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(
                res,
                vec![
                    (0, 4), // [0, 2, 4]
                    (0, 8), // [4, 6, 8]
                    (0, 8), // [8]
                    (1, 5), // [1, 3, 5]
                    (1, 9), // [5, 7, 9]
                    (1, 9), // [9]
                ]
            );
        }
    });
}
