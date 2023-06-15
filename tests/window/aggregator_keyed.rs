use itertools::Itertools;

use noir::operator::source::IteratorSource;
use noir::operator::window::CountWindow;

use super::utils::TestHelper;

#[test]
fn test_first_window_keyed() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|x| x % 2)
            .window(CountWindow::sliding(3, 2))
            .first()
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(
                res,
                vec![
                    (0, 0), // [0, 2, 4]
                    (0, 4), // [4, 6, 8]
                    // (0, 8), // [8]
                    (1, 1), // [1, 3, 5]
                    (1, 5), // [5, 7, 9]
                            // (1, 9), // [9]
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
            .fold(0, |acc, x| *acc += x)
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(
                res,
                vec![
                    (0, 0 + 2 + 4), // [0, 2, 4]
                    (0, 4 + 6 + 8), // [4, 6, 8]
                    // (0, 8),         // [8]
                    (1, 1 + 3 + 5), // [1, 3, 5]
                    (1, 5 + 7 + 9), // [5, 7, 9]
                                    // (1, 9),         // [9]
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
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(
                res,
                vec![
                    (0, 0 + 2 + 4), // [0, 2, 4]
                    (0, 4 + 6 + 8), // [4, 6, 8]
                    // (0, 8),         // [8]
                    (1, 1 + 3 + 5), // [1, 3, 5]
                    (1, 5 + 7 + 9), // [5, 7, 9]
                                    // (1, 9),         // [9]
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
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(
                res,
                vec![
                    (0, 0), // [0, 2, 4]
                    (0, 4), // [4, 6, 8]
                    // (0, 8), // [8]
                    (1, 1), // [1, 3, 5]
                    (1, 5), // [5, 7, 9]
                            // (1, 9), // [9]
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
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(
                res,
                vec![
                    (0, 4), // [0, 2, 4]
                    (0, 8), // [4, 6, 8]
                    // (0, 8), // [8]
                    (1, 5), // [1, 3, 5]
                    (1, 9), // [5, 7, 9]
                            // (1, 9), // [9]
                ]
            );
        }
    });
}

#[test]
fn test_map_window_keyed() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u16);
        let res = env
            .stream(source)
            .group_by(|x| x % 2)
            .window(CountWindow::sliding(3, 2))
            .map(|items| {
                let mut res = 1;
                for x in items {
                    res *= x;
                }
                res
            })
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(
                res,
                vec![
                    (0, 0),         // [0, 2, 4]
                    (0, 4 * 6 * 8), // [4, 6, 8]
                    // (0, 8),         // [8]
                    (1, 3 * 5), // [1, 3, 5]
                    (1, 5 * 7 * 9), // [5, 7, 9]
                                // (1, 9),         // [9]
                ]
                .into_iter()
                .sorted()
                .collect_vec()
            );
        }
    });
}
