use noir::operator::sink::StreamOutput;
use noir::operator::source::IteratorSource;

use super::utils::TestHelper;

#[test]
fn test_replay_no_blocks_in_between() {
    TestHelper::local_remote_env(|mut env| {
        let n = 5u64;
        let n_iter = 5;

        let source = IteratorSource::new(0..n);
        let res = env
            .stream(source)
            .shuffle()
            .map(|x| x)
            // the body of this iteration does not split the block (it's just a map)
            .replay(
                n_iter,
                1,
                |s, state| s.map(move |x| x * *state.get()),
                |delta: &mut u64, x| *delta += x,
                |old_state, delta| *old_state += delta,
                |state| {
                    *state -= 1;
                    true
                },
            )
            .collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res.len(), 1);
            let res = res.into_iter().next().unwrap();

            let mut state = 1;
            for _ in 0..n_iter {
                let s: u64 = (0..n).map(|x| x * state).sum();
                state = state + s - 1;
            }

            assert_eq!(res, state);
        }
    });
}

#[test]
fn test_replay_with_shuffle() {
    TestHelper::local_remote_env(|mut env| {
        let n = 20u64;
        let n_iter = 5;

        let source = IteratorSource::new(0..n);
        let res = env
            .stream(source)
            .shuffle()
            .map(|x| x)
            // the body of this iteration will split the block (there is a shuffle)
            .replay(
                n_iter,
                1,
                |s, state| s.shuffle().map(move |x| x * *state.get()),
                |delta: &mut u64, x| *delta += x,
                |old_state, delta| *old_state += delta,
                |state| {
                    *state -= 1;
                    true
                },
            )
            .collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res.len(), 1);
            let res = res.into_iter().next().unwrap();

            let mut state = 1;
            for _ in 0..n_iter {
                let s: u64 = (0..n).map(|x| x * state).sum();
                state = state + s - 1;
            }

            assert_eq!(res, state);
        }
    });
}

fn check_nested_result(res: StreamOutput<Vec<u64>>) {
    if let Some(res) = res.get() {
        assert_eq!(res.len(), 1);
        let res = res.into_iter().next().unwrap();

        let mut expected = 0u64;
        for _ in 0..2 {
            for _ in 0..2 {
                let mut inner = 0;
                for i in 0..10 {
                    inner += i;
                }
                expected += inner;
            }
        }

        assert_eq!(res, expected);
    }
}

#[test]
fn test_replay_nested_no_shuffle() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u64);
        let stream = env.stream(source).shuffle().replay(
            2,
            0,
            |s, _| {
                s.replay(
                    2,
                    0,
                    |s, _| s.reduce(|x, y| x + y),
                    |update: &mut u64, ele| *update += ele,
                    |state, update| *state += update,
                    |&mut _state| true,
                )
            },
            |update: &mut u64, ele| *update += ele,
            |state, update| *state += update,
            |&mut _state| true,
        );
        let res = stream.collect_vec();
        env.execute_blocking();
        check_nested_result(res);
    });
}

#[test]
fn test_replay_nested_shuffle_inner() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u64);
        let stream = env.stream(source).shuffle().replay(
            2,
            0,
            |s, _| {
                s.replay(
                    2,
                    0,
                    |s, _| s.shuffle().reduce(|x, y| x + y),
                    |update: &mut u64, ele| *update += ele,
                    |state, update| *state += update,
                    |&mut _state| true,
                )
            },
            |update: &mut u64, ele| *update += ele,
            |state, update| *state += update,
            |&mut _state| true,
        );
        let res = stream.collect_vec();
        env.execute_blocking();
        check_nested_result(res);
    });
}

#[test]
fn test_replay_nested_shuffle_outer() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u64);
        let stream = env.stream(source).shuffle().replay(
            2,
            0,
            |s, _| {
                s.shuffle().replay(
                    2,
                    0,
                    |s, _| s.reduce(|x, y| x + y),
                    |update: &mut u64, ele| *update += ele,
                    |state, update| *state += update,
                    |&mut _state| true,
                )
            },
            |update: &mut u64, ele| *update += ele,
            |state, update| *state += update,
            |&mut _state| true,
        );
        let res = stream.collect_vec();
        env.execute_blocking();
        check_nested_result(res);
    });
}

#[test]
fn test_replay_nested_shuffle_both() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u64);
        let stream = env.stream(source).shuffle().replay(
            2,
            0,
            |s, _| {
                s.shuffle().replay(
                    2,
                    0,
                    |s, _| s.shuffle().reduce(|x, y| x + y),
                    |update: &mut u64, ele| *update += ele,
                    |state, update| *state += update,
                    |&mut _state| true,
                )
            },
            |update: &mut u64, ele| *update += ele,
            |state, update| *state += update,
            |&mut _state| true,
        );
        let res = stream.collect_vec();
        env.execute_blocking();
        check_nested_result(res);
    });
}
