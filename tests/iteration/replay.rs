use rstream::operator::source::IteratorSource;
use rstream::test::TestHelper;

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
                |delta: u64, x| delta + x,
                |old_state, delta| old_state + delta,
                |state| {
                    *state -= 1;
                    true
                },
            )
            .collect_vec();
        env.execute();

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
        let n = 5u64;
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
                |s, state| s.map(move |x| x * *state.get()).shuffle(),
                |delta: u64, x| delta + x,
                |old_state, delta| old_state + delta,
                |state| {
                    *state -= 1;
                    true
                },
            )
            .collect_vec();
        env.execute();

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
