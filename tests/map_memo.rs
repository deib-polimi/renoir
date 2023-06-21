use itertools::Itertools;

use rand::{thread_rng, Rng};
use utils::TestHelper;

mod utils;

#[test]
fn map_memo_stream() {
    TestHelper::local_remote_env(|mut env| {
        let res = env
            .stream_par_iter(0..100u32)
            .map(|n| n % 5)
            .map_memo(|v| v * v, 1000)
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            let mut expected = (0..100u32).map(|n| (n % 5) * (n % 5)).collect_vec();
            res.sort();
            expected.sort();
            assert_eq!(res, expected);
        }
    });
}

#[test]
fn map_memo_by_stream() {
    TestHelper::local_remote_env(|mut env| {
        let res = env
            .stream_par_iter(0..1000u32)
            .map(|v| v as f32 + thread_rng().gen::<f32>() * 0.4)
            .map(|v| v.rem_euclid(30.))
            .map_memo_by(
                |v| {
                    let x = v.round() as i64;
                    x * x
                },
                |v| v.round() as i64,
                1000,
            )
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            let mut expected = (0..1000i64)
                .map(|v| v.rem_euclid(30))
                .map(|n| (n * n))
                .collect_vec();
            res.sort();
            expected.sort();
            assert_eq!(res, expected);
        }
    });
}
