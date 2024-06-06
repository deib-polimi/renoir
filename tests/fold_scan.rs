use utils::TestHelper;

mod utils;

#[test]
fn fold_scan() {
    TestHelper::local_remote_env(|ctx| {
        let res = ctx
            .stream_par_iter(200..210)
            .fold_scan(
                |acc: &mut usize, _| {
                    *acc += 1;
                },
                |global, local| {
                    *global += local;
                },
                0,
                |x, cnt| (x, *cnt),
            )
            .collect_vec();

        ctx.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort();
            assert_eq!(
                (200..210)
                    .map(|x| (x, (200..210).len()))
                    .collect::<Vec<_>>(),
                res
            );
        }
    });
}

#[test]
fn reduce_scan() {
    TestHelper::local_remote_env(|ctx| {
        let res = ctx
            .stream_par_iter(200..210i32)
            .reduce_scan(
                |x| (x, 1usize),
                |a, b| (a.0 + b.0, a.1 + b.1),
                |x, &(sum, cnt)| (x, sum, cnt),
            )
            .collect_vec();

        ctx.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort();
            assert_eq!(
                (200..210i32)
                    .map(|x| (x, (200..210i32).sum::<i32>(), (200..210).len()))
                    .collect::<Vec<_>>(),
                res
            );
        }
    });
}

#[test]
fn keyed_fold_scan() {
    TestHelper::local_remote_env(|ctx| {
        let res = ctx
            .stream_par_iter(0..100i32)
            .group_by(|e| e % 5)
            .fold_scan(
                0,
                |_k, acc: &mut i32, x| {
                    *acc += x;
                },
                |_k, acc, x| (x, *acc),
            )
            .unkey()
            .map(|t| (t.0, t.1 .0, t.1 .1))
            .collect_vec();

        ctx.execute_blocking();
        if let Some(mut res) = res.get() {
            let mut expected = (0..100)
                .map(|x| (x % 5, x, (0..100).filter(|m| m % 5 == x % 5).sum::<i32>()))
                .collect::<Vec<_>>();
            res.sort();
            expected.sort();
            assert_eq!(expected, res);
        }
    });
}

#[test]
fn keyed_reduce_scan() {
    TestHelper::local_remote_env(|ctx| {
        let res = ctx
            .stream_par_iter(0..100i32)
            .group_by(|e| e % 5)
            .reduce_scan(|_k, x| x, |_k, a, b| a + b, |_k, acc, x| (x, *acc))
            .unkey()
            .map(|t| (t.0, t.1 .0, t.1 .1))
            .collect_vec();

        ctx.execute_blocking();
        if let Some(mut res) = res.get() {
            let mut expected = (0..100)
                .map(|x| (x % 5, x, (0..100).filter(|m| m % 5 == x % 5).sum::<i32>()))
                .collect::<Vec<_>>();
            res.sort();
            expected.sort();
            assert_eq!(expected, res);
        }
    });
}
