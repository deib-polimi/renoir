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
