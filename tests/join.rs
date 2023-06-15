#![allow(clippy::type_complexity)]

use std::collections::HashSet;
use std::time::Duration;

use itertools::Itertools;

use noir::operator::source::IteratorSource;
use noir::BatchMode;
use utils::TestHelper;

mod utils;
macro_rules! run_test {
    ($env:expr, $n1:expr, $n2:expr, $m:expr, $ship:tt, $local:tt, $variant:tt) => {{
        let s1 = $env.stream(IteratorSource::new(0..$n1));
        let s2 = $env.stream(IteratorSource::new(0..$n2));
        let join = s1
            .batch_mode(BatchMode::adaptive(100, Duration::from_millis(100)))
            .join_with(s2, |x| *x as u8 % $m, |x| *x as u8 % $m);
        let ship = run_test!(@ship_pre, $ship, join);
        let local = run_test!(@local, $local, ship);
        let variant = run_test!(@variant, $variant, local);
        let res = run_test!(@ship_post, $ship, variant).collect_vec();
        $env.execute_blocking();
        if let Some(res) = res.get() {
            let res = res.into_iter().sorted().collect_vec();
            let expected = run_test!(@get_expected, $variant, $n1, $n2, $m);
            assert_eq!(res, expected);
        }
    }};
    // ship strategy
    (@ship_pre, hash, $prev:expr) => { $prev.ship_hash() };
    (@ship_pre, broadcast_right, $prev:expr) => { $prev.ship_broadcast_right() };
    (@ship_post, hash, $prev:expr) => { $prev.unkey() };
    (@ship_post, broadcast_right, $prev:expr) => { $prev };
    // local strategy
    (@local, hash, $prev:expr) => { $prev.local_hash() };
    (@local, sort_merge, $prev:expr) => { $prev.local_sort_merge() };
    // join variant
    (@variant, inner, $prev:expr) => { $prev.inner() };
    (@variant, left, $prev:expr) => { $prev.left() };
    (@variant, outer, $prev:expr) => { $prev.outer() };
    // expected results
    (@get_expected, inner, $n1:expr, $n2:expr, $m:expr) => {{
        build_expected_inner($n1, $n2, $m)
    }};
    (@get_expected, left, $n1:expr, $n2:expr, $m:expr) => {{
        build_expected_left($n1, $n2, $m)
    }};
    (@get_expected, outer, $n1:expr, $n2:expr, $m:expr) => {{
        build_expected_outer($n1, $n2, $m)
    }};
}

macro_rules! run_test_shortcut {
    ($env:expr, $n1:expr, $n2:expr, $m:expr, $variant:tt) => {{
        let s1 = $env.stream(IteratorSource::new(0..$n1));
        let s2 = $env.stream(IteratorSource::new(0..$n2));
        let join = s1
            .batch_mode(BatchMode::adaptive(100, Duration::from_millis(100)));
        let res = run_test_shortcut!(@variant, $variant, join, s2, |x: &u16| *x as u8 % $m, |x: &u32| *x as u8 % $m);
        let res = res.unkey().collect_vec();
        $env.execute_blocking();
        if let Some(res) = res.get() {
            let res = res.into_iter().sorted().collect_vec();
            let expected = run_test!(@get_expected, $variant, $n1, $n2, $m);
            assert_eq!(res, expected);
        }
    }};
    // join variant
    (@variant, inner, $prev:expr, $rhs:expr, $k1:expr, $k2:expr) => { $prev.join($rhs, $k1, $k2) };
    (@variant, left, $prev:expr, $rhs:expr, $k1:expr, $k2:expr) => { $prev.left_join($rhs, $k1, $k2) };
    (@variant, outer, $prev:expr, $rhs:expr, $k1:expr, $k2:expr) => { $prev.outer_join($rhs, $k1, $k2) };
}

fn build_expected_outer(n1: u16, n2: u32, m: u8) -> Vec<(u8, (Option<u16>, Option<u32>))> {
    let mut expected = vec![];
    let mut used_right = HashSet::new();
    for a in 0..n1 {
        let mut matched = false;
        for b in 0..n2 {
            if (a as u8) % m == (b as u8) % m {
                expected.push((a as u8 % m, (Some(a), Some(b))));
                used_right.insert(b);
                matched = true;
            }
        }
        if !matched {
            expected.push((a as u8 % m, (Some(a), None)));
        }
    }
    for b in 0..n2 {
        if !used_right.contains(&b) {
            expected.push((b as u8 % m, (None, Some(b))));
        }
    }
    expected.sort_unstable();
    expected
}

fn build_expected_inner(n1: u16, n2: u32, m: u8) -> Vec<(u8, (u16, u32))> {
    build_expected_outer(n1, n2, m)
        .into_iter()
        .filter_map(|(k, lr)| match lr {
            (Some(l), Some(r)) => Some((k, (l, r))),
            _ => None,
        })
        .collect_vec()
}

fn build_expected_left(n1: u16, n2: u32, m: u8) -> Vec<(u8, (u16, Option<u32>))> {
    build_expected_outer(n1, n2, m)
        .into_iter()
        .filter_map(|(k, lr)| match lr {
            (Some(l), r) => Some((k, (l, r))),
            _ => None,
        })
        .collect_vec()
}

#[test]
fn join_shortcut() {
    TestHelper::local_remote_env(|mut env| {
        run_test_shortcut!(env, 5, 10, 7, inner);
    });
}

#[test]
fn left_join_shortcut() {
    TestHelper::local_remote_env(|mut env| {
        run_test_shortcut!(env, 5, 10, 7, left);
    });
}

#[test]
fn outer_join_shortcut() {
    TestHelper::local_remote_env(|mut env| {
        run_test_shortcut!(env, 5, 10, 7, outer);
    });
}

#[test]
fn join_hash_hash_inner() {
    TestHelper::local_remote_env(|mut env| {
        run_test!(env, 5, 10, 7, hash, hash, inner);
    });
}

#[test]
fn join_hash_sort_merge_inner() {
    TestHelper::local_remote_env(|mut env| {
        run_test!(env, 5, 10, 7, hash, sort_merge, inner);
    });
}

#[test]
fn join_hash_hash_inner_big() {
    TestHelper::local_remote_env(|mut env| {
        run_test!(env, 200, 200, 7, hash, hash, inner);
    });
}

#[test]
fn join_hash_sort_merge_inner_big() {
    TestHelper::local_remote_env(|mut env| {
        run_test!(env, 200, 200, 7, hash, sort_merge, inner);
    });
}

#[test]
fn join_bc_hash_inner() {
    TestHelper::local_remote_env(|mut env| {
        run_test!(env, 5, 10, 7, broadcast_right, hash, inner);
    });
}

#[test]
fn join_bc_sort_merge_inner() {
    TestHelper::local_remote_env(|mut env| {
        run_test!(env, 5, 10, 7, broadcast_right, sort_merge, inner);
    });
}

#[test]
fn join_hash_hash_left() {
    TestHelper::local_remote_env(|mut env| {
        run_test!(env, 5, 10, 7, hash, hash, left);
    });
}

#[test]
fn join_hash_sort_merge_left() {
    TestHelper::local_remote_env(|mut env| {
        run_test!(env, 5, 10, 7, hash, sort_merge, left);
    });
}

#[test]
fn join_bc_hash_left() {
    TestHelper::local_remote_env(|mut env| {
        run_test!(env, 5, 10, 7, broadcast_right, hash, left);
    });
}

#[test]
fn join_bc_sort_merge_left() {
    TestHelper::local_remote_env(|mut env| {
        run_test!(env, 5, 10, 7, broadcast_right, sort_merge, left);
    });
}

#[test]
fn join_hash_hash_outer1() {
    TestHelper::local_remote_env(|mut env| {
        run_test!(env, 5, 10, 7, hash, hash, outer);
    });
}

#[test]
fn join_hash_sort_merge_outer1() {
    TestHelper::local_remote_env(|mut env| {
        run_test!(env, 5, 10, 7, hash, sort_merge, outer);
    });
}

#[test]
fn join_hash_hash_outer2() {
    TestHelper::local_remote_env(|mut env| {
        run_test!(env, 10, 5, 7, hash, hash, outer);
    });
}

#[test]
fn join_hash_sort_merge_outer2() {
    TestHelper::local_remote_env(|mut env| {
        run_test!(env, 10, 5, 7, hash, sort_merge, outer);
    });
}

#[test]
fn self_join() {
    TestHelper::local_remote_env(|mut env| {
        let n = 200u32;
        let s1 = env
            .stream(IteratorSource::new(0..n))
            .batch_mode(BatchMode::adaptive(100, Duration::from_millis(100)));
        let mut splits = s1.split(2).into_iter();

        let s1 = splits.next().unwrap();
        let s2 = splits.next().unwrap().shuffle().map(|n| n * 2);
        let res = s1.join(s2, |n| *n % 2, |n| *n % 2).unkey().collect_vec();
        env.execute_blocking();

        if let Some(mut res) = res.get() {
            let mut expected = vec![];
            for a in 0..n {
                for b in 0..n {
                    if a % 2 == 0 {
                        expected.push((0, (a, 2 * b)));
                    }
                }
            }
            res.sort_unstable();
            expected.sort_unstable();
            assert_eq!(res, expected);
        }
    });
}

#[test]
fn join_in_loop() {
    TestHelper::local_remote_env(|mut env| {
        let n = 200u32;
        let n_iter = 10;
        let s = env
            .stream(IteratorSource::new(0..n))
            .shuffle()
            .batch_mode(BatchMode::adaptive(100, Duration::from_millis(100)));

        let state = s
            .replay(
                n_iter,
                0,
                |s, _| {
                    let mut splits = s.split(2).into_iter();
                    let s1 = splits.next().unwrap();
                    let s2 = splits.next().unwrap().shuffle().map(|n| n * 3);
                    s1.join(s2, |n| *n % 2, |n| *n % 2)
                        .unkey()
                        .map(|(k, (l, r))| k + l + r)
                },
                |delta: &mut u32, item| *delta += item,
                |state, delta| *state += delta,
                |_| true,
            )
            .collect_vec();
        env.execute_blocking();
        if let Some(state) = state.get() {
            let state = state[0];
            let mut expected = 0;
            for _ in 0..n_iter {
                for a in 0..n {
                    for b in 0..n {
                        if a % 2 == (3 * b) % 2 {
                            expected += a % 2 + a + 3 * b;
                        }
                    }
                }
            }
            assert_eq!(state, expected);
        }
    });
}
