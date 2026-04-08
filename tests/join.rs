#![allow(clippy::type_complexity)]

use std::collections::HashSet;
use std::hash::Hash;
use std::time::Duration;

use itertools::Itertools;

use renoir::BatchMode;
use utils::TestHelper;

mod utils;
macro_rules! run_test {
    ($env:expr, $n1:expr, $n2:expr, $m:expr, $ship:tt, $local:tt, $variant:tt) => {{
        let s1 = $env.stream_iter(0..$n1);
        let s2 = $env.stream_iter(0..$n2);
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
        let s1 = $env.stream_iter(0..$n1);
        let s2 = $env.stream_iter(0..$n2);
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

fn reference_full_join<K: Eq + Hash + Clone + Ord, T1: Clone + Ord, T2: Clone + Ord>(
    left: impl IntoIterator<Item = (K, T1)>,
    left_is_outer: bool,
    right: impl IntoIterator<Item = (K, T2)>,
    right_is_outer: bool,
) -> Vec<(K, (Option<T1>, Option<T2>))> {
    use std::collections::HashMap;
    let mut expected = Vec::new();

    let mut left_map: HashMap<K, Vec<T1>> = HashMap::new();
    for (k, v) in left.into_iter() {
        left_map.entry(k).or_default().push(v);
    }

    let mut right_map: HashMap<K, Vec<T2>> = HashMap::new();
    for (k, v) in right.into_iter() {
        right_map.entry(k).or_default().push(v);
    }

    for (key, v1) in left_map.iter() {
        match right_map.get(key) {
            Some(matches) => {
                // Inner
                for lhs in v1 {
                    for rhs in matches {
                        expected.push((key.clone(), (Some(lhs.clone()), Some(rhs.clone()))));
                    }
                }
            }
            None if left_is_outer => {
                // Outer Left
                for lhs in v1 {
                    expected.push((key.clone(), (Some(lhs.clone()), None)));
                }
            }
            None => {
                // Inner Left
            }
        }
    }

    for (key, v2) in right_map.iter() {
        match left_map.get(key) {
            None if right_is_outer => {
                for rhs in v2 {
                    expected.push((key.clone(), (None, Some(rhs.clone()))));
                }
            }
            Some(_) | None => {} // Already covered before
        }
    }

    expected.sort_unstable();
    expected
}

fn reference_left<K: Eq + Hash + Clone + Ord, T1: Clone + Ord, T2: Clone + Ord>(
    left: impl IntoIterator<Item = (K, T1)>,
    right: impl IntoIterator<Item = (K, T2)>,
) -> Vec<(K, (T1, Option<T2>))> {
    let r = reference_full_join(left, true, right, false);
    r.into_iter()
        .map(|(k, (a, b))| (k, (a.unwrap(), b)))
        .collect()
}

fn reference_inner<K: Eq + Hash + Clone + Ord, T1: Clone + Ord, T2: Clone + Ord>(
    left: impl IntoIterator<Item = (K, T1)>,
    right: impl IntoIterator<Item = (K, T2)>,
) -> Vec<(K, (T1, T2))> {
    let r = reference_full_join(left, false, right, false);
    r.into_iter()
        .map(|(k, (a, b))| (k, (a.unwrap(), b.unwrap())))
        .collect()
}

fn reference_outer<K: Eq + Hash + Clone + Ord, T1: Clone + Ord, T2: Clone + Ord>(
    left: impl IntoIterator<Item = (K, T1)>,
    right: impl IntoIterator<Item = (K, T2)>,
) -> Vec<(K, (Option<T1>, Option<T2>))> {
    reference_full_join(left, true, right, true)
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
    TestHelper::local_remote_env(|env| {
        run_test_shortcut!(env, 5, 10, 7, inner);
    });
}

#[test]
fn left_join_shortcut() {
    TestHelper::local_remote_env(|env| {
        run_test_shortcut!(env, 5, 10, 7, left);
    });
}

#[test]
fn outer_join_shortcut() {
    TestHelper::local_remote_env(|env| {
        run_test_shortcut!(env, 5, 10, 7, outer);
    });
}

#[test]
fn join_hash_hash_inner() {
    TestHelper::local_remote_env(|env| {
        run_test!(env, 5, 10, 7, hash, hash, inner);
    });
}

#[test]
fn join_hash_sort_merge_inner() {
    TestHelper::local_remote_env(|env| {
        run_test!(env, 5, 10, 7, hash, sort_merge, inner);
    });
}

#[test]
fn join_hash_hash_inner_big() {
    TestHelper::local_remote_env(|env| {
        run_test!(env, 200, 200, 7, hash, hash, inner);
    });
}

#[test]
fn join_hash_sort_merge_inner_big() {
    TestHelper::local_remote_env(|env| {
        run_test!(env, 200, 200, 7, hash, sort_merge, inner);
    });
}

#[test]
fn join_bc_hash_inner() {
    TestHelper::local_remote_env(|env| {
        run_test!(env, 5, 10, 7, broadcast_right, hash, inner);
    });
}

#[test]
fn join_bc_sort_merge_inner() {
    TestHelper::local_remote_env(|env| {
        run_test!(env, 5, 10, 7, broadcast_right, sort_merge, inner);
    });
}

#[test]
fn join_hash_hash_left() {
    TestHelper::local_remote_env(|env| {
        run_test!(env, 5, 10, 7, hash, hash, left);
    });
}

#[test]
fn join_hash_sort_merge_left() {
    TestHelper::local_remote_env(|env| {
        run_test!(env, 5, 10, 7, hash, sort_merge, left);
    });
}

#[test]
fn join_bc_hash_left() {
    TestHelper::local_remote_env(|env| {
        run_test!(env, 5, 10, 7, broadcast_right, hash, left);
    });
}

#[test]
fn join_bc_sort_merge_left() {
    TestHelper::local_remote_env(|env| {
        run_test!(env, 5, 10, 7, broadcast_right, sort_merge, left);
    });
}

#[test]
fn join_hash_hash_outer1() {
    TestHelper::local_remote_env(|env| {
        run_test!(env, 5, 10, 7, hash, hash, outer);
    });
}

#[test]
fn join_hash_sort_merge_outer1() {
    TestHelper::local_remote_env(|env| {
        run_test!(env, 5, 10, 7, hash, sort_merge, outer);
    });
}

#[test]
fn join_hash_hash_outer2() {
    TestHelper::local_remote_env(|env| {
        run_test!(env, 10, 5, 7, hash, hash, outer);
    });
}

#[test]
fn join_hash_sort_merge_outer2() {
    TestHelper::local_remote_env(|env| {
        run_test!(env, 10, 5, 7, hash, sort_merge, outer);
    });
}

#[test]
fn self_join() {
    TestHelper::local_remote_env(|env| {
        let n = 200u32;
        let s1 = env
            .stream_iter(0..n)
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
    TestHelper::local_remote_env(|env| {
        let n = 200u32;
        let n_iter = 10;
        let s = env
            .stream_iter(0..n)
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

fn batch_mode_strategy() -> impl proptest::strategy::Strategy<Value = BatchMode> {
    use proptest::prelude::*;
    proptest::prop_oneof![
        Just(BatchMode::Single),
        Just(BatchMode::fixed(4)),
        Just(BatchMode::adaptive(16, Duration::from_millis(20))),
        Just(BatchMode::timed(16, Duration::from_millis(20))),
    ]
}

#[test]
fn left_join_proptest() {
    use proptest::collection::vec;
    use proptest::prelude::*;

    proptest!(|(
        left in vec(0u32..256, 0..100usize),
        right in vec(0u32..256, 0..100usize),
        m in 1u32..8u32,
        n in 1u32..8u32,
        bm in batch_mode_strategy(),
    )| {        TestHelper::local_remote_env(move |env| {
            let s1 = env.stream_iter(left.clone().into_iter());
            let s2 = env.stream_iter(right.clone().into_iter());
            let res = s1
                .batch_mode(bm)
                .left_join(s2, move |x| *x % m, move |x| *x % n)
                .unkey()
                .collect_vec();
            env.execute_blocking();
            if let Some(mut res) = res.get() {
                res.sort_unstable();

                let left = left.clone().into_iter().map(|l| (l % m, l));
                let right = right.clone().into_iter().map(|r| (r % n, r));
                let expected = reference_left(left, right);

                assert_eq!(res, expected);
            }
        });
    });
}

#[test]
fn inner_join_proptest() {
    use proptest::collection::vec;
    use proptest::prelude::*;

    proptest!(|(
        left in vec(0u32..256, 0..100usize),
        right in vec(0u32..256, 0..100usize),
        m in 1u32..8u32,
        n in 1u32..8u32,
        bm in batch_mode_strategy(),
    )| {        TestHelper::local_remote_env(move |env| {
            let s1 = env.stream_iter(left.clone().into_iter());
            let s2 = env.stream_iter(right.clone().into_iter());
            let res = s1
                .batch_mode(bm)
                .join(s2, move |x| *x % m, move |x| *x % n)
                .unkey()
                .collect_vec();
            env.execute_blocking();
            if let Some(mut res) = res.get() {
                res.sort_unstable();

                let left = left.clone().into_iter().map(|l| (l % m, l));
                let right = right.clone().into_iter().map(|r| (r % n, r));
                let expected = reference_inner(left, right);

                assert_eq!(res, expected);
            }
        });
    });
}

#[test]
fn outer_join_proptest() {
    use proptest::collection::vec;
    use proptest::prelude::*;

    proptest!(|(
        left in vec(0u32..256, 0..100usize),
        right in vec(0u32..256, 0..100usize),
        m in 1u32..8u32,
        n in 1u32..8u32,
        bm in batch_mode_strategy(),
    )| {        TestHelper::local_remote_env(move |env| {
            let s1 = env.stream_iter(left.clone().into_iter());
            let s2 = env.stream_iter(right.clone().into_iter());
            let res = s1
                .batch_mode(bm)
                .outer_join(s2, move |x| *x % m, move |x| *x % n)
                .unkey()
                .collect_vec();
            env.execute_blocking();
            if let Some(mut res) = res.get() {
                res.sort_unstable();

                let left = left.clone().into_iter().map(|l| (l % m, l));
                let right = right.clone().into_iter().map(|r| (r % n, r));
                let expected = reference_outer(left, right);

                assert_eq!(res, expected);
            }
        });
    });
}

#[test]
fn left_join_specific() {
    use std::sync::Arc;

    let left = vec![(1, 2), (1, 1), (2, 5), (2, 6), (3, 7)];
    let right = vec![(1, 3), (1, 4)];

    let left_data = Arc::new(left);
    let right_data = Arc::new(right);

    TestHelper::local_remote_env(move |env| {
        let s1 = env.stream_iter((*left_data).clone().into_iter());
        let s2 = env.stream_iter((*right_data).clone().into_iter());
        let res = s1
            .batch_mode(BatchMode::fixed(1))
            .left_join(s2, move |x| x.0, move |x| x.0)
            .drop_key()
            .map(|(a, b)| (a.1, b.map(|b| b.1)))
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            let mut expected: Vec<(u32, Option<u32>)> = vec![
                (2, Some(3)),
                (2, Some(4)),
                (1, Some(3)),
                (1, Some(4)),
                (5, None),
                (6, None),
                (7, None),
            ];

            expected.sort_unstable();
            res.sort_unstable();
            assert_eq!(res, expected);
        }
    });
}
