use std::time::Duration;

use common::NoirBenchBuilder;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use fxhash::FxHashMap;
use noir::operator::Operator;
use noir::EnvironmentConfig;
use noir::Stream;
use noir::StreamEnvironment;
use rand::prelude::*;
use rand::rngs::SmallRng;
use serde::{Deserialize, Serialize};

mod common;

#[derive(Serialize, Deserialize, Clone, Default)]
struct State {
    /// Maps each vertex to its current component.
    component: FxHashMap<u64, u64>,
    /// Whether the state has been updated in the current iteration.
    updated: bool,
    /// Number of iterations.
    iteration_count: usize,
}

impl State {
    fn new() -> Self {
        Self {
            component: FxHashMap::default(),
            updated: false,
            iteration_count: 0,
        }
    }
}

fn connected(input: Stream<(u64, u64), impl Operator<(u64, u64)> + 'static>) {
    let mut s = input.split(2);

    let nodes = s
        .pop()
        .unwrap()
        .flat_map(|(a, b)| [a, b])
        .group_by_fold(|x| *x, (), |_, _| (), |_, _| ())
        .unkey()
        .map(|(k, _)| k);

    let edges = s
        .pop()
        .unwrap()
        // edges are undirected
        .flat_map(|(x, y)| vec![(x, y), (y, x)]);

    let (result, dropme) = nodes
        // put each node in its own component
        .map(|x| (x, x))
        .iterate(
            10000,
            State::new(),
            move |s, state| {
                // propagate the component changes of the last iteration
                s.join(edges, |(x, _component)| *x, |(x, _y)| *x)
                    // for each component change (x, component) and each edge (x, y),
                    // propagate the change to y
                    .map(|(_, ((_x, component), (_, y)))| (y, component))
                    .drop_key()
                    // each vertex is assigned to the component with minimum id
                    .group_by_min_element(|(x, _component)| *x, |(_x, component)| *component)
                    .drop_key()
                    // filter only actual changes to component assignments
                    .filter_map(move |(x, component)| {
                        let old_component = *state.get().component.get(&x).unwrap_or(&x);
                        if old_component <= component {
                            None
                        } else {
                            Some((x, component))
                        }
                    })
            },
            |delta: &mut Vec<(u64, u64)>, (x, component)| {
                // collect all changes
                delta.push((x, component));
            },
            |state, changes| {
                // apply all changes
                state.updated = state.updated || !changes.is_empty();
                for (x, component) in changes {
                    state.component.insert(x, component);
                }
            },
            |state| {
                // stop if there were no changes
                let condition = state.updated;
                state.updated = false;
                state.iteration_count += 1;
                condition
            },
        );
    // we are interested in the state
    let _result = result.collect_vec();
    dropme.for_each(|_| {});
}

fn bench_main(c: &mut Criterion) {
    let mut g = c.benchmark_group("connected");
    g.sample_size(30);
    g.warm_up_time(Duration::from_secs(3));
    g.measurement_time(Duration::from_secs(12));
    g.throughput(Throughput::Elements(1));
    g.bench_function("connected", |b| {
        let builder = NoirBenchBuilder::new(
            || StreamEnvironment::new(EnvironmentConfig::local(4)),
            |n: u64, env: &mut StreamEnvironment| {
                let source = env.stream_par_iter(move |id, peers| {
                    let mut rng: SmallRng = SeedableRng::seed_from_u64(id ^ 0xdeadbeef);
                    (0..n / peers).map(move |_| rng.gen())
                });

                connected(source);
            },
        );
        b.iter_custom(|n| builder.bench(n))
    });
    g.finish();
}

criterion_group!(benches, bench_main);
criterion_main!(benches);
