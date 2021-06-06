use std::collections::{HashMap, HashSet};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use rstream::config::EnvironmentConfig;
use rstream::environment::StreamEnvironment;
use rstream::operator::source;

#[derive(Serialize, Deserialize, Clone, Default)]
struct State {
    /// Maps each vertex to its current component.
    component: HashMap<u64, u64>,
    /// Whether the state has been updated in the current iteration.
    updated: bool,
    /// Number of iterations.
    iteration_count: usize,
}

impl State {
    fn new() -> Self {
        Default::default()
    }

    fn get_component(&self, vertex: u64) -> Option<u64> {
        self.component.get(&vertex).cloned()
    }
}

fn main() {
    let (config, args) = EnvironmentConfig::from_args();
    if args.len() != 3 {
        panic!("Pass the number of iterations, vertex dataset and edges dataset as arguments");
    }
    let num_iterations: usize = args[0].parse().expect("Invalid number of iterations");
    let path_vertices = &args[1];
    let path_edges = &args[2];

    let mut env = StreamEnvironment::new(config);

    env.spawn_remote_workers();

    let vertices_source = source::CsvSource::<u64>::new(path_vertices).has_headers(false);
    let edges_source = source::CsvSource::<(u64, u64)>::new(path_edges)
        .delimiter(b' ')
        .has_headers(false);

    let edges = env
        .stream(edges_source)
        // edges are undirected
        .flat_map(|(x, y)| vec![(x, y), (y, x)]);

    let (result, dropme) = env
        .stream(vertices_source)
        // put each node in its own component
        .map(|x| (x, x))
        .iterate(
            num_iterations,
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
                        let old_component = state.get().get_component(x);
                        match old_component {
                            Some(old_component) if old_component <= component => None,
                            _ => Some((x, component)),
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
    let result = result.collect_vec();
    dropme.for_each(|_| {});

    let start = Instant::now();
    env.execute();
    let elapsed = start.elapsed();

    if let Some(res) = result.get() {
        let final_state = &res[0];
        if cfg!(debug) {
            for (x, component) in &final_state.component {
                eprintln!("{} -> {}", x, component);
            }
        }
        let components = final_state.component.values().collect::<HashSet<_>>().len();
        eprintln!("Number of components: {:?}", components);
        eprintln!("Iterations: {:?}", final_state.iteration_count);
    }
    eprintln!("Elapsed: {:?}", elapsed);
}
