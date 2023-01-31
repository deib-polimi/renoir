use std::{ops::AddAssign, time::Instant};

use noir::{prelude::*, IterationStateHandle};
use serde::{Deserialize, Serialize};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const EPS: f64 = 1e-8;
const DAMPENING: f64 = 0.85;

#[derive(Clone, Serialize, Deserialize, Debug)]
enum Msg {
    Init { rank_init: f64, adj: Vec<u64> },
    Update(f64),
    Output(f64),
}

impl Msg {
    fn rank(&self) -> f64 {
        match self {
            Msg::Init { rank_init, .. } => *rank_init,
            Msg::Update(rank) => *rank,
            Msg::Output(rank) => *rank,
        }
    }
}

impl AddAssign<Self> for Msg {
    fn add_assign(&mut self, rhs: Self) {
        match (self, rhs) {
            (Msg::Update(a), Msg::Update(b)) => *a += b,
            _ => panic!("Summing incompatible Msg"),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
struct TerminationCond {
    something_changed: bool,
    last_iteration: bool,
}

fn main() {
    let (config, args) = EnvironmentConfig::from_args();
    if args.len() != 4 {
        panic!("Pass the number of iterations, number of pages, pages dataset and links dataset as arguments");
    }
    let num_iterations: usize = args[0].parse().expect("Invalid number of iterations");
    let num_pages: usize = args[1].parse().expect("Invalid number of pages");
    let path_pages = &args[2];
    let path_links = &args[3];

    let mut env = StreamEnvironment::new(config);

    env.spawn_remote_workers();

    let pages_source = CsvSource::<u64>::new(path_pages).has_headers(false);
    let links_source = CsvSource::<(u64, u64)>::new(path_links).has_headers(false);

    let adj_list = env
        .stream(links_source)
        // construct adjacency list
        .group_by_fold(
            |(x, _y)| *x,
            Vec::new(),
            |edges, (_x, y)| edges.push(y),
            |edges1, mut edges2| edges1.append(&mut edges2),
        )
        .unkey();

    let init = env
        .stream(pages_source)
        .left_join(adj_list, |x| *x, |x| x.0)
        .map(move |(_, (_, vec))| Msg::Init {
            rank_init: 1.0 / num_pages as f64,
            adj: vec.map(|(_, v)| v).unwrap_or_default(),
        })
        .unkey();

    let (state, out) = init.iterate(
        num_iterations,
        TerminationCond::default(),
        move |s, state: IterationStateHandle<TerminationCond>| {
            s.to_keyed()
                .rich_flat_map({
                    let mut adj_list = vec![];
                    let mut rank = 0f64;

                    move |(x, msg): (_, Msg)| {
                        if state.get().last_iteration {
                            return vec![(*x, Msg::Output(rank))];
                        }
                        match msg {
                            Msg::Init { rank_init, adj } => {
                                adj_list = adj;
                                vec![(*x, Msg::Update(rank_init))]
                            }
                            Msg::Update(rank_update) => {
                                rank += rank_update;
                                let mut update = Vec::with_capacity(adj_list.len() + 1);

                                if !adj_list.is_empty() {
                                    let distribute = (rank * DAMPENING) / adj_list.len() as f64;
                                    update.extend(
                                        adj_list.iter().map(move |y| (*y, Msg::Update(distribute))),
                                    );
                                }

                                update.push((
                                    *x,
                                    Msg::Update((1.0 - DAMPENING) / num_pages as f64 - rank),
                                ));

                                update
                            }
                            Msg::Output(_) => panic!("should never have output here"),
                        }
                    }
                })
                .drop_key()
                .group_by_sum(|x| x.0, |x| x.1)
                .unkey()
        },
        |changed: &mut TerminationCond, x| {
            changed.something_changed = if let Msg::Update(a) = x.1 {
                a.abs() > EPS
            } else {
                false
            };
            if let Msg::Output { .. } = x.1 {
                changed.last_iteration = true;
            }
        },
        |global, local| {
            global.something_changed |= local.something_changed;
            global.last_iteration |= local.last_iteration;
        },
        |s| {
            let cond = !s.last_iteration;
            if !s.something_changed {
                s.last_iteration = true;
            }
            s.something_changed = false;
            cond
        },
    );

    let result = out.map(|(k, v)| (k, v.rank())).collect_vec();
    state.for_each(|_| {});

    let start = Instant::now();
    env.execute();
    let elapsed = start.elapsed();

    if let Some(mut res) = result.get() {
        eprintln!("Output: {:?}", res.len());
        if cfg!(debug_assertions) {
            res.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            for (x, rank) in res.iter().take(3) {
                eprintln!("{x}: {rank}");
            }
            eprintln!("...");
            for (x, rank) in res.iter().rev().take(3).rev() {
                eprintln!("{x}: {rank}");
            }
        }
    }
    eprintln!("Elapsed: {elapsed:?}");
}
