use std::time::Instant;

use noir::prelude::*;
use rand::prelude::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const INIT: i64 = 6_000_000;
const HOP: i64 = 1_000_000;

fn main() {
    env_logger::init();
    let (config, args) = EnvironmentConfig::from_args();

    // command-line args: numbers of nodes and edges in the random graph.
    let nodes: u64 = args[0].parse().unwrap();
    let edges: u64 = args[1].parse().unwrap();
    let max_iter: usize = 10000;

    let mut env = StreamEnvironment::new(config);
    env.spawn_remote_workers();

    let source = env
        .stream_par_iter(move |index, peers| {
            let mut rng1: SmallRng = SeedableRng::seed_from_u64(index);
            (0..(edges / peers)).map(move |_| (rng1.gen_range(0..nodes), rng1.gen_range(0..nodes)))
        })
        .batch_mode(BatchMode::fixed(1024));

    let mut split = source.split(2);

    let adj_list = split
        .pop()
        .unwrap()
        .group_by(|(x, _y)| *x)
        .fold(Vec::new(), |edges, (_x, y)| edges.push(y))
        .unkey();

    let init = split
        .pop()
        .unwrap()
        .flat_map(|(x, y)| [x, y])
        .group_by_fold(|x| *x, (), |_, _| (), |_, _| ())
        .unkey()
        .left_join(adj_list, |x| x.0, |x| x.0)
        .map(|(_, (_, vec))| (INIT, vec.map(|(_, v)| v).unwrap_or_default()));

    let out = init.delta_iterate(
        max_iter,
        |_, (rank, _), delta_rank| *rank += delta_rank,
        |x, (rank, adj_list)| {
            let mut update = Vec::with_capacity(adj_list.len() + 1);

            if !adj_list.is_empty() {
                let degree = adj_list.len() as i64;
                let new_share = (*rank * 5) / (6 * degree);
                for adj in adj_list {
                    update.push((*adj, new_share));
                }
            }

            update.push((*x, HOP - *rank));
            update
        },
        |_, (rank, _)| rank,
        |u| *u != 0,
        move |s| s.flatten().drop_key().group_by_sum(|(x, _)| *x, |x| x.1),
    );

    out.for_each(|x| {
        println!("{}:{}", x.0, x.1);
        core::hint::black_box(x);
    });

    let start = Instant::now();
    env.execute_blocking();
    let elapsed = start.elapsed();

    eprintln!("Elapsed {elapsed:?}");
}
