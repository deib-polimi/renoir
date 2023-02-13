use noir::prelude::*;
use std::time::Instant;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let (config, args) = EnvironmentConfig::from_args();
    if args.len() != 1 {
        panic!("Pass the number of integers to check");
    }
    let limit: u64 = args[0].parse().unwrap();
    let num_iter = 1000;

    let mut env = StreamEnvironment::new(config);

    env.spawn_remote_workers();

    let output = env
        .stream_par_iter(1..limit)
        .batch_mode(BatchMode::fixed(1024))
        .map(move |n| {
            let mut c = 0;
            let mut cur = n;
            while c < num_iter {
                if cur % 2 == 0 {
                    cur /= 2;
                } else {
                    cur = cur * 3 + 1;
                }
                c += 1;
                if cur <= 1 {
                    break;
                }
            }
            (c, n)
        })
        .reduce_assoc(|a, b| a.max(b))
        .collect::<Vec<_>>();

    let start = Instant::now();
    env.execute();
    let elapsed = start.elapsed();
    if let Some(state) = output.get() {
        eprintln!("Best: {state:?}");
    }
    eprintln!("Elapsed: {elapsed:?}");
}
