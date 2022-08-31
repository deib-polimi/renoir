use noir::prelude::*;
use std::time::Instant;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let (config, args) = EnvironmentConfig::from_args();
    if args.len() != 1 {
        panic!("Pass the number of integers to check");
    }
    let limit: i32 = args[0].parse().unwrap();
    let num_iter = 1000;

    let mut env = StreamEnvironment::new(config);

    env.spawn_remote_workers();

    let source = IteratorSource::new(1..limit);
    let (state, remaining) = env
        .stream(source)
        .shuffle()
        .map(|n| (0, n, n)) // (# of steps, current val, original val)
        .iterate(
            num_iter, // number of iterations
            (0, 0),   // initial state: (max steps, value)
            // loop body
            |s, _state| {
                s.filter(|(_c, cur, _n)| *cur > 1) // remove ones
                    .map(|(c, cur, n)| {
                        // Collatz
                        if cur % 2 == 0 {
                            (c + 1, cur / 2, n)
                        } else {
                            (c + 1, cur * 3 + 1, n)
                        }
                    })
            },
            // build a DeltaUpdate: get the maximum number of steps and
            // relative initial value
            |delta: &mut (i32, i32), x| *delta = (x.0, x.2).max(*delta),
            // update the global state with the highest number of steps
            |state, delta| *state = delta.max(*state),
            // loop condition: do all the 1000 iterations
            |_state| true,
        );
    let state = state.collect_vec();
    let remaining = remaining.collect_vec();
    let start = Instant::now();
    env.execute();
    let elapsed = start.elapsed();
    if let Some(state) = state.get() {
        eprintln!("Best: {:?}", state[0]);
    }
    if let Some(remaining) = remaining.get() {
        for rem in remaining {
            eprintln!("Remaining: {:?}", rem);
        }
    }
    eprintln!("Elapsed: {:?}", elapsed);
}
