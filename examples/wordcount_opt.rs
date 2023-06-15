use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::time::Instant;

use noir::prelude::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    env_logger::init();

    let (config, args) = EnvironmentConfig::from_args();
    if args.len() != 1 {
        panic!("Pass the dataset path as an argument");
    }
    let path = &args[0];

    let mut env = StreamEnvironment::new(config);

    env.spawn_remote_workers();

    let result = env
        .stream_file(path)
        .batch_mode(BatchMode::fixed(1024))
        .fold_assoc(
            HashMap::<String, u64, BuildHasherDefault<wyhash::WyHash>>::default(),
            |acc, line| {
                let mut word = String::with_capacity(8);
                for c in line.chars() {
                    if c.is_ascii_alphabetic() {
                        word.push(c.to_ascii_lowercase());
                    } else if !word.is_empty() {
                        let key = std::mem::replace(&mut word, String::with_capacity(8));
                        *acc.entry(key).or_default() += 1;
                    }
                }
            },
            |a, mut b| {
                for (k, v) in b.drain() {
                    *a.entry(k).or_default() += v;
                }
            },
        )
        .collect_vec();

    let start = Instant::now();
    env.execute_blocking();
    let elapsed = start.elapsed();

    if let Some(_res) = result.get() {
        eprintln!("Output: {:?}", _res[0].len());
        println!("{elapsed:?}");

        // use itertools::Itertools;
        // _res.iter()
        //     .sorted_by_key(|t| t.1)
        //     .rev()
        //     .take(10)
        //     .for_each(|(k, v)| eprintln!("{:>10}:{:>10}", k, v));
    }
}
