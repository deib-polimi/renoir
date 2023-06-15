use std::time::Instant;

use regex::Regex;

use noir::prelude::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(not(feature = "async-tokio"))]
fn main() {
    tracing_subscriber::fmt::init();

    let (config, args) = EnvironmentConfig::from_args();
    if args.len() != 1 {
        panic!("Pass the dataset path as an argument");
    }
    let path = &args[0];

    let mut env = StreamEnvironment::new(config);

    env.spawn_remote_workers();

    let source = FileSource::new(path);
    let tokenizer = Tokenizer::new();
    let result = env
        .stream(source)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map(move |line| tokenizer.tokenize(line))
        .group_by_count(|word: &String| word.clone())
        .collect_vec();
    let start = Instant::now();
    env.execute_blocking();
    let elapsed = start.elapsed();

    if let Some(_res) = result.get() {
        eprintln!("Output: {:?}", _res.len());
        println!("{elapsed:?}");

        // use itertools::Itertools;
        // _res.iter()
        //     .sorted_by_key(|t| t.1)
        //     .rev()
        //     .take(10)
        //     .for_each(|(k, v)| eprintln!("{:>10}:{:>10}", k, v));
    }
}

#[cfg(feature = "async-tokio")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt::init();

    let (config, args) = EnvironmentConfig::from_args();
    if args.len() != 1 {
        panic!("Pass the dataset path as an argument");
    }
    let path = &args[0];

    let mut env = StreamEnvironment::new(config);

    env.spawn_remote_workers();

    let source = FileSource::new(path);
    let tokenizer = Tokenizer::new();
    let result = env
        .stream(source)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map(move |line| tokenizer.tokenize(line))
        .group_by_count(|word: &String| word.clone())
        .collect_vec();
    let start = Instant::now();
    env.execute().await;
    let elapsed = start.elapsed();

    if let Some(_r) = result.get() {
        // println!("OK");
        println!("{:?}", elapsed);
        // _r.iter()
        //     .sorted_by_key(|t| t.1)
        //     .rev()
        //     .take(10)
        //     .for_each(|(k, v)| eprintln!("{:>10}:{:>10}", k, v));
    }
}

#[derive(Clone)]
struct Tokenizer {
    re: Regex,
}

impl Tokenizer {
    fn new() -> Self {
        Self {
            re: Regex::new(r"[A-Za-z]+").unwrap(),
        }
    }
    fn tokenize(&self, value: String) -> Vec<String> {
        self.re
            .find_iter(&value)
            .map(|t| t.as_str().to_lowercase())
            .collect()
    }
}
