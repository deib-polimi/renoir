use std::time::Instant;

use regex::Regex;

use renoir::prelude::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// /// Simpler alternative tokenization
// fn tokenize(s: &str) -> Vec<String> {
//     s.split_whitespace().map(str::to_lowercase).collect()
// }

#[cfg(not(feature = "tokio"))]
fn main() {
    tracing_subscriber::fmt::init();
    let (config, args) = RuntimeConfig::from_args();
    if args.len() != 2 {
        panic!("Pass the dataset path as an argument");
    }
    let path = &args[1];

    config.spawn_remote_workers();
    let env = StreamContext::new(config);

    let source = FileSource::new(path);
    let tokenizer = Tokenizer::new();
    let result = env
        .stream(source)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map(move |line| tokenizer.tokenize(line))
        .group_by(|word| word.clone())
        .fold(0, |count, _word| *count += 1)
        .collect_vec();
    let start = Instant::now();
    env.execute_blocking();
    let elapsed = start.elapsed();
    if let Some(_res) = result.get() {
        eprintln!("Output: {:?}", _res.len());
        println!("{elapsed:?}");
    }
}

#[cfg(feature = "tokio")]
#[tokio::main()]
async fn main() {
    tracing_subscriber::fmt::init();
    let (config, args) = RuntimeConfig::from_args();
    if args.len() != 2 {
        panic!("Pass the dataset path as an argument");
    }
    let path = &args[1];

    config.spawn_remote_workers();
    let env = StreamContext::new(config);

    let source = FileSource::new(path);
    let tokenizer = Tokenizer::new();
    let result = env
        .stream(source)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map(move |line| tokenizer.tokenize(line))
        .group_by(|word| word.clone())
        .fold(0, |count, _word| *count += 1)
        .collect_vec();
    let start = Instant::now();
    env.execute().await;
    let elapsed = start.elapsed();
    if let Some(_res) = result.get() {
        eprintln!("Output: {:?}", _res.len());
        println!("{:?}", elapsed);
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
