use std::time::{Duration, Instant};

use regex::Regex;

use renoir::prelude::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let (config, args) = RuntimeConfig::from_args();
    if args.len() != 2 {
        panic!("Pass the dataset path as an argument");
    }
    let path = &args[1];

    config.spawn_remote_workers();
    let env = StreamContext::new(config);

    let source = FileSource::new(path);
    let tokenizer = Tokenizer::new();
    env.stream(source)
        .batch_mode(BatchMode::adaptive(1000, Duration::from_millis(100)))
        .flat_map(move |line| tokenizer.tokenize(line))
        .group_by(|word| word.clone())
        .window(CountWindow::sliding(10, 5))
        .fold(0, |count, _word| *count += 1)
        .for_each(|_| {});
    let start = Instant::now();
    env.execute_blocking();
    let elapsed = start.elapsed();
    eprintln!("Elapsed: {elapsed:?}");
}

#[derive(Clone)]
struct Tokenizer {
    re: Regex,
}

impl Tokenizer {
    fn new() -> Self {
        Self {
            re: Regex::new(r"[^A-Za-z]+").unwrap(),
        }
    }
    fn tokenize(&self, value: String) -> Vec<String> {
        self.re
            .replace_all(&value, " ")
            .split_ascii_whitespace()
            .filter(|word| !word.is_empty())
            .map(|t| t.to_lowercase())
            .collect()
    }
}
