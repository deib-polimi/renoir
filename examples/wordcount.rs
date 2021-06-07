use std::time::Instant;

use regex::Regex;

use rstream::operator::source::FileSource;
use rstream::BatchMode;
use rstream::EnvironmentConfig;
use rstream::StreamEnvironment;

fn main() {
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
        .group_by(|word| word.clone())
        .fold(0, |count, _word| *count += 1)
        .collect_vec();
    let start = Instant::now();
    env.execute();
    let elapsed = start.elapsed();
    if let Some(res) = result.get() {
        eprintln!("Output: {:?}", res.len());
    }
    eprintln!("Elapsed: {:?}", elapsed);
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
