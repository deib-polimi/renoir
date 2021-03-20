use std::env;
use std::time::Instant;

use regex::Regex;

use rstream::block::BatchMode;
use rstream::config::EnvironmentConfig;
use rstream::environment::StreamEnvironment;
use rstream::operator::source;

fn main() {
    env_logger::init();

    let path = env::args()
        .nth(1)
        .expect("Pass the dataset path as an argument");
    let config = if let Some(ncore) = env::args().nth(2) {
        EnvironmentConfig::local(ncore.parse().expect("invalid number of cores"))
    } else {
        EnvironmentConfig::remote("config.yml").unwrap()
    };

    let mut env = StreamEnvironment::new(config);

    env.spawn_remote_workers();

    let source = source::FileSource::new(path);
    let tokenizer = Tokenizer::new();
    let stream = env
        .stream(source)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map(move |line| tokenizer.tokenize(line))
        .group_by(|word| word.clone())
        .fold(0, |count, _word| count + 1)
        .unkey();
    let result = stream.collect_vec();
    let start = Instant::now();
    env.execute();
    let elapsed = start.elapsed();
    if let Some(res) = result.get() {
        eprintln!("Output: {:?}", res.len());
    }
    eprintln!("Elapsed: {:?}", elapsed);
}

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
