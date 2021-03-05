use std::env;

use regex::Regex;

use rstream::config::EnvironmentConfig;
use rstream::environment::StreamEnvironment;
use rstream::operator::source;

#[async_std::main]
async fn main() {
    env_logger::init();

    let path = env::args()
        .nth(1)
        .expect("Pass the dataset path as an argument");

    // let config = EnvironmentConfig::local(4);
    let config = EnvironmentConfig::remote("config.yml").await.unwrap();
    let mut env = StreamEnvironment::new(config);
    let source = source::FileSource::new(path);
    let tokenizer = Tokenizer::new();
    let stream = env
        .stream(source)
        .flat_map(move |line| tokenizer.tokenize(line))
        .group_by(|word| word.clone())
        .fold(0, |count, _word| count + 1)
        .unkey();
    let result = stream.collect_vec();
    env.execute().await;
    println!("Output: {:?}", result.get());
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
            .filter(|word| word.len() > 0)
            .map(|t| t.to_lowercase())
            .collect()
    }
}
