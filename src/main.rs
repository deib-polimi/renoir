use async_std::stream::from_iter;

use operator::source;

use crate::environment::StreamEnvironment;

mod block;
mod environment;
mod operator;
mod scheduler;
mod stream;
mod worker;

#[async_std::main]
async fn main() {
    let mut env = StreamEnvironment::new();
    let source = source::StreamSource::new(from_iter(0..10));
    let stream = env
        .stream(source)
        .map(|x| x.to_string())
        .shuffle()
        .map(|s| s.len());
    // let result = stream.collect_vec();
    env.execute().await;
}
