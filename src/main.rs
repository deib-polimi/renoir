mod block;
mod environment;
mod operator;
mod source;
mod stream;

use crate::environment::StreamEnvironment;
use async_std::stream::from_iter;

fn main() {
    let env = StreamEnvironment::new();
    let source = source::StreamSource::new(from_iter(0..10));
}
