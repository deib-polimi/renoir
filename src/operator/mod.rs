use std::time::Duration;

use async_trait::async_trait;

pub use map::*;

mod map;
mod sink;
pub mod source;

pub enum StreamElement<Out> {
    Item(Out),
    Timestamped(Out, Duration),
    Watermark(Duration),
    End,
}

#[async_trait]
pub trait Operator<Out> {
    async fn next(&mut self) -> StreamElement<Out>;
}
