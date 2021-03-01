use std::time::Duration;

use async_trait::async_trait;

use crate::block::ExecutionMetadataRef;
pub use map::*;
pub use shuffle::*;

mod map;
mod shuffle;
pub mod sink;
pub mod source;

pub type Timestamp = Duration;

#[derive(Clone)]
pub enum StreamElement<Out>
where
    Out: Clone + Send + 'static,
{
    Item(Out),
    Timestamped(Out, Timestamp),
    Watermark(Timestamp),
    End,
}

#[async_trait]
pub trait Operator<Out>: Clone
where
    Out: Clone + Send + 'static,
{
    fn block_init(&mut self, metadata: ExecutionMetadataRef);

    async fn start(&mut self);

    async fn next(&mut self) -> StreamElement<Out>;

    fn to_string(&self) -> String;
}
