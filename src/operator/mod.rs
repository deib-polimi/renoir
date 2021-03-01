use std::time::Duration;

use async_trait::async_trait;

pub use key_by::*;
pub use map::*;
pub use shuffle::*;
pub use unkey::*;

use crate::scheduler::ExecutionMetadata;

mod key_by;
mod map;
mod shuffle;
pub mod sink;
pub mod source;
mod unkey;

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
    async fn setup(&mut self, metadata: ExecutionMetadata);

    async fn next(&mut self) -> StreamElement<Out>;

    fn to_string(&self) -> String;
}

impl<Out> StreamElement<Out>
where
    Out: Clone + Send + 'static,
{
    pub fn map<NewOut>(self, f: impl FnOnce(Out) -> NewOut) -> StreamElement<NewOut>
    where
        NewOut: Clone + Send + 'static,
    {
        match self {
            StreamElement::Item(item) => StreamElement::Item(f(item)),
            StreamElement::Timestamped(item, ts) => StreamElement::Timestamped(f(item), ts),
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::End => StreamElement::End,
        }
    }
}
