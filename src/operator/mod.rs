use std::time::Duration;

use async_trait::async_trait;

pub use end::*;
pub use group_by::*;
pub use key_by::*;
pub use map::*;
pub use shuffle::*;
pub use start::*;
pub use unkey::*;

use crate::scheduler::ExecutionMetadata;

mod end;
mod group_by;
mod key_by;
mod map;
mod shuffle;
pub mod sink;
pub mod source;
mod start;
mod unkey;

pub type Timestamp = Duration;

#[derive(Debug, Clone)]
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
    pub fn take(&self) -> StreamElement<()> {
        match self {
            StreamElement::Item(_) => StreamElement::Item(()),
            StreamElement::Timestamped(_, _) => StreamElement::Item(()),
            StreamElement::Watermark(w) => StreamElement::Watermark(*w),
            StreamElement::End => StreamElement::End,
        }
    }

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
