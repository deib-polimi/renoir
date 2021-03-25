use std::hash::Hash;
use std::time::Duration;

use serde::{Deserialize, Serialize};

pub use batch_mode::*;
pub use end::*;
pub use group_by::*;
pub use key_by::*;
pub use map::*;
pub use shuffle::*;
pub use start::*;
pub use unkey::*;

use crate::scheduler::ExecutionMetadata;

mod batch_mode;
mod end;
mod flatten;
mod fold;
mod group_by;
mod key_by;
mod keyed_fold;
mod keyed_reduce;
mod map;
mod reduce;
mod shuffle;
pub mod sink;
pub mod source;
mod split;
mod start;
mod unkey;

/// Marker trait that all the types inside a stream should implement.
pub trait Data: Clone + Send + Sync + Serialize + for<'a> Deserialize<'a> + 'static {}
impl<T: Clone + Send + Sync + Serialize + for<'a> Deserialize<'a> + 'static> Data for T {}

/// Maker trait that all the keys should implement.
pub trait DataKey: Data + Hash + Eq {}
impl<T: Data + Hash + Eq> DataKey for T {}

/// When using timestamps and watermarks, this type expresses the timestamp of a message or of a
/// watermark.
pub type Timestamp = Duration;

/// An element of the stream. This is what enters and exits from the operators.
///
/// An operator may need to change the content of a `StreamElement` (e.g. a `Map` may change the
/// value of the `Item`). Usually `Watermark` and `End` are simply forwarded to the next operator in
/// the chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamElement<Out> {
    /// A normal element containing just the value of the message.
    Item(Out),
    /// Like `Item`, but it's attached with a timestamp, it's used to ensure the ordering of the
    /// messages.
    Timestamped(Out, Timestamp),
    /// When an operator receives a `Watermark` with timestamp `t`, the operator will never see any
    /// message with timestamp less or equal to `t`.
    Watermark(Timestamp),
    /// Flush the internal batch since there will be too much delay till the next message to come.
    FlushBatch,
    /// The last message an operator will receive, indicating that the stream has ended.
    End,
}

/// An operator represents a unit of computation. It's always included inside a chain of operators,
/// inside a block.
///
/// Each operator implements the `Operator<Out>` trait, it produced a stream of `Out` elements.
///
/// An `Operator` must be Clone since it is part of a single chain when it's built, but it has to
/// be cloned to spawn the replicas of the block.
///
/// This trait has some `async` function, due to a limitation of rust `async_trait` must be used.

pub trait Operator<Out: Data>: Clone {
    /// Setup the operator chain. This is called before any call to `next` and it's used to
    /// initialize the operator. When it's called the operator has already been cloned and it will
    /// never be cloned again. Therefore it's safe to store replica-specific metadata inside of it.
    ///
    /// It's important that each operator (except the start of a chain) calls `.setup()` recursively
    /// on the previous operators.
    fn setup(&mut self, metadata: ExecutionMetadata);

    /// Take a value from the previous operator, process it and return it.
    fn next(&mut self) -> StreamElement<Out>;

    /// A string representation of the operator and its predecessors.
    fn to_string(&self) -> String;
}

impl<Out: Data> StreamElement<Out> {
    /// Create a new `StreamElement` with an `Item(())` if `self` contains an item, otherwise it
    /// returns the same variant of `self`.
    pub(crate) fn take(&self) -> StreamElement<()> {
        match self {
            StreamElement::Item(_) => StreamElement::Item(()),
            StreamElement::Timestamped(_, _) => StreamElement::Item(()),
            StreamElement::Watermark(w) => StreamElement::Watermark(*w),
            StreamElement::End => StreamElement::End,
            StreamElement::FlushBatch => StreamElement::FlushBatch,
        }
    }

    /// Change the type of the element inside the `StreamElement`.
    pub(crate) fn map<NewOut: Data>(self, f: impl FnOnce(Out) -> NewOut) -> StreamElement<NewOut> {
        match self {
            StreamElement::Item(item) => StreamElement::Item(f(item)),
            StreamElement::Timestamped(item, ts) => StreamElement::Timestamped(f(item), ts),
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::End => StreamElement::End,
            StreamElement::FlushBatch => StreamElement::FlushBatch,
        }
    }
}
