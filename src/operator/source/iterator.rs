use std::fmt::Display;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure, Replication};
use crate::operator::source::Source;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::Stream;

/// Source that consumes an iterator and emits all its elements into the stream.
///
/// The iterator will be consumed **only from one replica**, therefore this source is not parallel.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct IteratorSource<It>
where
    It: Iterator + Send + 'static,
    It::Item: Send,
{
    #[derivative(Debug = "ignore")]
    inner: It,
    terminated: bool,
}

impl<It> Display for IteratorSource<It>
where
    It: Iterator + Send + 'static,
    It::Item: Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IteratorSource<{}>", std::any::type_name::<It::Item>())
    }
}

impl<It> IteratorSource<It>
where
    It: Iterator + Send + 'static,
    It::Item: Send,
{
    /// Create a new source that reads the items from the iterator provided as input.
    ///
    /// **Note**: this source is **not parallel**, the iterator will be consumed only on a single
    /// replica, on all the others no item will be read from the iterator. If you want to achieve
    /// parallelism you need to add an operator that shuffles the data (e.g.
    /// [`Stream::shuffle`](crate::Stream::shuffle)).
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamContext, RuntimeConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamContext::new(RuntimeConfig::local(1));
    /// let source = IteratorSource::new((0..5));
    /// let s = env.stream(source);
    /// ```
    pub fn new(inner: It) -> Self {
        Self {
            inner,
            terminated: false,
        }
    }
}

impl<It> Source for IteratorSource<It>
where
    It: Iterator + Send + 'static,
    It::Item: Send,
{
    fn replication(&self) -> Replication {
        Replication::One
    }
}

impl<It> Operator for IteratorSource<It>
where
    It: Iterator + Send + 'static,
    It::Item: Send,
{
    type Out = It::Item;

    fn setup(&mut self, _metadata: &mut ExecutionMetadata) {}

    fn next(&mut self) -> StreamElement<Self::Out> {
        if self.terminated {
            return StreamElement::Terminate;
        }
        // TODO: with adaptive batching this does not work since it never emits FlushBatch messages
        match self.inner.next() {
            Some(t) => StreamElement::Item(t),
            None => {
                self.terminated = true;
                StreamElement::FlushAndRestart
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Self::Out, _>("IteratorSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl<It> Clone for IteratorSource<It>
where
    It: Iterator + Send + 'static,
    It::Item: Send,
{
    fn clone(&self) -> Self {
        // Since this is a non-parallel source, we don't want the other replicas to emit any value
        panic!("IteratorSource cannot be cloned, replication should be 1");
    }
}

impl crate::StreamContext {
    /// Convenience method, creates a `IteratorSource` and makes a stream using `StreamContext::stream`
    pub fn stream_iter<It>(&self, iterator: It) -> Stream<IteratorSource<It>>
    where
        It: Iterator + Send + 'static,
        It::Item: Send,
    {
        let source = IteratorSource::new(iterator);
        self.stream(source)
    }
}
