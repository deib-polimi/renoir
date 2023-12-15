use std::fmt::Display;

use futures::{Stream, StreamExt};

use crate::block::{BlockStructure, OperatorKind, OperatorStructure, Replication};
use crate::operator::source::Source;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

/// Source that consumes an iterator and emits all its elements into the stream.
///
/// The iterator will be consumed **only from one replica**, therefore this source is not parallel.

#[derive(Derivative)]
#[derivative(Debug)]
pub struct AsyncStreamSource<S>
where
    S: Stream + Send + Unpin + 'static,
    S::Item: Send,
{
    #[derivative(Debug = "ignore")]
    inner: S,
    terminated: bool,
}

impl<S> Display for AsyncStreamSource<S>
where
    S: Stream + Send + Unpin + 'static,
    S::Item: Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamSource<{}>", std::any::type_name::<S::Item>())
    }
}

impl<S> AsyncStreamSource<S>
where
    S: Stream + Send + Unpin + 'static,
    S::Item: Send,
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
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::AsyncStreamSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let stream = futures::stream::iter(0..10u32);
    /// let source = AsyncStreamSource::new(stream);
    /// let s = env.stream(source);
    /// ```
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            terminated: false,
        }
    }
}

impl<S> Source for AsyncStreamSource<S>
where
    S: Stream + Send + Unpin + 'static,
    S::Item: Send,
{
    fn replication(&self) -> Replication {
        Replication::One
    }
}

impl<S> Operator for AsyncStreamSource<S>
where
    S: Stream + Send + Unpin + 'static,
    S::Item: Send,
{
    type Out = <S as Stream>::Item;

    fn setup(&mut self, _metadata: &mut ExecutionMetadata) {}

    fn next(&mut self) -> StreamElement<Self::Out> {
        if self.terminated {
            return StreamElement::Terminate;
        }
        // TODO: with adaptive batching this does not work since S never emits FlushBatch messages
        let rt = tokio::runtime::Handle::current();
        match rt.block_on(self.inner.next()) {
            Some(t) => StreamElement::Item(t),
            None => {
                self.terminated = true;
                StreamElement::FlushAndRestart
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Self::Out, _>("AsyncStreamSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl<S> Clone for AsyncStreamSource<S>
where
    S: Stream + Send + Unpin + 'static,
    S::Item: Send,
{
    fn clone(&self) -> Self {
        // Since this is a non-parallel source, we don't want the other replicas to emit any value
        panic!("AsyncStreamSource cannot be cloned, replication should be 1");
    }
}
