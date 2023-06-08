use std::fmt::Display;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure, Replication};
use crate::operator::source::Source;
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::Stream;

/// Source that consumes an iterator and emits all its elements into the stream.
///
/// The iterator will be consumed **only from one replica**, therefore this source is not parallel.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct IteratorSource<Out: Data, It>
where
    It: Iterator<Item = Out> + Send + 'static,
{
    #[derivative(Debug = "ignore")]
    inner: It,
    terminated: bool,
}

impl<Out: Data, It> Display for IteratorSource<Out, It>
where
    It: Iterator<Item = Out> + Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IteratorSource<{}>", std::any::type_name::<Out>())
    }
}

impl<Out: Data, It> IteratorSource<Out, It>
where
    It: Iterator<Item = Out> + Send + 'static,
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
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
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

impl<Out: Data, It> Source<Out> for IteratorSource<Out, It>
where
    It: Iterator<Item = Out> + Send + 'static,
{
    fn replication(&self) -> Replication {
        Replication::One
    }
}

impl<Out: Data, It> Operator<Out> for IteratorSource<Out, It>
where
    It: Iterator<Item = Out> + Send + 'static,
{
    fn setup(&mut self, _metadata: &mut ExecutionMetadata) {}

    fn next(&mut self) -> StreamElement<Out> {
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
        let mut operator = OperatorStructure::new::<Out, _>("IteratorSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl<Out: Data, It> Clone for IteratorSource<Out, It>
where
    It: Iterator<Item = Out> + Send + 'static,
{
    fn clone(&self) -> Self {
        // Since this is a non-parallel source, we don't want the other replicas to emit any value
        panic!("IteratorSource cannot be cloned, replication should be 1");
    }
}

impl crate::StreamEnvironment {
    /// Convenience method, creates a `IteratorSource` and makes a stream using `StreamEnvironment::stream`
    pub fn stream_iter<Out: Data, It: Iterator<Item = Out> + Send + 'static>(
        &mut self,
        iterator: It,
    ) -> Stream<Out, IteratorSource<Out, It>> {
        let source = IteratorSource::new(iterator);
        self.stream(source)
    }
}
