use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::operator::source::Source;
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

/// Source that consumes an iterator and emits all its elements into the stream.
///
/// The iterator is cloned in each replica (i.e. once for each core), and then it will be consumed
/// fully.
#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub struct ParallelIteratorSource<Out: Data, It>
where
    It: Iterator<Item = Out> + Send + Clone + 'static,
{
    #[derivative(Debug = "ignore")]
    inner: It,
    terminated: bool,
}

impl<Out: Data, It> ParallelIteratorSource<Out, It>
where
    It: Iterator<Item = Out> + Send + Clone + 'static,
{
    /// Create a new source that reads the items from the iterator provided as input.
    ///
    /// The iterator is cloned in each replica (i.e. once for each core), and then it will be consumed
    /// fully.
    ///
    /// ## Example
    ///
    /// ```
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::ParallelIteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// // the elements (0..5) repeated for each core
    /// let source = ParallelIteratorSource::new((0..5));
    /// let s = env.stream(source);
    /// ```
    pub fn new(inner: It) -> Self {
        Self {
            inner,
            terminated: false,
        }
    }
}

impl<Out: Data, It> Source<Out> for ParallelIteratorSource<Out, It>
where
    It: Iterator<Item = Out> + Send + Clone + 'static,
{
    fn get_max_parallelism(&self) -> Option<usize> {
        None
    }
}

impl<Out: Data, It> Operator<Out> for ParallelIteratorSource<Out, It>
where
    It: Iterator<Item = Out> + Send + Clone + 'static,
{
    fn setup(&mut self, _metadata: ExecutionMetadata) {}

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

    fn to_string(&self) -> String {
        format!("StreamSource<{}>", std::any::type_name::<Out>())
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("ParallelIteratorSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}
