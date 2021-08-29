use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::operator::source::Source;
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

/// This enum wraps either an `Iterator` that yields the items, or a generator function that
/// produces such iterator.
///
/// This enum is `Clone` only _before_ generating the iterator. The generator function must be
/// `Clone`, but the resulting iterator doesn't have to be so.
enum IteratorGenerator<It, GenIt, Out>
where
    It: Iterator<Item = Out> + Send + 'static,
    GenIt: FnOnce(usize, usize) -> It + Send + Clone,
{
    /// The function that generates the iterator.
    Generator(GenIt),
    /// The actual iterator that produces the items.
    Iterator(It),
    /// An extra variant used when moving the generator out of the enum, and before putting back the
    /// iterator. This makes this enum panic-safe in the `generate` method.
    Generating,
}

impl<It, GenIt, Out> IteratorGenerator<It, GenIt, Out>
where
    It: Iterator<Item = Out> + Send + 'static,
    GenIt: FnOnce(usize, usize) -> It + Send + Clone,
{
    /// Consume the generator function and store the produced iterator.
    ///
    /// This method can be called only once.
    fn generate(&mut self, global_id: usize, num_replicas: usize) {
        let gen = std::mem::replace(self, IteratorGenerator::Generating);
        let iter = match gen {
            IteratorGenerator::Generator(gen) => gen(global_id, num_replicas),
            _ => unreachable!("generate on non-Generator variant"),
        };
        *self = IteratorGenerator::Iterator(iter);
    }

    /// If the `generate` method has been called, get the next element from the iterator.
    fn next(&mut self) -> Option<Out> {
        match self {
            IteratorGenerator::Iterator(iter) => iter.next(),
            _ => unreachable!("next on non-Iterator variant"),
        }
    }
}

impl<It, GenIt, Out> Clone for IteratorGenerator<It, GenIt, Out>
where
    It: Iterator<Item = Out> + Send + 'static,
    GenIt: FnOnce(usize, usize) -> It + Send + Clone,
{
    fn clone(&self) -> Self {
        match self {
            Self::Generator(gen) => Self::Generator(gen.clone()),
            _ => panic!("Can clone only before generating the iterator"),
        }
    }
}

/// Source that ingests items into a stream using the maximum parallelism. The items are from the
/// iterators returned by a generating function.
///
/// Each replica (i.e. each core) will have a different iterator. The iterator are produced by a
/// generating function passed to the [`ParallelIteratorSource::new`] method.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct ParallelIteratorSource<Out: Data, It, GenIt>
where
    It: Iterator<Item = Out> + Send + 'static,
    GenIt: FnOnce(usize, usize) -> It + Send + Clone,
{
    #[derivative(Debug = "ignore")]
    inner: IteratorGenerator<It, GenIt, Out>,
    terminated: bool,
}

impl<Out: Data, It, GenIt> ParallelIteratorSource<Out, It, GenIt>
where
    It: Iterator<Item = Out> + Send + 'static,
    GenIt: FnOnce(usize, usize) -> It + Send + Clone,
{
    /// Create a new source that ingest items into the stream using the maximum parallelism
    /// available.
    ///
    /// The function passed as argument is cloned in each core, and called to get the iterator for
    /// that replica. The first parameter passed to the function is a 0-based index of the replica,
    /// while the second is the total number of replicas.
    ///
    /// ## Example
    ///
    /// ```
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::ParallelIteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// // generate the numbers from 0 to 99 using multiple replicas
    /// let n = 100;
    /// let source = ParallelIteratorSource::new(move |id, num_replicas| {
    ///     let chunk_size = (n + num_replicas - 1) / num_replicas;
    ///     let remaining = n - n.min(chunk_size * id);
    ///     let range = remaining.min(chunk_size);
    ///     
    ///     let start = id * chunk_size;
    ///     let stop = id * chunk_size + range;
    ///     start..stop
    /// });
    /// let s = env.stream(source);
    /// ```
    pub fn new(generator: GenIt) -> Self {
        Self {
            inner: IteratorGenerator::Generator(generator),
            terminated: false,
        }
    }
}

impl<Out: Data, It, GenIt> Source<Out> for ParallelIteratorSource<Out, It, GenIt>
where
    It: Iterator<Item = Out> + Send + 'static,
    GenIt: FnOnce(usize, usize) -> It + Send + Clone,
{
    fn get_max_parallelism(&self) -> Option<usize> {
        None
    }
}

impl<Out: Data, It, GenIt> Operator<Out> for ParallelIteratorSource<Out, It, GenIt>
where
    It: Iterator<Item = Out> + Send + 'static,
    GenIt: FnOnce(usize, usize) -> It + Send + Clone,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.inner
            .generate(metadata.global_id, metadata.replicas.len());
    }

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

impl<Out: Data, It, GenIt> Clone for ParallelIteratorSource<Out, It, GenIt>
where
    It: Iterator<Item = Out> + Send + 'static,
    GenIt: FnOnce(usize, usize) -> It + Send + Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            terminated: false,
        }
    }
}
