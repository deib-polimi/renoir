use std::fmt::Display;
use std::ops::Range;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure, Replication};
use crate::operator::source::Source;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::{CoordUInt, Stream};

pub trait IntoParallelSource: Clone + Send {
    type Iter: Iterator;
    fn generate_iterator(self, index: CoordUInt, peers: CoordUInt) -> Self::Iter;
}

impl<It, G> IntoParallelSource for G
where
    It: Iterator + Send + 'static,
    G: FnOnce(CoordUInt, CoordUInt) -> It + Send + Clone,
{
    type Iter = It;

    fn generate_iterator(self, index: CoordUInt, peers: CoordUInt) -> Self::Iter {
        self(index, peers)
    }
}

impl IntoParallelSource for Range<u64> {
    type Iter = Range<u64>;

    fn generate_iterator(self, index: CoordUInt, peers: CoordUInt) -> Self::Iter {
        let n = self.end - self.start;
        let chunk_size = (n.saturating_add(peers - 1)) / peers;
        let start = self.start.saturating_add(index * chunk_size);
        let end = (start.saturating_add(chunk_size))
            .min(self.end)
            .max(self.start);

        start..end
    }
}

macro_rules! impl_into_parallel_source_range {
    ($t:ty) => {
        impl IntoParallelSource for Range<$t> {
            type Iter = Range<$t>;

            fn generate_iterator(self, index: CoordUInt, peers: CoordUInt) -> Self::Iter {
                let index: i64 = index.try_into().unwrap();
                let peers: i64 = peers.try_into().unwrap();
                let n = self.end as i64 - self.start as i64;
                let chunk_size = (n.saturating_add(peers - 1)) / peers;
                let start = (self.start as i64).saturating_add(index * chunk_size);
                let end = (start.saturating_add(chunk_size))
                    .min(self.end as i64)
                    .max(self.start as i64);

                let (start, end) = (start.try_into().unwrap(), end.try_into().unwrap());
                start..end
            }
        }
    };
}

impl_into_parallel_source_range!(u8);
impl_into_parallel_source_range!(u16);
impl_into_parallel_source_range!(u32);

impl_into_parallel_source_range!(usize);

impl_into_parallel_source_range!(i8);
impl_into_parallel_source_range!(i16);
impl_into_parallel_source_range!(i32);
impl_into_parallel_source_range!(i64);
impl_into_parallel_source_range!(isize);

/// This enum wraps either an `Iterator` that yields the items, or a generator function that
/// produces such iterator.
///
/// This enum is `Clone` only _before_ generating the iterator. The generator function must be
/// `Clone`, but the resulting iterator doesn't have to be so.
enum IteratorGenerator<Source: IntoParallelSource> {
    /// The function that generates the iterator.
    Generator(Source),
    /// The actual iterator that produces the items.
    Iterator(Source::Iter),
    /// An extra variant used when moving the generator out of the enum, and before putting back the
    /// iterator. This makes this enum panic-safe in the `generate` method.
    Generating,
}

impl<Source: IntoParallelSource> IteratorGenerator<Source> {
    /// Consume the generator function and store the produced iterator.
    ///
    /// This method can be called only once.
    fn generate(&mut self, global_id: CoordUInt, instances: CoordUInt) {
        let gen = std::mem::replace(self, IteratorGenerator::Generating);
        let iter = match gen {
            IteratorGenerator::Generator(gen) => gen.generate_iterator(global_id, instances),
            _ => unreachable!("generate on non-Generator variant"),
        };
        *self = IteratorGenerator::Iterator(iter);
    }

    /// If the `generate` method has been called, get the next element from the iterator.
    fn next(&mut self) -> Option<<Source::Iter as Iterator>::Item> {
        match self {
            IteratorGenerator::Iterator(iter) => iter.next(),
            _ => unreachable!("next on non-Iterator variant"),
        }
    }
}

impl<Source: IntoParallelSource> Clone for IteratorGenerator<Source> {
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
pub struct ParallelIteratorSource<Source>
where
    Source: IntoParallelSource,
{
    #[derivative(Debug = "ignore")]
    inner: IteratorGenerator<Source>,
    terminated: bool,
}

impl<Source> Display for ParallelIteratorSource<Source>
where
    Source: IntoParallelSource,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ParallelIteratorSource<{}>",
            std::any::type_name::<<Source::Iter as Iterator>::Item>()
        )
    }
}

impl<S> Operator for ParallelIteratorSource<S>
where
    S: IntoParallelSource,
    S::Iter: Send,
    <S::Iter as Iterator>::Item: Send,
{
    type Out = <S::Iter as Iterator>::Item;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.inner.generate(
            metadata.global_id,
            metadata
                .replicas
                .len()
                .try_into()
                .expect("Num replicas > max id"),
        );
    }

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
        let mut operator =
            OperatorStructure::new::<<S::Iter as Iterator>::Item, _>("ParallelIteratorSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl<S> Clone for ParallelIteratorSource<S>
where
    S: IntoParallelSource,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            terminated: false,
        }
    }
}

impl crate::StreamContext {
    /// Convenience method, creates a `ParallelIteratorSource` and makes a stream using `StreamContext::stream`
    /// # Example:
    /// ```
    /// use noir_compute::prelude::*;
    ///
    /// let env = StreamContext::new_local();
    ///
    /// env.stream_par_iter(0..10)
    ///     .for_each(|q| println!("a: {q}"));
    ///
    /// let n = 10;
    /// env.stream_par_iter(
    ///     move |id, instances| {
    ///         let chunk_size = (n + instances - 1) / instances;
    ///         let remaining = n - n.min(chunk_size * id);
    ///         let range = remaining.min(chunk_size);
    ///         
    ///         let start = id * chunk_size;
    ///         let stop = id * chunk_size + range;
    ///         start..stop
    ///     })
    ///    .for_each(|q| println!("b: {q}"));
    ///
    /// env.execute_blocking();
    /// ```
    pub fn stream_par_iter<Source>(
        &self,
        generator: Source,
    ) -> Stream<ParallelIteratorSource<Source>>
    where
        Source: IntoParallelSource + 'static,
        Source::Iter: Send,
        <Source::Iter as Iterator>::Item: Send,
    {
        let source = ParallelIteratorSource::new(generator);
        self.stream(source)
    }
}

impl<S> ParallelIteratorSource<S>
where
    S: IntoParallelSource,
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
    /// # use noir_compute::{StreamContext, RuntimeConfig};
    /// # use noir_compute::operator::source::ParallelIteratorSource;
    /// # let mut env = StreamContext::new_local();
    /// // generate the numbers from 0 to 99 using multiple replicas
    /// let n = 100;
    /// let source = ParallelIteratorSource::new(move |id, instances| {
    ///     let chunk_size = (n + instances - 1) / instances;
    ///     let remaining = n - n.min(chunk_size * id);
    ///     let range = remaining.min(chunk_size);
    ///     
    ///     let start = id * chunk_size;
    ///     let stop = id * chunk_size + range;
    ///     start..stop
    /// });
    /// let s = env.stream(source);
    /// ```
    pub fn new(generator: S) -> Self {
        Self {
            inner: IteratorGenerator::Generator(generator),
            terminated: false,
        }
    }
}

impl<S> Source for ParallelIteratorSource<S>
where
    S: IntoParallelSource,
    S::Iter: Send,
    <S::Iter as Iterator>::Item: Send,
{
    fn replication(&self) -> Replication {
        Replication::Unlimited
    }
}
