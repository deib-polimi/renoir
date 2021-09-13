use core::iter::{IntoIterator, Iterator};
use std::collections::VecDeque;
use std::iter::repeat;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, DataKey, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Flatten<Out: Data, IterOut, MakeIter, NewOut: Data, PreviousOperators>
where
    IterOut: Iterator<Item = NewOut> + Clone + Send + 'static,
    MakeIter: Fn(Out) -> IterOut + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    // used to store elements that have not been returned by next() yet
    buffer: VecDeque<StreamElement<NewOut>>,
    // Make an element of type `Out` iterable
    // This is used to make `Flatten` behave differently when applied to `Stream` or `KeyedStream`
    // Takes `Out` as input, returns an `Iterator` with items of type `NewOut`
    #[derivative(Debug = "ignore")]
    make_iter: MakeIter,
    _out: PhantomData<Out>,
    _iter_out: PhantomData<Out>,
}

impl<Out: Data, IterOut, MakeIter, NewOut: Data, PreviousOperators>
    Flatten<Out, IterOut, MakeIter, NewOut, PreviousOperators>
where
    IterOut: Iterator<Item = NewOut> + Clone + Send + 'static,
    MakeIter: Fn(Out) -> IterOut + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn new(prev: PreviousOperators, make_iter: MakeIter) -> Self {
        Self {
            prev,
            buffer: Default::default(),
            make_iter,
            _out: Default::default(),
            _iter_out: Default::default(),
        }
    }
}

impl<Out: Data, IterOut, MakeIter, NewOut: Data, PreviousOperators> Operator<NewOut>
    for Flatten<Out, IterOut, MakeIter, NewOut, PreviousOperators>
where
    IterOut: Iterator<Item = NewOut> + Clone + Send + 'static,
    MakeIter: Fn(Out) -> IterOut + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<NewOut> {
        while self.buffer.is_empty() {
            match self.prev.next() {
                StreamElement::Item(item) => {
                    self.buffer
                        .extend((self.make_iter)(item).map(StreamElement::Item));
                }
                StreamElement::Timestamped(item, ts) => {
                    self.buffer.extend(
                        (self.make_iter)(item).map(|value| StreamElement::Timestamped(value, ts)),
                    );
                }
                StreamElement::Watermark(ts) => return StreamElement::Watermark(ts),
                StreamElement::Terminate => return StreamElement::Terminate,
                StreamElement::FlushAndRestart => return StreamElement::FlushAndRestart,
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
            }
        }

        self.buffer.pop_front().unwrap()
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> Flatten<{} -> {}>",
            self.prev.to_string(),
            std::any::type_name::<Out>(),
            std::any::type_name::<NewOut>()
        )
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<NewOut, _>("Flatten"))
    }
}

impl<Out: Data, OperatorChain, NewOut: Data> Stream<Out, OperatorChain>
where
    Out: IntoIterator<Item = NewOut>,
    <Out as IntoIterator>::IntoIter: Clone + Send + 'static,
    OperatorChain: Operator<Out> + 'static,
{
    /// Transform this stream of containers into a stream of all the contained values.
    ///
    /// **Note**: this is very similar to [`Iteartor::flatten`](std::iter::Iterator::flatten)
    ///
    /// ## Example
    ///
    /// ```
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new(vec![
    ///     vec![1, 2, 3],
    ///     vec![],
    ///     vec![4, 5],
    /// ].into_iter()));
    /// let res = s.flatten().collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![1, 2, 3, 4, 5]);
    /// ```
    pub fn flatten(self) -> Stream<NewOut, impl Operator<NewOut>> {
        self.add_operator(|prev| Flatten::new(prev, |x| x.into_iter()))
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Apply a mapping operation to each element of the stream, the resulting stream will be the
    /// flattened values of the result of the mapping.
    ///
    /// **Note**: this is very similar to [`Iteartor::flat_map`](std::iter::Iterator::flat_map)
    ///
    /// ## Example
    ///
    /// ```
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..3)));
    /// let res = s.flat_map(|n| vec![n, n]).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0, 0, 1, 1, 2, 2]);
    /// ```
    pub fn flat_map<MapOut: Data, NewOut: Data, F>(
        self,
        f: F,
    ) -> Stream<NewOut, impl Operator<NewOut>>
    where
        MapOut: IntoIterator<Item = NewOut>,
        <MapOut as IntoIterator>::IntoIter: Clone + Send + 'static,
        F: Fn(Out) -> MapOut + Send + Clone + 'static,
    {
        self.map(f).flatten()
    }
}

impl<Key: DataKey, Out: Data, NewOut: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    Out: IntoIterator<Item = NewOut>,
    <Out as IntoIterator>::IntoIter: Clone + Send + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Transform this stream of containers into a stream of all the contained values.
    ///
    /// **Note**: this is very similar to [`Iteartor::flatten`](std::iter::Iterator::flatten)
    ///
    /// ## Example
    ///
    /// ```
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env
    ///     .stream(IteratorSource::new(vec![
    ///         vec![0, 1, 2],
    ///         vec![3, 4, 5],
    ///         vec![6, 7]
    ///     ].into_iter()))
    ///     .group_by(|v| v[0] % 2);
    /// let res = s.flatten().collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (0, 1), (0, 2), (0, 6), (0, 7), (1, 3), (1, 4), (1, 5)]);
    /// ```
    pub fn flatten(self) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>> {
        self.add_operator(|prev| Flatten::new(prev, |(k, x)| repeat(k).zip(x.into_iter())))
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Apply a mapping operation to each element of the stream, the resulting stream will be the
    /// flattened values of the result of the mapping.
    ///
    /// **Note**: this is very similar to [`Iteartor::flat_map`](std::iter::Iterator::flat_map).
    ///
    /// ## Example
    ///
    /// ```
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..3))).group_by(|&n| n % 2);
    /// let res = s.flat_map(|(_key, n)| vec![n, n]).collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (0, 0), (0, 2), (0, 2), (1, 1), (1, 1)]);
    /// ```
    pub fn flat_map<NewOut: Data, MapOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        MapOut: IntoIterator<Item = NewOut>,
        <MapOut as IntoIterator>::IntoIter: Clone + Send + 'static,
        F: Fn(KeyValue<&Key, Out>) -> MapOut + Send + Clone + 'static,
    {
        self.map(f).flatten()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::operator::flatten::Flatten;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn test_flatten_no_timestamps() {
        let fake_operator = FakeOperator::new(
            vec![
                vec![],
                vec![0, 1, 2, 3],
                vec![],
                vec![4],
                vec![5, 6, 7],
                vec![],
            ]
            .into_iter(),
        );
        let mut flatten = Flatten::new(fake_operator, |x| x.into_iter());
        for i in 0..=7 {
            assert_eq!(flatten.next(), StreamElement::Item(i));
        }
        assert_eq!(flatten.next(), StreamElement::Terminate);
    }

    #[test]
    fn test_flatten_timestamped() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Timestamped(vec![], Duration::from_secs(0)));
        fake_operator.push(StreamElement::Timestamped(
            vec![1, 2, 3],
            Duration::from_secs(1),
        ));
        fake_operator.push(StreamElement::Timestamped(vec![4], Duration::from_secs(2)));
        fake_operator.push(StreamElement::Timestamped(vec![], Duration::from_secs(3)));
        fake_operator.push(StreamElement::Watermark(Duration::from_secs(4)));

        let mut flatten = Flatten::new(fake_operator, |x| x.into_iter());

        assert_eq!(
            flatten.next(),
            StreamElement::Timestamped(1, Duration::from_secs(1))
        );
        assert_eq!(
            flatten.next(),
            StreamElement::Timestamped(2, Duration::from_secs(1))
        );
        assert_eq!(
            flatten.next(),
            StreamElement::Timestamped(3, Duration::from_secs(1))
        );
        assert_eq!(
            flatten.next(),
            StreamElement::Timestamped(4, Duration::from_secs(2))
        );
        assert_eq!(
            flatten.next(),
            StreamElement::Watermark(Duration::from_secs(4))
        );
        assert_eq!(flatten.next(), StreamElement::Terminate);
    }
}
