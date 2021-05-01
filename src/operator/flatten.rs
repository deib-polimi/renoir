use core::iter::{IntoIterator, Iterator};
use std::collections::VecDeque;
use std::iter::repeat;

use std::sync::Arc;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, DataKey, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Flatten<Out: Data, IterOut, NewOut: Data, PreviousOperators>
where
    IterOut: Iterator<Item = NewOut> + Clone + Send + 'static,
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    // used to store elements that have not been returned by next() yet
    buffer: VecDeque<StreamElement<NewOut>>,
    // Make an element of type `Out` iterable
    // This is used to make `Flatten` behave differently when applied to `Stream` or `KeyedStream`
    // Takes `Out` as input, returns an `Iterator` with items of type `NewOut`
    #[derivative(Debug = "ignore")]
    make_iter: Arc<dyn Fn(Out) -> IterOut + Send + Sync>,
}

impl<Out: Data, IterOut, NewOut: Data, PreviousOperators>
    Flatten<Out, IterOut, NewOut, PreviousOperators>
where
    IterOut: Iterator<Item = NewOut> + Clone + Send + 'static,
    PreviousOperators: Operator<Out>,
{
    fn new<F>(prev: PreviousOperators, make_iter: F) -> Self
    where
        F: Fn(Out) -> IterOut + Send + Sync + 'static,
    {
        Self {
            prev,
            buffer: Default::default(),
            make_iter: Arc::new(make_iter),
        }
    }
}

impl<Out: Data, IterOut, NewOut: Data, PreviousOperators> Operator<NewOut>
    for Flatten<Out, IterOut, NewOut, PreviousOperators>
where
    IterOut: Iterator<Item = NewOut> + Clone + Send + 'static,
    PreviousOperators: Operator<Out>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<NewOut> {
        while self.buffer.is_empty() {
            match self.prev.next() {
                StreamElement::Item(item) => {
                    self.buffer = (self.make_iter)(item).map(StreamElement::Item).collect()
                }
                StreamElement::Timestamped(item, ts) => {
                    self.buffer = (self.make_iter)(item)
                        .map(|value| StreamElement::Timestamped(value, ts))
                        .collect()
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
    pub fn flatten(self) -> Stream<NewOut, impl Operator<NewOut>> {
        self.add_operator(|prev| Flatten::new(prev, |x| x.into_iter()))
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    // FIXME: MapOut does not really need to be Data, for example the normal iterators are not
    //        serializable
    pub fn flat_map<MapOut: Data, NewOut: Data, F>(
        self,
        f: F,
    ) -> Stream<NewOut, impl Operator<NewOut>>
    where
        MapOut: IntoIterator<Item = NewOut>,
        <MapOut as IntoIterator>::IntoIter: Clone + Send + 'static,
        F: Fn(Out) -> MapOut + Send + Sync + 'static,
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
    pub fn flatten(self) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>> {
        self.add_operator(|prev| Flatten::new(prev, |(k, x)| repeat(k).zip(x.into_iter())))
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    pub fn flat_map<NewOut: Data, MapOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        MapOut: IntoIterator<Item = NewOut>,
        <MapOut as IntoIterator>::IntoIter: Clone + Send + 'static,
        F: Fn(KeyValue<&Key, Out>) -> MapOut + Send + Sync + 'static,
    {
        self.map(f).flatten()
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::flatten::Flatten;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;
    use std::time::Duration;

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
