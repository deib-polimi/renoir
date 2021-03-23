use core::iter::{IntoIterator, Iterator};
use std::collections::VecDeque;
use std::iter::repeat;

use std::sync::Arc;

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

impl<Out: Data, IterOut, NewOut: Data, PreviousOperators> Operator<NewOut>
    for Flatten<Out, IterOut, NewOut, PreviousOperators>
where
    IterOut: Iterator<Item = NewOut> + Clone + Send + 'static,
    PreviousOperators: Operator<Out> + Send,
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
                StreamElement::End => return StreamElement::End,
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
}

impl<In: Data, Out: Data, OperatorChain, NewOut: Data> Stream<In, Out, OperatorChain>
where
    Out: IntoIterator<Item = NewOut>,
    <Out as IntoIterator>::IntoIter: Clone + Send + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn flatten(self) -> Stream<In, NewOut, impl Operator<NewOut>> {
        self.add_operator(|prev| Flatten {
            prev,
            buffer: Default::default(),
            // just convert `Out` to an `Iterator<Item = NewOut>`
            make_iter: Arc::new(|x| x.into_iter()),
        })
    }
}

impl<In: Data, Out: Data, OperatorChain> Stream<In, Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn flat_map<MapOut: Data, NewOut: Data, F>(
        self,
        f: F,
    ) -> Stream<In, NewOut, impl Operator<NewOut>>
    where
        MapOut: IntoIterator<Item = NewOut>,
        <MapOut as IntoIterator>::IntoIter: Clone + Send + 'static,
        F: Fn(Out) -> MapOut + Send + Sync + 'static,
    {
        self.map(f).flatten()
    }
}

impl<In: Data, Key: DataKey, Out: Data, NewOut: Data, OperatorChain>
    KeyedStream<In, Key, Out, OperatorChain>
where
    Out: IntoIterator<Item = NewOut>,
    <Out as IntoIterator>::IntoIter: Clone + Send + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn flatten(self) -> KeyedStream<In, Key, NewOut, impl Operator<KeyValue<Key, NewOut>>> {
        self.add_operator(|prev| Flatten {
            prev,
            buffer: Default::default(),
            // convert a `KeyValue<Key, Out>` to an `Interator<Item = KeyValue<Key, NewOut>>`
            // repeat is used to have the same key for every value of type NewOut
            make_iter: Arc::new(move |(k, x)| repeat(k).zip(x.into_iter())),
        })
    }
}

impl<In: Data, Key: DataKey, Out: Data, OperatorChain> KeyedStream<In, Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn flat_map<NewOut: Data, MapOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<In, Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
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
    use itertools::Itertools;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[test]
    fn flatten_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(
            vec![
                vec![],
                vec![1u8, 2, 3],
                vec![4, 5],
                vec![],
                vec![6, 7, 8],
                vec![],
            ]
            .into_iter(),
        );
        let res = env.stream(source).flatten().collect_vec();
        env.execute();
        assert_eq!(res.get().unwrap(), (1..=8).collect_vec());
    }

    #[test]
    fn flatten_keyed_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|v| v % 2)
            .map(|(_k, v)| vec![v, v, v])
            .flatten()
            .unkey()
            .collect_vec();
        env.execute();
        let expected = (0..10u8)
            .flat_map(|x| vec![(x % 2, x), (x % 2, x), (x % 2, x)])
            .sorted()
            .collect_vec();
        let res = res.get().unwrap().into_iter().sorted().collect_vec();
        assert_eq!(expected, res);
    }

    #[test]
    fn flat_map_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(0..10u8);
        let res = env
            .stream(source)
            .flat_map(|x| vec![x, 10 * x, 20 * x])
            .collect_vec();
        env.execute();
        let expected = (0..10u8)
            .flat_map(|x| vec![x, 10 * x, 20 * x])
            .collect_vec();
        assert_eq!(res.get().unwrap(), expected);
    }

    #[test]
    fn flat_map_keyed_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|v| v % 2)
            .flat_map(|(_k, v)| vec![v, v, v])
            .unkey()
            .collect_vec();
        env.execute();
        let expected = (0..10u8)
            .flat_map(|x| vec![(x % 2, x), (x % 2, x), (x % 2, x)])
            .sorted()
            .collect_vec();
        let res = res.get().unwrap().into_iter().sorted().collect_vec();
        assert_eq!(expected, res);
    }
}
