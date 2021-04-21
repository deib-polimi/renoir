use std::sync::Arc;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, DataKey, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Map<Out: Data, NewOut: Data, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    #[derivative(Debug = "ignore")]
    f: Arc<dyn Fn(Out) -> NewOut + Send + Sync>,
}

impl<Out: Data, NewOut: Data, PreviousOperators> Map<Out, NewOut, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn new<F>(prev: PreviousOperators, f: F) -> Self
    where
        F: Fn(Out) -> NewOut + Send + Sync + 'static,
    {
        Self {
            prev,
            f: Arc::new(f),
        }
    }
}

impl<Out: Data, NewOut: Data, PreviousOperators> Operator<NewOut>
    for Map<Out, NewOut, PreviousOperators>
where
    PreviousOperators: Operator<Out> + Send,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<NewOut> {
        self.prev.next().map(&*self.f)
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> Map<{} -> {}>",
            self.prev.to_string(),
            std::any::type_name::<Out>(),
            std::any::type_name::<NewOut>()
        )
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<NewOut, _>("Map"))
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn map<NewOut: Data, F>(self, f: F) -> Stream<NewOut, impl Operator<NewOut>>
    where
        F: Fn(Out) -> NewOut + Send + Sync + 'static,
    {
        self.add_operator(|prev| Map::new(prev, f))
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn map<NewOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        F: Fn(KeyValue<&Key, Out>) -> NewOut + Send + Sync + 'static,
    {
        self.add_operator(|prev| {
            Map::new(prev, move |(k, v)| {
                let mapped_value = f((&k, v));
                (k, mapped_value)
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::operator::{Map, Operator, StreamElement};
    use crate::test::FakeOperator;
    use std::time::Duration;

    #[test]
    fn map_stream() {
        let mut fake_operator = FakeOperator::new(0..10u8);
        for i in 0..10 {
            fake_operator.push(StreamElement::Timestamped(i, Duration::from_secs(i as u64)));
        }
        fake_operator.push(StreamElement::Watermark(Duration::from_secs(100)));

        let map = Map::new(fake_operator, |x| x.to_string());
        let map = Map::new(map, |x| x + "000");
        let mut map = Map::new(map, |x| u32::from_str(&x).unwrap());

        for i in 0..10 {
            let elem = map.next();
            assert_eq!(elem, StreamElement::Item(i * 1000));
        }
        for i in 0..10 {
            let elem = map.next();
            assert_eq!(
                elem,
                StreamElement::Timestamped(i * 1000, Duration::from_secs(i as u64))
            );
        }
        assert_eq!(
            map.next(),
            StreamElement::Watermark(Duration::from_secs(100))
        );
        assert_eq!(map.next(), StreamElement::Terminate);
    }
}
