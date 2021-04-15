use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, DataKey, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};
use std::marker::PhantomData;

#[derive(Clone)]
struct Filter<Out: Data, PreviousOperator, Predicate>
where
    Predicate: Fn(&Out) -> bool + Clone + 'static,
    PreviousOperator: Operator<Out> + Send + 'static,
{
    prev: PreviousOperator,
    predicate: Predicate,
    _out: PhantomData<Out>,
}

impl<Out: Data, PreviousOperator, Predicate> Operator<Out>
    for Filter<Out, PreviousOperator, Predicate>
where
    Predicate: Fn(&Out) -> bool + Clone + 'static,
    PreviousOperator: Operator<Out> + Send + 'static,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Out> {
        loop {
            match self.prev.next() {
                StreamElement::Item(ref item) | StreamElement::Timestamped(ref item, _)
                    if !(self.predicate)(item) => {}
                element => return element,
            }
        }
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> Filter<{}>",
            self.prev.to_string(),
            std::any::type_name::<Out>()
        )
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Out, _>("Filter"))
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn filter<Predicate>(self, predicate: Predicate) -> Stream<Out, impl Operator<Out>>
    where
        Predicate: Fn(&Out) -> bool + Clone + 'static,
    {
        self.add_operator(|prev| Filter {
            prev,
            predicate,
            _out: PhantomData,
        })
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn filter<Predicate>(
        self,
        predicate: Predicate,
    ) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>>
    where
        Predicate: Fn(&KeyValue<Key, Out>) -> bool + Clone + 'static,
    {
        self.add_operator(|prev| Filter {
            prev,
            predicate,
            _out: PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[test]
    fn filter_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let res = env.stream(source).filter(|x| x % 2 == 1).collect_vec();
        env.execute();
        let res = res.get().unwrap();
        assert_eq!(res, &[1, 3, 5, 7, 9]);
    }

    #[test]
    fn filter_keyed_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|n| n % 2)
            .filter(|(_key, x)| *x < 6)
            .unkey()
            .map(|(_k, v)| v)
            .collect_vec();
        env.execute();
        let mut res = res.get().unwrap();
        res.sort_unstable();
        assert_eq!(res, (0..6u8).collect_vec());
    }
}
