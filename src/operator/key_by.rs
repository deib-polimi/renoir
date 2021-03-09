use std::hash::Hash;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::operator::Keyer;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};
use std::sync::Arc;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct KeyBy<Key, Out, OperatorChain>
where
    Key: Clone + Serialize + DeserializeOwned + Send + Hash + Eq + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    #[derivative(Debug = "ignore")]
    keyer: Keyer<Key, Out>,
}

impl<Key, Out, OperatorChain> KeyBy<Key, Out, OperatorChain>
where
    Key: Clone + Serialize + DeserializeOwned + Send + Hash + Eq + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out>,
{
    pub fn new(prev: OperatorChain, keyer: Keyer<Key, Out>) -> Self {
        Self { prev, keyer }
    }
}

impl<Key, Out, OperatorChain> Operator<KeyValue<Key, Out>> for KeyBy<Key, Out, OperatorChain>
where
    Key: Clone + Serialize + DeserializeOwned + Send + Hash + Eq + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out> + Send,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<KeyValue<Key, Out>> {
        match self.prev.next() {
            StreamElement::Item(t) => StreamElement::Item(((self.keyer)(&t), t)),
            StreamElement::Timestamped(t, ts) => {
                StreamElement::Timestamped(((self.keyer)(&t), t), ts)
            }
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::End => StreamElement::End,
        }
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> KeyBy<{}>",
            self.prev.to_string(),
            std::any::type_name::<Key>(),
        )
    }
}

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    In: Clone + Serialize + DeserializeOwned + Send + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn key_by<Key, Keyer>(
        self,
        keyer: Keyer,
    ) -> KeyedStream<In, Key, Out, impl Operator<KeyValue<Key, Out>>>
    where
        Key: Clone + Serialize + DeserializeOwned + Send + Hash + Eq + 'static,
        Keyer: Fn(&Out) -> Key + Send + Sync + 'static,
    {
        let keyer = Arc::new(keyer);
        KeyedStream(self.add_operator(|prev| KeyBy::new(prev, keyer)))
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use std::stream::from_iter;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[std::test]
    fn key_by_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(from_iter(0..10u8));
        let res = env.stream(source).key_by(|&n| n).unkey().collect_vec();
        env.execute();
        let res = res.get().unwrap().into_iter().sorted().collect_vec();
        let expected = (0..10u8).map(|n| (n, n)).collect_vec();
        assert_eq!(res, expected);
    }

    #[std::test]
    fn key_by_stream2() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(from_iter(0..100u8));
        let res = env
            .stream(source)
            .key_by(|&n| n.to_string().chars().next().unwrap())
            .unkey()
            .collect_vec();
        env.execute();
        let res = res.get().unwrap().into_iter().sorted().collect_vec();
        let expected = (0..100u8)
            .map(|n| (n.to_string().chars().next().unwrap(), n))
            .sorted()
            .collect_vec();
        assert_eq!(res, expected);
    }
}
