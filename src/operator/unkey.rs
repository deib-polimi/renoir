use std::hash::Hash;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::operator::Operator;
use crate::stream::{KeyValue, KeyedStream, Stream};

impl<In, Key, Out, OperatorChain> KeyedStream<In, Key, Out, OperatorChain>
where
    Key: Clone + Serialize + DeserializeOwned + Send + Hash + Eq + 'static,
    In: Clone + Serialize + DeserializeOwned + Send + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn unkey(self) -> Stream<In, KeyValue<Key, Out>, impl Operator<KeyValue<Key, Out>>> {
        self.0
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
    fn unkey_keyed_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(from_iter(0..10u8));
        let res = env
            .stream(source)
            .key_by(|&n| n.to_string())
            .unkey()
            .collect_vec();
        env.execute();
        let res = res.get().unwrap().into_iter().sorted().collect_vec();
        let expected = (0..10u8).map(|n| (n.to_string(), n)).collect_vec();
        assert_eq!(res, expected);
    }
}
