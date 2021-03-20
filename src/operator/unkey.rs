use std::hash::Hash;

use crate::operator::{Data, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};

impl<In: Data, Key, Out: Data, OperatorChain> KeyedStream<In, Key, Out, OperatorChain>
where
    Key: Data + Hash + Eq,
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn unkey(self) -> Stream<In, KeyValue<Key, Out>, impl Operator<KeyValue<Key, Out>>> {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[test]
    fn unkey_keyed_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(0..10u8);
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
