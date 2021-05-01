use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    pub fn unkey(self) -> Stream<KeyValue<Key, Out>, impl Operator<KeyValue<Key, Out>>> {
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
        let source = source::IteratorSource::new(0..10u8);
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
