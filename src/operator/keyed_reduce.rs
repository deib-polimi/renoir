use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream};

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn reduce<F>(self, f: F) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>>
    where
        F: Fn(Out, Out) -> Out + Send + Sync + 'static,
    {
        self.fold(None, move |acc, value| match acc {
            None => Some(value),
            Some(acc) => Some(f(acc, value)),
        })
        .map(|(_, value)| value.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[test]
    fn reduce_keyed_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(0..10u32);
        let res = env
            .stream(source)
            .group_by(|n| n % 2)
            .map(|(_, x)| x.to_string())
            .reduce(|acc, y| acc + &y)
            .unkey()
            .collect_vec();
        env.execute();
        let res = res.get().unwrap().into_iter().sorted().collect_vec();
        assert_eq!(res.len(), 2);
        assert_eq!(res[0], (0, "02468".into()));
        assert_eq!(res[1], (1, "13579".into()));
    }
}
