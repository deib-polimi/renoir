use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};
use std::sync::Arc;

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn group_by_reduce<Key: DataKey, Keyer, F>(
        self,
        keyer: Keyer,
        f: F,
    ) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>>
    where
        Keyer: Fn(&Out) -> Key + Send + Sync + 'static,
        F: Fn(Out, Out) -> Out + Send + Sync + 'static,
    {
        // FIXME: remove Arc if reduce function will be Clone
        let f = Arc::new(f);
        let f2 = f.clone();

        self.group_by_fold(
            keyer,
            None,
            move |acc, value| match acc {
                None => Some(value),
                Some(acc) => Some(f(acc, value)),
            },
            move |acc1, acc2| match acc1 {
                None => acc2,
                Some(acc1) => match acc2 {
                    None => Some(acc1),
                    Some(acc2) => Some(f2(acc1, acc2)),
                },
            },
        )
        .map(|(_, value)| value.unwrap())
    }
}

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
    fn group_by_reduce_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(0..10u32);
        let res = env
            .stream(source)
            .map(|x| x.to_string())
            .group_by_reduce(|n| n.parse::<u32>().unwrap() % 2, |acc, y| acc + &y)
            .unkey()
            .collect_vec();
        env.execute();
        let res = res.get().unwrap().into_iter().sorted().collect_vec();
        assert_eq!(res.len(), 2);
        assert_eq!(res[0], (0, "02468".into()));
        assert_eq!(res[1], (1, "13579".into()));
    }

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
