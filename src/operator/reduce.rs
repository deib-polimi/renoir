use crate::operator::{Data, Operator};
use crate::stream::Stream;
use std::sync::Arc;

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn reduce<F>(self, f: F) -> Stream<Out, impl Operator<Out>>
    where
        F: Fn(Out, Out) -> Out + Send + Sync + 'static,
    {
        self.fold(None, move |acc, value| match acc {
            None => Some(value),
            Some(acc) => Some(f(acc, value)),
        })
        .map(|value| value.unwrap())
    }

    pub fn reduce_assoc<F>(self, f: F) -> Stream<Out, impl Operator<Out>>
    where
        F: Fn(Out, Out) -> Out + Send + Sync + 'static,
    {
        // FIXME: remove Arc if reduce function will be Clone
        let f = Arc::new(f);
        let f2 = f.clone();

        self.fold_assoc(
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
        .map(|value| value.unwrap())
    }
}

#[cfg(test)]
mod tests {

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[test]
    fn reduce_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(0..10u8);
        let res = env.stream(source).reduce(|acc, v| acc + v).collect_vec();
        env.execute();
        let res = res.get().unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], 45);
    }

    #[test]
    fn reduce_assoc_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(0..10u8);
        let res = env
            .stream(source)
            .reduce_assoc(|acc, v| acc + v)
            .collect_vec();
        env.execute();
        let res = res.get().unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], 45);
    }

    #[test]
    fn reduce_shuffled_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(0..10u8);
        let res = env
            .stream(source)
            .shuffle()
            .reduce(|acc, v| acc + v)
            .collect_vec();
        env.execute();
        let res = res.get().unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], 45);
    }

    #[test]
    fn reduce_assoc_shuffled_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(0..10u8);
        let res = env
            .stream(source)
            .shuffle()
            .reduce_assoc(|acc, v| acc + v)
            .collect_vec();
        env.execute();
        let res = res.get().unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], 45);
    }
}
