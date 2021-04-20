use crate::operator::window::generic_operator::GenericWindowOperator;
use crate::operator::{Data, DataKey, Operator, WindowDescription};
use crate::stream::{KeyValue, KeyedStream, WindowedStream};
use std::iter::Sum;

impl<Key: DataKey, Out: Data, WindowDescr, OperatorChain>
    WindowedStream<Key, Out, OperatorChain, WindowDescr>
where
    WindowDescr: WindowDescription<Key, Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
    for<'a> Out: Sum<&'a Out>,
{
    pub fn sum(self) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>> {
        let stream = self.inner;
        let descr = self.descr;

        stream.add_operator(|prev| {
            GenericWindowOperator::new("Sum", prev, descr, |window| window.items().sum())
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::{source, CountWindow};

    #[test]
    fn test_sum_window() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|x| x % 2)
            .window(CountWindow::sliding(3, 2))
            .sum()
            .unkey()
            .map(|(_, x)| x)
            .collect_vec();
        env.execute();
        let mut res = res.get().unwrap();
        res.sort_unstable();
        assert_eq!(res, vec![6, 8, 9, 9, 18, 21]);
    }
}
