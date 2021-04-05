use crate::operator::window::generic_operator::GenericWindowOperator;
use crate::operator::{Data, DataKey, Operator, WindowDescription};
use crate::stream::{KeyValue, KeyedStream, WindowedStream};

impl<Key: DataKey, Out: Data, WindowDescr, OperatorChain>
    WindowedStream<Key, Out, OperatorChain, WindowDescr>
where
    WindowDescr: WindowDescription<Key, Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn fold<NewOut: Data, F>(
        self,
        init: NewOut,
        fold: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        F: Fn(NewOut, &Out) -> NewOut + Clone + 'static,
    {
        let descr = self.descr.clone();
        self.inner.add_operator(|prev| {
            GenericWindowOperator::new("Fold", prev, descr, move |window| {
                let mut res = init.clone();
                for value in window.items() {
                    res = (fold)(res, value);
                }
                res
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::{source, CountWindow};

    #[test]
    fn test_fold_window() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|x| x % 2)
            .window(CountWindow::new(3, 2))
            .fold(0, |acc, x| acc + x)
            .unkey()
            .map(|(_, x)| x)
            .collect_vec();
        env.execute();
        let mut res = res.get().unwrap();
        res.sort_unstable();
        // Windows
        // [0, 2, 4] -> 6
        // [4, 6, 8] -> 18
        // [8] -> 8
        // [1, 3, 5] -> 9
        // [5, 7, 9] -> 21
        // [9] -> 9
        assert_eq!(res, vec![6, 8, 9, 9, 18, 21]);
    }
}
