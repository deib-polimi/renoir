use std::collections::VecDeque;

use crate::operator::window::KeyedWindowManager;
use crate::operator::{Data, DataKey, Operator, StreamElement, WindowDescription, WindowGenerator};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, WindowedStream};

#[derive(Clone)]
pub(crate) struct First<Key: DataKey, Out: Data, WindowDescr, OperatorChain>
where
    WindowDescr: WindowDescription<Key, Out> + Clone,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    manager: KeyedWindowManager<Key, Out, WindowDescr>,
    prev: OperatorChain,
    buffer: VecDeque<StreamElement<KeyValue<Key, Out>>>,
}

impl<Key: DataKey, Out: Data, WindowDescr, OperatorChain>
    First<Key, Out, WindowDescr, OperatorChain>
where
    WindowDescr: WindowDescription<Key, Out> + Clone,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    pub(crate) fn new(prev: OperatorChain, descr: WindowDescr) -> Self {
        Self {
            manager: KeyedWindowManager::new(descr),
            prev,
            buffer: Default::default(),
        }
    }
}

impl<Key: DataKey, Out: Data, WindowDescr, OperatorChain> Operator<KeyValue<Key, Out>>
    for First<Key, Out, WindowDescr, OperatorChain>
where
    WindowDescr: WindowDescription<Key, Out> + Clone,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<KeyValue<Key, Out>> {
        loop {
            if let Some(item) = self.buffer.pop_front() {
                return item;
            }

            for (key, window_gen) in self.manager.add(self.prev.next()) {
                while let Some(window) = window_gen.next_window() {
                    if let Some(first) = window.items().next() {
                        self.buffer.push_back(if let Some(ts) = window.timestamp {
                            StreamElement::Timestamped((key.clone(), first.clone()), ts)
                        } else {
                            StreamElement::Item((key.clone(), first.clone()))
                        });
                    }
                }
            }

            while let Some(el) = self.manager.next_extra_item() {
                self.buffer.push_back(el);
            }
        }
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> {} -> First",
            self.prev.to_string(),
            self.manager.to_string()
        )
    }
}

impl<Key: DataKey, Out: Data, WindowDescr, OperatorChain>
    WindowedStream<Key, Out, OperatorChain, WindowDescr>
where
    WindowDescr: WindowDescription<Key, Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    /// For each window, return the first element.
    pub fn first(self) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>> {
        let descr = self.descr.clone();
        self.inner.add_operator(|prev| First::new(prev, descr))
    }
}

#[cfg(test)]
mod tests {
    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::{source, CountWindow};

    #[test]
    fn test_first() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|x| x % 2)
            .window(CountWindow::new(3, 2))
            .first()
            .unkey()
            .map(|(_, x)| x)
            .collect_vec();
        env.execute();
        let mut res = res.get().unwrap();
        res.sort_unstable();
        assert_eq!(res, vec![0, 1, 4, 5, 8, 9]);
    }
}
