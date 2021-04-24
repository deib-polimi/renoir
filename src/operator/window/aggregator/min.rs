use crate::operator::window::generic_operator::GenericWindowOperator;
use crate::operator::{Data, DataKey, Operator, WindowDescription};
use crate::stream::{KeyValue, KeyedStream, KeyedWindowedStream, Stream, WindowedStream};

impl<Key: DataKey, Out: Data + Ord, WindowDescr, OperatorChain>
    KeyedWindowedStream<Key, Out, OperatorChain, WindowDescr>
where
    WindowDescr: WindowDescription<Key, Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn min(self) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>> {
        let stream = self.inner;
        let descr = self.descr;

        stream.add_operator(|prev| {
            GenericWindowOperator::new("Min", prev, descr, |window| {
                window.items().min().unwrap().clone()
            })
        })
    }
}

impl<Out: Data + Ord, WindowDescr, OperatorChain> WindowedStream<Out, OperatorChain, WindowDescr>
where
    WindowDescr: WindowDescription<(), Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<(), Out>> + Send + 'static,
{
    pub fn min(self) -> Stream<Out, impl Operator<Out>> {
        self.inner.min().unkey().map(|(_, x)| x)
    }
}
