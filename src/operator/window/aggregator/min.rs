use crate::operator::{Data, DataKey, Operator, WindowDescription};
use crate::stream::{KeyValue, KeyedStream, KeyedWindowedStream, Stream, WindowedStream};

impl<Key: DataKey, Out: Data + Ord, WindowDescr, OperatorChain>
    KeyedWindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Key, Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn min(self) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>> {
        self.add_generic_window_operator("Min", |window| window.items().min().unwrap().clone())
    }
}

impl<Out: Data + Ord, WindowDescr, OperatorChain>
    WindowedStream<Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<(), Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<(), Out>> + Send + 'static,
{
    pub fn min(self) -> Stream<Out, impl Operator<Out>> {
        self.inner.min().unkey().map(|(_, x)| x)
    }
}
