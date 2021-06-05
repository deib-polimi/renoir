use crate::operator::{Data, DataKey, Operator, WindowDescription};
use crate::stream::{KeyValue, KeyedStream, KeyedWindowedStream, Stream, WindowedStream};
use std::iter::Sum;

impl<Key: DataKey, Out: Data, WindowDescr, OperatorChain>
    KeyedWindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Key, Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
    for<'a> Out: Sum<&'a Out>,
{
    pub fn sum(self) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>> {
        self.add_generic_window_operator("WindowSum", |window| window.items().sum())
    }
}

impl<Out: Data + Ord, WindowDescr, OperatorChain>
    WindowedStream<Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<(), Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<(), Out>> + 'static,
    for<'a> Out: Sum<&'a Out>,
{
    pub fn sum(self) -> Stream<Out, impl Operator<Out>> {
        self.inner.sum().drop_key()
    }
}
