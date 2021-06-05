use crate::operator::{Data, DataKey, Operator, WindowDescription};
use crate::stream::{KeyValue, KeyedStream, KeyedWindowedStream, Stream, WindowedStream};

impl<Key: DataKey, Out: Data, WindowDescr, OperatorChain>
    KeyedWindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Key, Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// For each window, return the first element.
    pub fn first(self) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>> {
        self.add_generic_window_operator("WindowFirst", |window| {
            window.items().next().unwrap().clone()
        })
    }
}

impl<Out: Data, WindowDescr, OperatorChain> WindowedStream<Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<(), Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<(), Out>> + 'static,
{
    pub fn first(self) -> Stream<Out, impl Operator<Out>> {
        self.inner.first().drop_key()
    }
}
