use crate::operator::{Data, DataKey, Operator, WindowDescription};
use crate::stream::{KeyValue, KeyedStream, KeyedWindowedStream, Stream, WindowedStream};

impl<Key: DataKey, Out: Data, WindowDescr, OperatorChain>
    KeyedWindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Key, Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    pub fn map<NewOut: Data, F>(
        self,
        map_func: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        F: Fn(&mut dyn Iterator<Item = &Out>) -> NewOut + Clone + Send + 'static,
    {
        self.add_generic_window_operator("WindowMap", move |window| (map_func)(&mut window.items()))
    }
}

impl<Out: Data, WindowDescr, OperatorChain> WindowedStream<Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<(), Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<(), Out>> + 'static,
{
    pub fn map<NewOut: Data, F>(self, map_func: F) -> Stream<NewOut, impl Operator<NewOut>>
    where
        F: Fn(&mut dyn Iterator<Item = &Out>) -> NewOut + Clone + Send + 'static,
    {
        self.inner.map(map_func).unkey().map(|(_, x)| x)
    }
}
