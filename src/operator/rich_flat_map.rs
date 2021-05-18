use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn rich_flat_map<MapOut: Data, NewOut: Data, F>(
        self,
        f: F,
    ) -> Stream<NewOut, impl Operator<NewOut>>
    where
        MapOut: IntoIterator<Item = NewOut>,
        <MapOut as IntoIterator>::IntoIter: Clone + Send + 'static,
        F: FnMut(Out) -> MapOut + Send + Clone + 'static,
    {
        self.rich_map(f).flatten()
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    pub fn rich_flat_map<NewOut: Data, MapOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        MapOut: IntoIterator<Item = NewOut>,
        <MapOut as IntoIterator>::IntoIter: Clone + Send + 'static,
        F: FnMut(KeyValue<&Key, Out>) -> MapOut + Clone + Send + 'static,
    {
        self.rich_map(f).flatten()
    }
}
