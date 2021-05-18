use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn rich_filter_map<NewOut: Data, F>(self, f: F) -> Stream<NewOut, impl Operator<NewOut>>
    where
        F: FnMut(Out) -> Option<NewOut> + Send + Clone + 'static,
    {
        self.rich_map(f).filter(|x| x.is_some()).map(|x| x.unwrap())
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    pub fn rich_filter_map<NewOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        F: FnMut(KeyValue<&Key, Out>) -> Option<NewOut> + Send + Clone + 'static,
    {
        self.rich_map(f)
            .filter(|(_, x)| x.is_some())
            .map(|(_, x)| x.unwrap())
    }
}
