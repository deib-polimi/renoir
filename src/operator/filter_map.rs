use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn filter_map<NewOut: Data, F>(self, f: F) -> Stream<NewOut, impl Operator<NewOut>>
    where
        F: Fn(Out) -> Option<NewOut> + Send + Sync + 'static,
    {
        self.map(f).filter(|x| x.is_some()).map(|x| x.unwrap())
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn filter_map<NewOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        F: Fn(KeyValue<&Key, Out>) -> Option<NewOut> + Send + Sync + 'static,
    {
        self.map(f)
            .filter(|(_, x)| x.is_some())
            .map(|(_, x)| x.unwrap())
    }
}
