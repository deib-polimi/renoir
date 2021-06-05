use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    pub fn unkey(self) -> Stream<KeyValue<Key, Out>, impl Operator<KeyValue<Key, Out>>> {
        self.0
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    pub fn drop_key(self) -> Stream<Out, impl Operator<Out>> {
        self.0.map(|(_k, v)| v)
    }
}
