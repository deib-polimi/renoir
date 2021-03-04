use std::hash::Hash;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::operator::Operator;
use crate::stream::{KeyValue, KeyedStream, Stream};

impl<In, Key, Out, OperatorChain> KeyedStream<In, Key, Out, OperatorChain>
where
    Key: Clone + Serialize + DeserializeOwned + Send + Hash + Eq + 'static,
    In: Clone + Serialize + DeserializeOwned + Send + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn unkey(self) -> Stream<In, KeyValue<Key, Out>, impl Operator<KeyValue<Key, Out>>> {
        self.0
    }
}
