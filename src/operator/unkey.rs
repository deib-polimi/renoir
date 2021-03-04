use std::hash::Hash;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

#[derive(Debug, Clone)]
pub struct Unkey<Key, Out, OperatorChain>
where
    Key: Clone + Serialize + DeserializeOwned + Send + Hash + Eq + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    prev: OperatorChain,
    _key_type: PhantomData<Key>,
    _out_type: PhantomData<Out>,
}

impl<Key, Out, OperatorChain> Unkey<Key, Out, OperatorChain>
where
    Key: Clone + Serialize + DeserializeOwned + Send + Hash + Eq + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    pub fn new(prev: OperatorChain) -> Self {
        Self {
            prev,
            _key_type: Default::default(),
            _out_type: Default::default(),
        }
    }
}

#[async_trait]
impl<Key, Out, OperatorChain> Operator<Out> for Unkey<Key, Out, OperatorChain>
where
    Key: Clone + Serialize + DeserializeOwned + Send + Hash + Eq + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + Send,
{
    async fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata).await;
    }

    async fn next(&mut self) -> StreamElement<Out> {
        self.prev.next().await.map(|(_key, val)| val)
    }

    fn to_string(&self) -> String {
        format!("{} -> Unkey", self.prev.to_string())
    }
}

impl<In, Key, Out, OperatorChain> KeyedStream<In, Key, Out, OperatorChain>
where
    Key: Clone + Serialize + DeserializeOwned + Send + Hash + Eq + 'static,
    In: Clone + Serialize + DeserializeOwned + Send + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn unkey(self) -> Stream<In, Out, Unkey<Key, Out, OperatorChain>> {
        self.0.add_operator(|prev| Unkey::new(prev))
    }
}
