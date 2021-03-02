use std::hash::Hash;
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

#[derive(Debug)]
pub struct Unkey<Key, Out, OperatorChain>
where
    Key: Clone + Send + Hash + Eq + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    prev: OperatorChain,
    _key_type: PhantomData<Key>,
    _out_type: PhantomData<Out>,
}

impl<Key, Out, OperatorChain> Unkey<Key, Out, OperatorChain>
where
    Key: Clone + Send + Hash + Eq + 'static,
    Out: Clone + Send + 'static,
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
    Key: Clone + Send + Hash + Eq + 'static,
    Out: Clone + Send + 'static,
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

impl<Key, Out, OperatorChain> Clone for Unkey<Key, Out, OperatorChain>
where
    Key: Clone + Send + Hash + Eq + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            _key_type: Default::default(),
            _out_type: Default::default(),
        }
    }
}

impl<In, Key, Out, OperatorChain> KeyedStream<In, Key, Out, OperatorChain>
where
    Key: Clone + Send + Hash + Eq + 'static,
    In: Clone + Send + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn unkey(self) -> Stream<In, Out, Unkey<Key, Out, OperatorChain>> {
        self.0.add_operator(|prev| Unkey::new(prev))
    }
}