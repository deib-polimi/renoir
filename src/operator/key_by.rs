use std::hash::Hash;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::operator::Keyer;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::KeyValue;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct KeyBy<Key, Out, OperatorChain>
where
    Key: Clone + Serialize + DeserializeOwned + Send + Hash + Eq + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    #[derivative(Debug = "ignore")]
    keyer: Keyer<Key, Out>,
}

impl<Key, Out, OperatorChain> KeyBy<Key, Out, OperatorChain>
where
    Key: Clone + Serialize + DeserializeOwned + Send + Hash + Eq + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out>,
{
    pub fn new(prev: OperatorChain, keyer: Keyer<Key, Out>) -> Self {
        Self { prev, keyer }
    }
}

#[async_trait]
impl<Key, Out, OperatorChain> Operator<KeyValue<Key, Out>> for KeyBy<Key, Out, OperatorChain>
where
    Key: Clone + Serialize + DeserializeOwned + Send + Hash + Eq + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out> + Send,
{
    async fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata).await;
    }

    async fn next(&mut self) -> StreamElement<KeyValue<Key, Out>> {
        match self.prev.next().await {
            StreamElement::Item(t) => StreamElement::Item(((self.keyer)(&t), t)),
            StreamElement::Timestamped(t, ts) => {
                StreamElement::Timestamped(((self.keyer)(&t), t), ts)
            }
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::End => StreamElement::End,
        }
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> KeyBy<{}>",
            self.prev.to_string(),
            std::any::type_name::<Key>(),
        )
    }
}
