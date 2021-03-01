use std::hash::Hash;

use async_trait::async_trait;

use crate::operator::sink::Keyer;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::KeyValue;

pub struct KeyBy<Key, Out, OperatorChain>
where
    Key: Clone + Send + Hash + Eq + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    keyer: Keyer<Key, Out>,
}

impl<Key, Out, OperatorChain> KeyBy<Key, Out, OperatorChain>
where
    Key: Clone + Send + Hash + Eq + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out>,
{
    pub fn new(prev: OperatorChain, keyer: Keyer<Key, Out>) -> Self {
        Self { prev, keyer }
    }
}

#[async_trait]
impl<Key, Out, OperatorChain> Operator<KeyValue<Key, Out>> for KeyBy<Key, Out, OperatorChain>
where
    Key: Clone + Send + Hash + Eq + 'static,
    Out: Clone + Send + 'static,
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

impl<Key, Out, OperatorChain> Clone for KeyBy<Key, Out, OperatorChain>
where
    Key: Clone + Send + Hash + Eq + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out> + Send,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            keyer: self.keyer.clone(),
        }
    }
}
