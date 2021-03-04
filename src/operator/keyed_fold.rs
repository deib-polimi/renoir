use core::iter::Iterator;
use std::collections::HashMap;
use std::hash::Hash;

use async_std::sync::Arc;
use async_trait::async_trait;

use crate::operator::{Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct KeyedFold<Key, Out, NewOut, PreviousOperators>
where
    Out: Clone + Send + 'static,
    Key: Clone + Send + Hash + Eq + 'static,
    PreviousOperators: Operator<KeyValue<Key, Out>>,
    NewOut: Clone + Send + 'static,
{
    prev: PreviousOperators,
    #[derivative(Debug = "ignore")]
    fold: Arc<dyn Fn(NewOut, Out) -> NewOut + Send + Sync>,
    init: NewOut,
    accumulators: HashMap<Key, NewOut>,
    timestamps: HashMap<Key, Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
}

#[async_trait]
impl<Key, Out, NewOut, PreviousOperators> Operator<KeyValue<Key, NewOut>>
    for KeyedFold<Key, Out, NewOut, PreviousOperators>
where
    Out: Clone + Send + 'static,
    Key: Clone + Send + Hash + Eq + 'static,
    PreviousOperators: Operator<KeyValue<Key, Out>> + Send,
    NewOut: Clone + Send + 'static,
{
    async fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata).await;
    }

    async fn next(&mut self) -> StreamElement<KeyValue<Key, NewOut>> {
        while !self.received_end {
            match self.prev.next().await {
                StreamElement::End => self.received_end = true,
                StreamElement::Watermark(ts) => {
                    self.max_watermark = Some(self.max_watermark.unwrap_or(ts).max(ts))
                }
                StreamElement::Item((k, v)) => {
                    let acc = self
                        .accumulators
                        .remove(&k)
                        .unwrap_or_else(|| self.init.clone());
                    self.accumulators.insert(k, (self.fold)(acc, v));
                }
                StreamElement::Timestamped((k, v), ts) => {
                    let acc = self
                        .accumulators
                        .remove(&k)
                        .unwrap_or_else(|| self.init.clone());
                    self.accumulators.insert(k.clone(), (self.fold)(acc, v));
                    self.timestamps
                        .entry(k)
                        .and_modify(|entry| *entry = (*entry).max(ts))
                        .or_insert(ts);
                }
            }
        }

        if let Some(k) = self.accumulators.keys().next() {
            let key = k.clone();
            let entry = self.accumulators.remove_entry(&key).unwrap();
            if let Some(ts) = self.timestamps.remove(&key) {
                return StreamElement::Timestamped(entry, ts);
            } else {
                return StreamElement::Item(entry);
            }
        }

        if let Some(ts) = self.max_watermark.take() {
            return StreamElement::Watermark(ts);
        }

        StreamElement::End
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> KeyedFold<{} -> {}>",
            self.prev.to_string(),
            std::any::type_name::<KeyValue<Key, Out>>(),
            std::any::type_name::<KeyValue<Key, NewOut>>()
        )
    }
}

impl<In, Key, Out, OperatorChain> KeyedStream<In, Key, Out, OperatorChain>
where
    Key: Clone + Send + Hash + Eq + 'static,
    In: Clone + Send + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn fold<NewOut, F>(
        self,
        init: NewOut,
        f: F,
    ) -> KeyedStream<In, Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        NewOut: Clone + Send + 'static,
        F: Fn(NewOut, Out) -> NewOut + Send + Sync + 'static,
    {
        self.add_operator(|prev| KeyedFold {
            prev,
            fold: Arc::new(f),
            init,
            accumulators: Default::default(),
            timestamps: Default::default(),
            max_watermark: None,
            received_end: false,
        })
    }
}
