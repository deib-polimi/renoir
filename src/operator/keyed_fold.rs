use core::iter::Iterator;
use std::collections::HashMap;
use std::sync::Arc;

use crate::operator::{Data, DataKey, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct KeyedFold<Key: DataKey, Out: Data, NewOut: Data, PreviousOperators>
where
    PreviousOperators: Operator<KeyValue<Key, Out>>,
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

impl<Key: DataKey, Out: Data, NewOut: Data, PreviousOperators> Operator<KeyValue<Key, NewOut>>
    for KeyedFold<Key, Out, NewOut, PreviousOperators>
where
    PreviousOperators: Operator<KeyValue<Key, Out>> + Send,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<KeyValue<Key, NewOut>> {
        while !self.received_end {
            match self.prev.next() {
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
                // this block wont sent anything until the stream ends
                StreamElement::FlushBatch => {}
            }
        }

        if let Some(k) = self.accumulators.keys().next() {
            let key = k.clone();
            let entry = self.accumulators.remove_entry(&key).unwrap();
            return if let Some(ts) = self.timestamps.remove(&key) {
                StreamElement::Timestamped(entry, ts)
            } else {
                StreamElement::Item(entry)
            };
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

impl<In: Data, Key: DataKey, Out: Data, OperatorChain> KeyedStream<In, Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn fold<NewOut: Data, F>(
        self,
        init: NewOut,
        f: F,
    ) -> KeyedStream<In, Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
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

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[test]
    fn fold_keyed_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|n| n % 2)
            .fold("".to_string(), |s, n| s + &n.to_string())
            .unkey()
            .collect_vec();
        env.execute();
        let res = res.get().unwrap().into_iter().sorted().collect_vec();
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].1, "02468");
        assert_eq!(res[1].1, "13579");
    }
}
