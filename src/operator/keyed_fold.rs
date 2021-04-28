use core::iter::Iterator;
use std::collections::hash_map::{DefaultHasher, Entry};
use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::Arc;

use crate::block::{BlockStructure, NextStrategy, OperatorStructure};
use crate::operator::{Data, DataKey, EndBlock, KeyBy, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

#[derive(Derivative)]
#[derivative(Debug, Clone)]
pub struct KeyedFold<Key: DataKey, Out: Data, NewOut: Data, PreviousOperators>
where
    PreviousOperators: Operator<KeyValue<Key, Out>>,
{
    prev: PreviousOperators,
    #[derivative(Debug = "ignore")]
    fold: Arc<dyn Fn(&mut NewOut, Out) + Send + Sync>,
    init: NewOut,
    accumulators: HashMap<Key, NewOut>,
    timestamps: HashMap<Key, Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
    received_end_iter: bool,
}

impl<Key: DataKey, Out: Data, NewOut: Data, PreviousOperators: Operator<KeyValue<Key, Out>>>
    KeyedFold<Key, Out, NewOut, PreviousOperators>
{
    fn new<F>(prev: PreviousOperators, init: NewOut, fold: F) -> Self
    where
        F: Fn(&mut NewOut, Out) + Send + Sync + 'static,
    {
        KeyedFold {
            prev,
            fold: Arc::new(fold),
            init,
            accumulators: Default::default(),
            timestamps: Default::default(),
            max_watermark: None,
            received_end: false,
            received_end_iter: false,
        }
    }

    /// Process a new item, folding it with the accumulator inside the hashmap.
    fn process_item(&mut self, key: Key, value: Out) {
        match self.accumulators.entry(key) {
            Entry::Vacant(entry) => {
                let mut acc = self.init.clone();
                (self.fold)(&mut acc, value);
                entry.insert(acc);
            }
            Entry::Occupied(mut entry) => {
                (self.fold)(entry.get_mut(), value);
            }
        }
    }
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
                StreamElement::Terminate => self.received_end = true,
                StreamElement::FlushAndRestart => {
                    self.received_end = true;
                    self.received_end_iter = true;
                }
                StreamElement::Watermark(ts) => {
                    self.max_watermark = Some(self.max_watermark.unwrap_or(ts).max(ts))
                }
                StreamElement::Item((k, v)) => {
                    self.process_item(k, v);
                }
                StreamElement::Timestamped((k, v), ts) => {
                    self.process_item(k.clone(), v);
                    self.timestamps
                        .entry(k)
                        .and_modify(|entry| *entry = (*entry).max(ts))
                        .or_insert(ts);
                }
                // this block won't sent anything until the stream ends
                StreamElement::FlushBatch => {}
            }
        }

        if let Some(k) = self.accumulators.keys().next() {
            let key = k.clone();
            let (key, value) = self.accumulators.remove_entry(&key).unwrap();
            return if let Some(ts) = self.timestamps.remove(&key) {
                StreamElement::Timestamped((key, value), ts)
            } else {
                StreamElement::Item((key, value))
            };
        }

        if let Some(ts) = self.max_watermark.take() {
            return StreamElement::Watermark(ts);
        }

        // the end was not really the end... just the end of one iteration!
        if self.received_end_iter {
            self.received_end_iter = false;
            self.received_end = false;
            return StreamElement::FlushAndRestart;
        }

        StreamElement::Terminate
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> KeyedFold<{} -> {}>",
            self.prev.to_string(),
            std::any::type_name::<KeyValue<Key, Out>>(),
            std::any::type_name::<KeyValue<Key, NewOut>>()
        )
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<KeyValue<Key, NewOut>, _>(
                "KeyedFold",
            ))
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn group_by_fold<Key: DataKey, NewOut: Data, Keyer, Local, Global>(
        self,
        keyer: Keyer,
        init: NewOut,
        local: Local,
        global: Global,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        Keyer: Fn(&Out) -> Key + Send + Sync + 'static,
        Local: Fn(&mut NewOut, Out) + Send + Sync + 'static,
        Global: Fn(&mut NewOut, NewOut) + Send + Sync + 'static,
    {
        let keyer = Arc::new(keyer);

        // GroupBy based on key
        let next_strategy: NextStrategy<(Key, NewOut)> =
            NextStrategy::GroupBy(Arc::new(move |(key, _out)| {
                let mut s = DefaultHasher::new();
                key.hash(&mut s);
                s.finish() as usize
            }));

        let new_stream = self
            // key_by with given keyer
            .add_operator(|prev| KeyBy::new(prev, keyer.clone()))
            // local fold
            .add_operator(|prev| KeyedFold::new(prev, init.clone(), local))
            // group by key
            .add_block(EndBlock::new, next_strategy)
            // global fold
            .add_operator(|prev| KeyedFold::new(prev, init.clone(), global));

        KeyedStream(new_stream)
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn fold<NewOut: Data, F>(
        self,
        init: NewOut,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        F: Fn(&mut NewOut, Out) + Send + Sync + 'static,
    {
        self.add_operator(|prev| KeyedFold::new(prev, init, f))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use itertools::Itertools;

    use crate::operator::keyed_fold::KeyedFold;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    #[allow(clippy::identity_op)]
    fn test_keyed_fold_no_timestamp() {
        let data = (0..10u8).map(|x| (x % 2, x)).collect_vec();
        let fake_operator = FakeOperator::new(data.into_iter());
        let mut keyed_fold = KeyedFold::new(fake_operator, 0, |a, b| *a += b);

        let mut res = vec![];
        for _ in 0..2 {
            let item = keyed_fold.next();
            match item {
                StreamElement::Item(x) => res.push(x),
                other => panic!("Expecting StreamElement::Item, got {}", other.variant()),
            }
        }

        assert_eq!(keyed_fold.next(), StreamElement::Terminate);

        res.sort_unstable();
        assert_eq!(res[0].1, 0 + 2 + 4 + 6 + 8);
        assert_eq!(res[1].1, 1 + 3 + 5 + 7 + 9);
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn test_keyed_fold_timestamp() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Timestamped((0, 0), Duration::from_secs(1)));
        fake_operator.push(StreamElement::Timestamped((1, 1), Duration::from_secs(2)));
        fake_operator.push(StreamElement::Timestamped((0, 2), Duration::from_secs(3)));
        fake_operator.push(StreamElement::Watermark(Duration::from_secs(4)));

        let mut keyed_fold = KeyedFold::new(fake_operator, 0, |a, b| *a += b);

        let mut res = vec![];
        for _ in 0..2 {
            let item = keyed_fold.next();
            match item {
                StreamElement::Timestamped(x, ts) => res.push((x, ts)),
                other => panic!(
                    "Expecting StreamElement::Timestamped, got {}",
                    other.variant()
                ),
            }
        }

        assert_eq!(
            keyed_fold.next(),
            StreamElement::Watermark(Duration::from_secs(4))
        );
        assert_eq!(keyed_fold.next(), StreamElement::Terminate);

        res.sort_unstable();
        assert_eq!(res[0].0 .1, 0 + 2);
        assert_eq!(res[0].1, Duration::from_secs(3));
        assert_eq!(res[1].0 .1, 1);
        assert_eq!(res[1].1, Duration::from_secs(2));
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn test_keyed_fold_end_iter() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Item((0, 0)));
        fake_operator.push(StreamElement::Item((0, 2)));
        fake_operator.push(StreamElement::FlushAndRestart);
        fake_operator.push(StreamElement::Item((1, 1)));
        fake_operator.push(StreamElement::Item((1, 3)));
        fake_operator.push(StreamElement::FlushAndRestart);

        let mut keyed_fold = KeyedFold::new(fake_operator, 0, |a, b| *a += b);

        assert_eq!(keyed_fold.next(), StreamElement::Item((0, 0 + 2)));
        assert_eq!(keyed_fold.next(), StreamElement::FlushAndRestart);
        assert_eq!(keyed_fold.next(), StreamElement::Item((1, 1 + 3)));
        assert_eq!(keyed_fold.next(), StreamElement::FlushAndRestart);
        assert_eq!(keyed_fold.next(), StreamElement::Terminate);
    }
}
