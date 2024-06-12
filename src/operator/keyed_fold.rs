use core::iter::Iterator;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Display;

use crate::block::{BlockStructure, OperatorStructure};

use crate::operator::{Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;
use crate::stream::KeyedItem;

pub struct KeyedFold<O: Send + Clone, F, Op>
where
    F: Fn(&mut O, <Op::Out as KeyedItem>::Value) + Send + Clone,
    Op: Operator,
    Op::Out: KeyedItem,
{
    prev: Op,
    fold: F,
    init: O,
    accumulators: HashMap<<Op::Out as KeyedItem>::Key, O, crate::block::GroupHasherBuilder>,
    timestamps: HashMap<<Op::Out as KeyedItem>::Key, Timestamp, crate::block::GroupHasherBuilder>,
    ready: Vec<StreamElement<(<Op::Out as KeyedItem>::Key, O)>>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
    received_end_iter: bool,
}

impl<O: Send + Clone, F: Clone, Op: Clone> Clone for KeyedFold<O, F, Op>
where
    F: Fn(&mut O, <Op::Out as KeyedItem>::Value) + Send + Clone,
    Op: Operator,
    Op::Out: KeyedItem,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            fold: self.fold.clone(),
            init: self.init.clone(),
            accumulators: self.accumulators.clone(),
            timestamps: self.timestamps.clone(),
            ready: self.ready.clone(),
            max_watermark: self.max_watermark,
            received_end: self.received_end,
            received_end_iter: self.received_end_iter,
        }
    }
}

impl<O: Send + Clone, F, Op> Display for KeyedFold<O, F, Op>
where
    F: Fn(&mut O, <Op::Out as KeyedItem>::Value) + Send + Clone,
    Op: Operator,
    Op::Out: KeyedItem,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> KeyedFold<{} -> {}>",
            self.prev,
            std::any::type_name::<Op::Out>(),
            std::any::type_name::<(<Op::Out as KeyedItem>::Key, O)>()
        )
    }
}

impl<O, F, Op> KeyedFold<O, F, Op>
where
    Op::Out: KeyedItem,
    F: Fn(&mut O, <Op::Out as KeyedItem>::Value) + Send + Clone,
    O: Send + Clone,
    Op: Operator,
{
    pub(super) fn new(prev: Op, init: O, fold: F) -> Self {
        KeyedFold {
            prev,
            fold,
            init,
            accumulators: Default::default(),
            timestamps: Default::default(),
            ready: Default::default(),
            max_watermark: None,
            received_end: false,
            received_end_iter: false,
        }
    }

    /// Process a new item, folding it with the accumulator inside the hashmap.
    fn process_item(
        &mut self,
        key: <Op::Out as KeyedItem>::Key,
        value: <Op::Out as KeyedItem>::Value,
    ) {
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

impl<O: Send + Clone, F, Op> Operator for KeyedFold<O, F, Op>
where
    F: Fn(&mut O, <Op::Out as KeyedItem>::Value) + Send + Clone,
    Op: Operator,
    Op::Out: KeyedItem,
{
    type Out = (<Op::Out as KeyedItem>::Key, O);

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Self::Out> {
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
                StreamElement::Item(kv) => {
                    let (k, v) = kv.into_kv();
                    self.process_item(k, v);
                }
                StreamElement::Timestamped(kv, ts) => {
                    let (k, v) = kv.into_kv();
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

        // move all the accumulators into a faster vec
        if !self.accumulators.is_empty() {
            // take a reference to move into the closure, avoiding moving "self"
            let timestamps = &mut self.timestamps;
            self.ready
                .extend(self.accumulators.drain().map(|(key, value)| {
                    if let Some(ts) = timestamps.remove(&key) {
                        StreamElement::Timestamped((key, value), ts)
                    } else {
                        StreamElement::Item((key, value))
                    }
                }));
        }

        // consume the ready items
        if let Some(elem) = self.ready.pop() {
            return elem;
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

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Self::Out, _>("KeyedFold"))
    }
}

#[cfg(test)]
mod tests {
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
                other => panic!("Expecting StreamElement::Item, got {}", other.variant_str()),
            }
        }

        assert_eq!(keyed_fold.next(), StreamElement::Terminate);

        res.sort_unstable();
        assert_eq!(res[0].1, 0 + 2 + 4 + 6 + 8);
        assert_eq!(res[1].1, 1 + 3 + 5 + 7 + 9);
    }

    #[test]
    #[cfg(feature = "timestamp")]
    #[allow(clippy::identity_op)]
    fn test_keyed_fold_timestamp() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Timestamped((0, 0), 1));
        fake_operator.push(StreamElement::Timestamped((1, 1), 2));
        fake_operator.push(StreamElement::Timestamped((0, 2), 3));
        fake_operator.push(StreamElement::Watermark(4));

        let mut keyed_fold = KeyedFold::new(fake_operator, 0, |a, b| *a += b);

        let mut res = vec![];
        for _ in 0..2 {
            let item = keyed_fold.next();
            match item {
                StreamElement::Timestamped(x, ts) => res.push((x, ts)),
                other => panic!(
                    "Expecting StreamElement::Timestamped, got {}",
                    other.variant_str()
                ),
            }
        }

        assert_eq!(keyed_fold.next(), StreamElement::Watermark(4));
        assert_eq!(keyed_fold.next(), StreamElement::Terminate);

        res.sort_unstable();
        assert_eq!(res[0].0 .1, 0 + 2);
        assert_eq!(res[0].1, 3);
        assert_eq!(res[1].0 .1, 1);
        assert_eq!(res[1].1, 2);
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
