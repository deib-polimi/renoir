use core::iter::Iterator;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hasher;
use std::mem::MaybeUninit;
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
    fold: Arc<dyn Fn(NewOut, Out) -> NewOut + Send + Sync>,
    init: NewOut,
    /// For each key store the corresponding partial accumulation.
    ///
    /// ## Safety
    ///
    /// This is a `MaybeUninit` for optimizing the accesses to the hashmap. Inside this hashmap
    /// there could be uninitialized data only during the execution of a fold. After that the data
    /// is immediately initialized back.
    #[derivative(Clone(clone_with = "clone_with_default"))]
    accumulators: HashMap<Key, MaybeUninit<NewOut>>,
    timestamps: HashMap<Key, Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
    received_end_iter: bool,
}

fn clone_with_default<T: Default>(_: &T) -> T {
    T::default()
}

impl<Key: DataKey, Out: Data, NewOut: Data, PreviousOperators: Operator<KeyValue<Key, Out>>>
    KeyedFold<Key, Out, NewOut, PreviousOperators>
{
    fn new<F>(prev: PreviousOperators, init: NewOut, fold: F) -> Self
    where
        F: Fn(NewOut, Out) -> NewOut + Send + Sync + 'static,
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

    /// Process a new item, folding it and putting back the value inside the hashmap.
    fn process_item(&mut self, key: Key, value: Out) {
        let acc = self.accumulators.get_mut(&key);
        match acc {
            Some(acc) => {
                let mut temp = MaybeUninit::uninit();
                std::mem::swap(acc, &mut temp);
                // SAFETY: assuming fold doesn't panic, all the data inside the hashmap
                // is initialized. The swapped uninitialized data won't ever be accessed
                // since it's swapped back immediately after this.
                temp = MaybeUninit::new((self.fold)(unsafe { temp.assume_init() }, value));
                std::mem::swap(acc, &mut temp);
            }
            None => {
                self.accumulators
                    .insert(key, MaybeUninit::new((self.fold)(self.init.clone(), value)));
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
                StreamElement::End => self.received_end = true,
                StreamElement::IterEnd => {
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
                // SAFETY: inside the hashmap there is always initialized data
                StreamElement::Timestamped((key, unsafe { value.assume_init() }), ts)
            } else {
                // SAFETY: inside the hashmap there is always initialized data
                StreamElement::Item((key, unsafe { value.assume_init() }))
            };
        }

        if let Some(ts) = self.max_watermark.take() {
            return StreamElement::Watermark(ts);
        }

        // the end was not really the end... just the end of one iteration!
        if self.received_end_iter {
            self.received_end_iter = false;
            self.received_end = false;
            return StreamElement::IterEnd;
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
        Local: Fn(NewOut, Out) -> NewOut + Send + Sync + 'static,
        Global: Fn(NewOut, NewOut) -> NewOut + Send + Sync + 'static,
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
        F: Fn(NewOut, Out) -> NewOut + Send + Sync + 'static,
    {
        self.add_operator(|prev| KeyedFold::new(prev, init, f))
    }
}

#[cfg(test)]
mod tests {
    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[test]
    fn group_by_fold_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by_fold(
                |n| n % 2,
                "".to_string(),
                |s, n| s + &n.to_string(),
                |s1, s2| s1 + &s2,
            )
            .unkey()
            .collect_vec();
        env.execute();
        let mut res = res.get().unwrap();
        res.sort();
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].1, "02468");
        assert_eq!(res[1].1, "13579");
    }

    #[test]
    fn fold_keyed_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|n| n % 2)
            .fold("".to_string(), |s, n| s + &n.to_string())
            .unkey()
            .collect_vec();
        env.execute();
        let mut res = res.get().unwrap();
        res.sort();
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].1, "02468");
        assert_eq!(res[1].1, "13579");
    }

    #[test]
    fn group_by_fold_shuffled_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .shuffle()
            .group_by_fold(
                |n| n % 2,
                Vec::new(),
                |mut v, n| {
                    v.push(n);
                    v
                },
                |mut v1, mut v2| {
                    v1.append(&mut v2);
                    v1
                },
            )
            .unkey()
            .collect_vec();
        env.execute();

        let mut res = res.get().unwrap();
        res.sort_unstable();
        res[0].1.sort_unstable();
        res[1].1.sort_unstable();
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].1, &[0, 2, 4, 6, 8]);
        assert_eq!(res[1].1, &[1, 3, 5, 7, 9]);
    }

    #[test]
    fn fold_shuffled_keyed_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .shuffle()
            .group_by(|n| n % 2)
            .fold(Vec::new(), |mut v, n| {
                v.push(n);
                v
            })
            .unkey()
            .collect_vec();
        env.execute();
        let mut res = res.get().unwrap();

        res.sort_unstable();
        res[0].1.sort_unstable();
        res[1].1.sort_unstable();
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].1, &[0, 2, 4, 6, 8]);
        assert_eq!(res[1].1, &[1, 3, 5, 7, 9]);
    }
}
