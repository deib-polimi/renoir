use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::{Debug, Display},
    marker::PhantomData,
};

use crate::{
    block::{NextStrategy, OperatorStructure},
    network::Coord,
    operator::{
        BinaryElement, BinaryStartOperator, Data, DataKey, ExchangeData, Operator, Start,
        StreamElement,
    },
    KeyedStream,
};

use super::{InnerJoinTuple, JoinVariant, OuterJoinTuple};

type BinaryTuple<K, V1, V2> = BinaryElement<(K, V1), (K, V2)>;

/// This type keeps the elements of a side of the join.
#[derive(Debug, Clone)]
struct SideHashMap<Key: DataKey, Out> {
    /// The actual items on this side, grouped by key.
    ///
    /// Note that when the other side ends this map is emptied.
    data: HashMap<Key, Vec<Out>, crate::block::GroupHasherBuilder>,
    /// The set of all the keys seen.
    ///
    /// Note that when this side ends this set is emptied since it won't be used again.
    keys: HashSet<Key>,
    /// Whether this side has ended.
    ended: bool,
    /// The number of items received.
    count: usize,
}

impl<Key: DataKey, Out> Default for SideHashMap<Key, Out> {
    fn default() -> Self {
        Self {
            data: Default::default(),
            keys: Default::default(),
            ended: false,
            count: 0,
        }
    }
}

#[derive(Clone)]
struct JoinKeyedOuter<K: DataKey + ExchangeData, V1: ExchangeData, V2: ExchangeData> {
    prev: BinaryStartOperator<(K, V1), (K, V2)>,
    variant: JoinVariant,
    _k: PhantomData<K>,
    _v1: PhantomData<V1>,
    _v2: PhantomData<V2>,
    coord: Option<Coord>,

    /// The content of the left side.
    left: SideHashMap<K, V1>,
    /// The content of the right side.
    right: SideHashMap<K, V2>,

    buffer: VecDeque<(K, OuterJoinTuple<V1, V2>)>,
}

impl<K: DataKey + ExchangeData, V1: ExchangeData, V2: ExchangeData> JoinKeyedOuter<K, V1, V2> {
    pub(crate) fn new(prev: BinaryStartOperator<(K, V1), (K, V2)>, variant: JoinVariant) -> Self {
        JoinKeyedOuter {
            prev,
            variant,
            _k: PhantomData,
            _v1: PhantomData,
            _v2: PhantomData,
            coord: Default::default(),
            left: Default::default(),
            right: Default::default(),
            buffer: Default::default(),
        }
    }

    fn process_item(&mut self, item: BinaryTuple<K, V1, V2>) {
        let left_outer = self.variant.left_outer();
        let right_outer = self.variant.right_outer();
        match item {
            BinaryElement::Left((key, v1)) => {
                self.left.count += 1;
                if let Some(right) = self.right.data.get(&key) {
                    // the left item has at least one right matching element
                    for v2 in right {
                        self.buffer
                            .push_back((key.clone(), (Some(v1.clone()), Some(v2.clone()))));
                    }
                } else if self.right.ended && left_outer {
                    // if the left item has no right correspondent, but the right has already ended
                    // we might need to generate the outer tuple.
                    self.buffer
                        .push_back((key.clone(), (Some(v1.clone()), None)));
                }
                if right_outer {
                    self.left.keys.insert(key.clone());
                }
                if !self.right.ended {
                    self.left.data.entry(key).or_default().push(v1);
                }
            }
            BinaryElement::Right((key, v2)) => {
                self.right.count += 1;
                if let Some(left) = self.left.data.get(&key) {
                    // the left item has at least one right matching element
                    for v1 in left {
                        self.buffer
                            .push_back((key.clone(), (Some(v1.clone()), Some(v2.clone()))));
                    }
                } else if self.left.ended && right_outer {
                    // if the left item has no right correspondent, but the right has already ended
                    // we might need to generate the outer tuple.
                    self.buffer
                        .push_back((key.clone(), (None, Some(v2.clone()))));
                }
                if left_outer {
                    self.right.keys.insert(key.clone());
                }
                if !self.left.ended {
                    self.right.data.entry(key).or_default().push(v2);
                }
            }
            BinaryElement::LeftEnd => {
                log::debug!(
                    "Left side of join ended with {} elements on the left \
                    and {} elements on the right",
                    self.left.count,
                    self.right.count
                );
                if right_outer {
                    // left ended and this is a right-outer, so we need to generate (None, Some)
                    // tuples. For each value on the right side, before dropping the right hashmap,
                    // search if there was already a match.
                    for (key, right) in self.right.data.drain() {
                        if !self.left.keys.contains(&key) {
                            for v2 in right {
                                self.buffer.push_back((key.clone(), (None, Some(v2))));
                            }
                        }
                    }
                } else {
                    // in any case, we won't need the right hashmap anymore.
                    self.right.data.clear();
                }
                // we will never look at it, and nothing will be inserted, drop it freeing some memory.
                self.left.keys.clear();
                self.left.ended = true;
            }
            BinaryElement::RightEnd => {
                log::debug!(
                    "Right side of join ended with {} elements on the left \
                    and {} elements on the right",
                    self.left.count,
                    self.right.count
                );
                if left_outer {
                    // right ended and this is a left-outer, so we need to generate (None, Some)
                    // tuples. For each value on the left side, before dropping the left hashmap,
                    // search if there was already a match.
                    for (key, left) in self.left.data.drain() {
                        if !self.right.keys.contains(&key) {
                            for v1 in left {
                                self.buffer.push_back((key.clone(), (Some(v1), None)));
                            }
                        }
                    }
                } else {
                    // in any case, we won't need the left hashmap anymore.
                    self.left.data.clear();
                }
                // we will never look at it, and nothing will be inserted, drop it freeing some memory.
                self.right.keys.clear();
                self.right.ended = true;
            }
        }
    }
}

impl<K: DataKey + ExchangeData, V1: ExchangeData, V2: ExchangeData> Display
    for JoinKeyedOuter<K, V1, V2>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> JoinKeyed<{},{},{}>",
            self.prev,
            std::any::type_name::<K>(),
            std::any::type_name::<V1>(),
            std::any::type_name::<V2>(),
        )
    }
}

impl<K: DataKey + ExchangeData, V1: ExchangeData, V2: ExchangeData> Operator
    for JoinKeyedOuter<K, V1, V2>
{
    type Out = (K, OuterJoinTuple<V1, V2>);

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.prev.setup(metadata);
        self.coord = Some(metadata.coord);
    }

    fn next(&mut self) -> crate::operator::StreamElement<(K, OuterJoinTuple<V1, V2>)> {
        while self.buffer.is_empty() {
            match self.prev.next() {
                StreamElement::Item(el) => self.process_item(el),
                StreamElement::FlushAndRestart => {
                    assert!(self.left.ended);
                    assert!(self.right.ended);
                    assert!(self.left.data.is_empty());
                    assert!(self.right.data.is_empty());
                    assert!(self.left.keys.is_empty());
                    assert!(self.right.keys.is_empty());
                    self.left.ended = false;
                    self.left.count = 0;
                    self.right.ended = false;
                    self.right.count = 0;
                    log::debug!(
                        "JoinLocalHash at {} emitted FlushAndRestart",
                        self.coord.unwrap()
                    );
                    return StreamElement::FlushAndRestart;
                }
                StreamElement::Terminate => return StreamElement::Terminate,
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Watermark(_) | StreamElement::Timestamped(_, _) => {
                    panic!("Cannot yet join timestamped streams")
                }
            }
        }

        let item = self.buffer.pop_front().unwrap();
        StreamElement::Item(item)
    }

    fn structure(&self) -> crate::block::BlockStructure {
        self.prev.structure().add_operator(
            OperatorStructure::new::<(K, InnerJoinTuple<V1, V2>), _>("JoinKeyed"),
        )
    }
}

#[derive(Clone)]
struct JoinKeyedInner<K: DataKey + ExchangeData, V1: ExchangeData, V2: ExchangeData> {
    prev: BinaryStartOperator<(K, V1), (K, V2)>,
    _k: PhantomData<K>,
    _v1: PhantomData<V1>,
    _v2: PhantomData<V2>,
    coord: Option<Coord>,

    /// The content of the left side.
    left: HashMap<K, Vec<V1>, crate::block::CoordHasherBuilder>,
    /// The content of the right side.
    right: HashMap<K, Vec<V2>, crate::block::CoordHasherBuilder>,

    left_ended: bool,
    right_ended: bool,

    buffer: VecDeque<(K, InnerJoinTuple<V1, V2>)>,
}

impl<K: DataKey + ExchangeData, V1: ExchangeData, V2: ExchangeData> Display
    for JoinKeyedInner<K, V1, V2>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> JoinKeyedInner<{},{},{}>",
            self.prev,
            std::any::type_name::<K>(),
            std::any::type_name::<V1>(),
            std::any::type_name::<V2>(),
        )
    }
}

impl<K: DataKey + ExchangeData + Debug, V1: ExchangeData + Debug, V2: ExchangeData + Debug>
    JoinKeyedInner<K, V1, V2>
{
    pub(crate) fn new(prev: BinaryStartOperator<(K, V1), (K, V2)>) -> Self {
        JoinKeyedInner {
            prev,
            _k: PhantomData,
            _v1: PhantomData,
            _v2: PhantomData,
            coord: Default::default(),
            left: Default::default(),
            right: Default::default(),
            buffer: Default::default(),
            left_ended: false,
            right_ended: false,
        }
    }

    fn process_item(&mut self, item: BinaryTuple<K, V1, V2>) {
        match item {
            BinaryElement::Left((key, v1)) => {
                if let Some(right) = self.right.get(&key) {
                    // the left item has at least one right matching element
                    for v2 in right {
                        self.buffer
                            .push_back((key.clone(), (v1.clone(), v2.clone())));
                    }
                }
                self.left.entry(key).or_default().push(v1);
            }
            BinaryElement::Right((key, v2)) => {
                if let Some(left) = self.left.get(&key) {
                    // the left item has at least one right matching element
                    for v1 in left {
                        self.buffer
                            .push_back((key.clone(), (v1.clone(), v2.clone())));
                    }
                }
                self.right.entry(key).or_default().push(v2);
            }
            BinaryElement::LeftEnd => {
                self.left_ended = true;
                self.right.clear();
                if self.right_ended {
                    self.left.clear();
                    self.right.clear();
                }
            }
            BinaryElement::RightEnd => {
                self.right_ended = true;
                self.left.clear();
                if self.left_ended {
                    self.left.clear();
                    self.right.clear();
                }
            }
        }
    }
}

impl<K: DataKey + ExchangeData + Debug, V1: ExchangeData + Debug, V2: ExchangeData + Debug> Operator
    for JoinKeyedInner<K, V1, V2>
{
    type Out = (K, InnerJoinTuple<V1, V2>);

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.coord = Some(metadata.coord);
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> crate::operator::StreamElement<(K, InnerJoinTuple<V1, V2>)> {
        while self.buffer.is_empty() {
            match self.prev.next() {
                StreamElement::Item(el) => self.process_item(el),
                StreamElement::FlushAndRestart => {
                    assert!(self.left.is_empty());
                    assert!(self.right.is_empty());
                    log::debug!(
                        "JoinLocalHash at {} emitted FlushAndRestart",
                        self.coord.unwrap()
                    );
                    self.left_ended = false;
                    self.right_ended = false;
                    return StreamElement::FlushAndRestart;
                }
                StreamElement::Terminate => return StreamElement::Terminate,
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Watermark(_) | StreamElement::Timestamped(_, _) => {
                    panic!("Cannot yet join timestamped streams")
                }
            }
        }

        let item = self.buffer.pop_front().unwrap();
        StreamElement::Item(item)
    }

    fn structure(&self) -> crate::block::BlockStructure {
        self.prev.structure().add_operator(
            OperatorStructure::new::<(K, InnerJoinTuple<V1, V2>), _>("JoinKeyed"),
        )
    }
}

impl<K: DataKey + ExchangeData + Debug, V1: Data + ExchangeData + Debug, O1> KeyedStream<O1>
where
    O1: Operator<Out = (K, V1)> + 'static,
{
    pub fn join_outer<V2: Data + ExchangeData + Debug, O2>(
        self,
        rhs: KeyedStream<O2>,
    ) -> KeyedStream<impl Operator<Out = (K, (Option<V1>, Option<V2>))>>
    where
        O2: Operator<Out = (K, V2)> + 'static,
    {
        let next_strategy1 = NextStrategy::only_one();
        let next_strategy2 = NextStrategy::only_one();

        let inner =
            self.0
                .binary_connection(rhs.0, Start::multiple, next_strategy1, next_strategy2);

        let s = inner.add_operator(move |prev| JoinKeyedOuter::new(prev, JoinVariant::Outer));
        KeyedStream(s)
    }

    pub fn join<V2: Data + ExchangeData + Debug, O2>(
        self,
        rhs: KeyedStream<O2>,
    ) -> KeyedStream<impl Operator<Out = (K, (V1, V2))>>
    where
        O2: Operator<Out = (K, V2)> + 'static,
    {
        let next_strategy1 = NextStrategy::only_one();
        let next_strategy2 = NextStrategy::only_one();

        let inner =
            self.0
                .binary_connection(rhs.0, Start::multiple, next_strategy1, next_strategy2);

        let s = inner.add_operator(move |prev| JoinKeyedInner::new(prev));
        KeyedStream(s)
    }
}
