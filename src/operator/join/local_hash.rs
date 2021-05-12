#![allow(clippy::type_complexity)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::Coord;
use crate::operator::join::ship::{ShipBroadcastRight, ShipHash, ShipStrategy};
use crate::operator::join::start::{JoinElement, JoinStartBlock};
use crate::operator::{
    DataKey, ExchangeData, InnerJoinTuple, JoinVariant, KeyerFn, LeftJoinTuple, Operator,
    OuterJoinTuple, StreamElement,
};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

/// This type keeps the elements of a side of the join.
#[derive(Debug, Clone)]
struct SideHashMap<Key: DataKey, Out> {
    /// The actual items on this side, grouped by key.
    ///
    /// Note that when the other side ends this map is emptied.
    data: HashMap<Key, Vec<Out>>,
    /// The set of all the keys seen.
    ///
    /// Note that when this side ends this set is emptied since it won't be used again.
    keys: HashSet<Key>,
    /// Whether this side has ended.
    ended: bool,
}

impl<Key: DataKey, Out> Default for SideHashMap<Key, Out> {
    fn default() -> Self {
        Self {
            data: Default::default(),
            keys: Default::default(),
            ended: false,
        }
    }
}

/// This operator performs the join using the local hash strategy.
///
/// The previous operator should be a `JoinStartBlock` that emits the `JoinElement` of the two
/// incoming streams.
///
/// This operator is able to produce the outer join tuples (the most general type of join), but it
/// can be asked to skip generating the `None` tuples if the join was actually inner.
#[derive(Clone, Debug)]
struct JoinLocalHash<
    Key: DataKey,
    Out1: ExchangeData,
    Out2: ExchangeData,
    OperatorChain: Operator<JoinElement<Key, Out1, Out2>>,
> {
    prev: OperatorChain,
    coord: Coord,

    /// The content of the left side.
    left: SideHashMap<Key, Out1>,
    /// The content of the right side.
    right: SideHashMap<Key, Out2>,
    /// The variant of join to build.
    ///
    /// This is used for optimizing the behaviour in case of inner and left joins, avoiding to
    /// generate useless tuples.
    variant: JoinVariant,
    /// The already generated tuples, but not yet returned.
    buffer: VecDeque<KeyValue<Key, OuterJoinTuple<Out1, Out2>>>,
}

impl<
        Key: DataKey,
        Out1: ExchangeData,
        Out2: ExchangeData,
        OperatorChain: Operator<JoinElement<Key, Out1, Out2>>,
    > JoinLocalHash<Key, Out1, Out2, OperatorChain>
{
    fn new(prev: OperatorChain, variant: JoinVariant) -> Self {
        Self {
            prev,
            coord: Default::default(),
            left: Default::default(),
            right: Default::default(),
            variant,
            buffer: Default::default(),
        }
    }

    /// Add a new item on the _left_ side, storing the newly generated tuples inside the buffer.
    ///
    /// This can be used to add _right_ tuples by swapping left and right parameters.
    fn add_item<OutL: ExchangeData, OutR: ExchangeData>(
        (key, item): (Key, OutL),
        left: &mut SideHashMap<Key, OutL>,
        right: &mut SideHashMap<Key, OutR>,
        left_outer: bool,
        right_outer: bool,
        buffer: &mut VecDeque<(Key, OuterJoinTuple<Out1, Out2>)>,
        make_pair: impl Fn(Option<OutL>, Option<OutR>) -> OuterJoinTuple<Out1, Out2>,
    ) {
        if let Some(right) = right.data.get(&key) {
            // the left item has at least one right matching element
            for rhs in right {
                buffer.push_back((
                    key.clone(),
                    make_pair(Some(item.clone()), Some(rhs.clone())),
                ));
            }
        } else if right.ended && left_outer {
            // if the left item has no right correspondent, but the right has already ended
            // we might need to generate the outer tuple.
            buffer.push_back((key.clone(), make_pair(Some(item.clone()), None)));
        } else {
            // either the rhs is not ended (so we cannot generate anything for now), or
            // it's left inner, so we cannot generate left-outer tuples.
        }
        if right_outer {
            left.keys.insert(key.clone());
        }
        if !right.ended {
            left.data.entry(key).or_default().push(item);
        }
    }

    /// Mark the left side as ended, generating all the remaining tuples if the join is outer.
    ///
    /// This can be used to mark also the right side by swapping the parameters.
    fn side_ended<OutL, OutR>(
        right_outer: bool,
        left: &mut SideHashMap<Key, OutL>,
        right: &mut SideHashMap<Key, OutR>,
        buffer: &mut VecDeque<(Key, OuterJoinTuple<Out1, Out2>)>,
        make_pair: impl Fn(Option<OutL>, Option<OutR>) -> OuterJoinTuple<Out1, Out2>,
    ) {
        if right_outer {
            // left ended and this is a right-outer, so we need to generate (None, Some)
            // tuples. For each value on the right side, before dropping the right hashmap,
            // search if there was already a match.
            for (key, right) in right.data.drain() {
                if !left.keys.contains(&key) {
                    for rhs in right {
                        buffer.push_back((key.clone(), make_pair(None, Some(rhs))));
                    }
                }
            }
        } else {
            // in any case, we won't need the right hashmap anymore.
            right.data.clear();
        }
        // we will never look at it, and nothing will be inserted, drop it freeing some memory.
        left.keys.clear();
        left.ended = true;
    }
}

impl<
        Key: DataKey,
        Out1: ExchangeData,
        Out2: ExchangeData,
        OperatorChain: Operator<JoinElement<Key, Out1, Out2>>,
    > Operator<KeyValue<Key, OuterJoinTuple<Out1, Out2>>>
    for JoinLocalHash<Key, Out1, Out2, OperatorChain>
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.coord = metadata.coord;
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<(Key, OuterJoinTuple<Out1, Out2>)> {
        if let Some(item) = self.buffer.pop_front() {
            return StreamElement::Item(item);
        }

        match self.prev.next() {
            StreamElement::Item(JoinElement::Left(item)) => Self::add_item(
                item,
                &mut self.left,
                &mut self.right,
                self.variant.left_outer(),
                self.variant.right_outer(),
                &mut self.buffer,
                |x, y| (x, y),
            ),
            StreamElement::Item(JoinElement::Right(item)) => Self::add_item(
                item,
                &mut self.right,
                &mut self.left,
                self.variant.right_outer(),
                self.variant.left_outer(),
                &mut self.buffer,
                |x, y| (y, x),
            ),
            StreamElement::Item(JoinElement::LeftEnd) => Self::side_ended(
                self.variant.right_outer(),
                &mut self.left,
                &mut self.right,
                &mut self.buffer,
                |x, y| (x, y),
            ),
            StreamElement::Item(JoinElement::RightEnd) => Self::side_ended(
                self.variant.left_outer(),
                &mut self.right,
                &mut self.left,
                &mut self.buffer,
                |x, y| (y, x),
            ),
            StreamElement::FlushAndRestart => {
                assert!(self.left.ended);
                assert!(self.right.ended);
                assert!(self.left.data.is_empty());
                assert!(self.right.data.is_empty());
                assert!(self.left.keys.is_empty());
                assert!(self.right.keys.is_empty());
                self.left.ended = false;
                self.right.ended = false;
                debug!("JoinLocalHash at {} emitted FlushAndRestart", self.coord);
                return StreamElement::FlushAndRestart;
            }
            StreamElement::Terminate => return StreamElement::Terminate,
            StreamElement::FlushBatch => return StreamElement::FlushBatch,
            StreamElement::Watermark(_) | StreamElement::Timestamped(_, _) => {
                panic!("Cannot yet join timestamped streams")
            }
        }
        self.next()
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> JoinLocalHash<{}>",
            self.prev.to_string(),
            std::any::type_name::<Key>()
        )
    }

    fn structure(&self) -> BlockStructure {
        self.prev.structure().add_operator(OperatorStructure::new::<
            KeyValue<Key, OuterJoinTuple<Out1, Out2>>,
            _,
        >("JoinLocalHash"))
    }
}

/// This is an intermediate type for building a join operator.
///
/// The ship strategy has already been selected and it's stored in `ShipStrat`, the local strategy
/// is hash and now the join variant has to be selected.
///
/// Note that `outer` join is not supported if the ship strategy is `broadcast_right`.
pub struct JoinStreamLocalHash<
    Key: DataKey,
    Out1: ExchangeData,
    Out2: ExchangeData,
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
    ShipStrat: ShipStrategy,
> {
    stream: Stream<JoinElement<Key, Out1, Out2>, JoinStartBlock<Key, Out1, Out2, Keyer1, Keyer2>>,
    _s: PhantomData<ShipStrat>,
}

impl<
        Key: DataKey,
        Out1: ExchangeData,
        Out2: ExchangeData,
        Keyer1,
        Keyer2,
        ShipStrat: ShipStrategy,
    > JoinStreamLocalHash<Key, Out1, Out2, Keyer1, Keyer2, ShipStrat>
where
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    pub(crate) fn new(
        stream: Stream<
            JoinElement<Key, Out1, Out2>,
            JoinStartBlock<Key, Out1, Out2, Keyer1, Keyer2>,
        >,
    ) -> Self {
        Self {
            stream,
            _s: Default::default(),
        }
    }
}

impl<Key: DataKey, Out1: ExchangeData, Out2: ExchangeData, Keyer1, Keyer2>
    JoinStreamLocalHash<Key, Out1, Out2, Keyer1, Keyer2, ShipHash>
where
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    pub fn inner(
        self,
    ) -> KeyedStream<
        Key,
        InnerJoinTuple<Out1, Out2>,
        impl Operator<KeyValue<Key, InnerJoinTuple<Out1, Out2>>>,
    > {
        let inner = self
            .stream
            .add_operator(|prev| JoinLocalHash::new(prev, JoinVariant::Inner));
        KeyedStream(inner).map(|(_key, (lhs, rhs))| (lhs.unwrap(), rhs.unwrap()))
    }

    pub fn left(
        self,
    ) -> KeyedStream<
        Key,
        LeftJoinTuple<Out1, Out2>,
        impl Operator<KeyValue<Key, LeftJoinTuple<Out1, Out2>>>,
    > {
        let inner = self
            .stream
            .add_operator(|prev| JoinLocalHash::new(prev, JoinVariant::Left));
        KeyedStream(inner).map(|(_key, (lhs, rhs))| (lhs.unwrap(), rhs))
    }

    pub fn outer(
        self,
    ) -> KeyedStream<
        Key,
        OuterJoinTuple<Out1, Out2>,
        impl Operator<KeyValue<Key, OuterJoinTuple<Out1, Out2>>>,
    > {
        let inner = self
            .stream
            .add_operator(|prev| JoinLocalHash::new(prev, JoinVariant::Outer));
        KeyedStream(inner)
    }
}

impl<Key: DataKey, Out1: ExchangeData, Out2: ExchangeData, Keyer1, Keyer2>
    JoinStreamLocalHash<Key, Out1, Out2, Keyer1, Keyer2, ShipBroadcastRight>
where
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    pub fn inner(
        self,
    ) -> Stream<
        KeyValue<Key, InnerJoinTuple<Out1, Out2>>,
        impl Operator<KeyValue<Key, InnerJoinTuple<Out1, Out2>>>,
    > {
        self.stream
            .add_operator(|prev| JoinLocalHash::new(prev, JoinVariant::Inner))
            .map(|(key, (lhs, rhs))| (key, (lhs.unwrap(), rhs.unwrap())))
    }

    pub fn left(
        self,
    ) -> Stream<
        KeyValue<Key, LeftJoinTuple<Out1, Out2>>,
        impl Operator<KeyValue<Key, LeftJoinTuple<Out1, Out2>>>,
    > {
        self.stream
            .add_operator(|prev| JoinLocalHash::new(prev, JoinVariant::Left))
            .map(|(key, (lhs, rhs))| (key, (lhs.unwrap(), rhs)))
    }
}
