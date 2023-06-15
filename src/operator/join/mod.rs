//! Structures for building the join operators.
//!
//! The actual operators are [`Stream::join`], [`Stream::left_join`], [`Stream::outer_join`] and
//! [`Stream::join_with`].

use std::marker::PhantomData;

pub use local_hash::JoinStreamLocalHash;
pub use local_sort_merge::JoinStreamLocalSortMerge;
pub use ship::{ShipBroadcastRight, ShipHash, ShipStrategy};

pub use crate::operator::join::ship::{JoinStreamShipBroadcastRight, JoinStreamShipHash};
use crate::operator::{Data, DataKey, ExchangeData, KeyerFn, Operator};
use crate::stream::{KeyedStream, Stream};

mod keyed_join;
mod local_hash;
mod local_sort_merge;
mod ship;

/// Type alias for a pair of joined items in an inner join.
pub type InnerJoinTuple<Out1, Out2> = (Out1, Out2);
/// Type alias for a pair of joined items in a left join.
pub type LeftJoinTuple<Out1, Out2> = (Out1, Option<Out2>);
/// Type alias for a pair of joined items in an outer join.
pub type OuterJoinTuple<Out1, Out2> = (Option<Out1>, Option<Out2>);

/// The variant of the join, either a inner, a left or a full outer join.
#[derive(Clone, Debug)]
pub(crate) enum JoinVariant {
    /// The join is full inner.
    Inner,
    /// The join is a left outer join..
    ///
    /// This means that all the left elements will appear at least once in the output.
    Left,
    /// The join is full outer.
    ///
    /// This means that all the elements will appear in at least one output tuple.
    Outer,
}

impl JoinVariant {
    /// Whether this variant is left outer (either left or full outer).
    pub(crate) fn left_outer(&self) -> bool {
        matches!(self, JoinVariant::Left | JoinVariant::Outer)
    }

    /// Whether this variant is right outer (i.e. full outer since we don't support right join).
    pub(crate) fn right_outer(&self) -> bool {
        matches!(self, JoinVariant::Outer)
    }
}

/// Intermediate stream type for building the join between two streams.
///
/// This type has methods for selecting the ship strategy of the join, later you will be able to
/// select the local strategy, and finally the variant of the join.
pub struct JoinStream<
    Key,
    Out1: ExchangeData,
    Out2: ExchangeData,
    OperatorChain1,
    OperatorChain2,
    Keyer1,
    Keyer2,
> where
    OperatorChain1: Operator<Out1>,
    OperatorChain2: Operator<Out2>,
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    /// The stream of the left side.
    pub(crate) lhs: Stream<Out1, OperatorChain1>,
    /// The stream of the right side.
    pub(crate) rhs: Stream<Out2, OperatorChain2>,
    /// The function for extracting the join key from the left stream.
    pub(crate) keyer1: Keyer1,
    /// The function for extracting the join key from the right stream.
    pub(crate) keyer2: Keyer2,

    _key: PhantomData<Key>,
}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Given two stream, create a stream with all the pairs (left item from the left stream, right
    /// item from the right), such that the key obtained with `keyer1` on an item from the left is
    /// equal to the key obtained with `keyer2` on an item from the right.
    ///
    /// This is an inner join, very similar to `SELECT a, b FROM a JOIN b ON keyer1(a) = keyer2(b)`.
    ///
    /// This is a shortcut for: `self.join_with(...).ship_hash().local_hash().inner()`.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s1 = env.stream(IteratorSource::new(0..5u8));
    /// let s2 = env.stream(IteratorSource::new(0..5i32));
    /// let res = s1.join(s2, |n| (n % 5) as i32, |n| n % 2).drop_key().collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (0, 2), (0, 4), (1, 1), (1, 3)]);
    /// ```
    pub fn join<Out2: ExchangeData, OperatorChain2, Key, Keyer1, Keyer2>(
        self,
        rhs: Stream<Out2, OperatorChain2>,
        keyer1: Keyer1,
        keyer2: Keyer2,
    ) -> KeyedStream<Key, InnerJoinTuple<Out, Out2>, impl Operator<(Key, InnerJoinTuple<Out, Out2>)>>
    where
        Key: DataKey,
        OperatorChain2: Operator<Out2> + 'static,
        Keyer1: Fn(&Out) -> Key + KeyerFn<Key, Out>,
        Keyer2: Fn(&Out2) -> Key + KeyerFn<Key, Out2>,
    {
        self.join_with(rhs, keyer1, keyer2)
            .ship_hash()
            .local_hash()
            .inner()
    }

    /// Given two stream, create a stream with all the pairs (left item from the left stream, right
    /// item from the right), such that the key obtained with `keyer1` on an item from the left is
    /// equal to the key obtained with `keyer2` on an item from the right.
    ///
    /// This is a **left** join, meaning that if an item from the left does not find and element
    /// from the right with which make a pair, an extra pair `(left, None)` is generated. If you
    /// want to have a _right_ join, you just need to switch the two sides and use a left join.
    ///
    /// This is very similar to `SELECT a, b FROM a LEFT JOIN b ON keyer1(a) = keyer2(b)`.
    ///
    /// This is a shortcut for: `self.join_with(...).ship_hash().local_hash().left()`.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s1 = env.stream(IteratorSource::new(0..5u8));
    /// let s2 = env.stream(IteratorSource::new(0..5i32));
    /// let res = s1.left_join(s2, |n| (n % 5) as i32, |n| n % 2).drop_key().collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, Some(0)), (0, Some(2)), (0, Some(4)), (1, Some(1)), (1, Some(3)), (2, None), (3, None), (4, None)]);
    /// ```
    pub fn left_join<Out2: ExchangeData, OperatorChain2, Key, Keyer1, Keyer2>(
        self,
        rhs: Stream<Out2, OperatorChain2>,
        keyer1: Keyer1,
        keyer2: Keyer2,
    ) -> KeyedStream<Key, LeftJoinTuple<Out, Out2>, impl Operator<(Key, LeftJoinTuple<Out, Out2>)>>
    where
        Key: DataKey,
        OperatorChain2: Operator<Out2> + 'static,
        Keyer1: Fn(&Out) -> Key + KeyerFn<Key, Out>,
        Keyer2: Fn(&Out2) -> Key + KeyerFn<Key, Out2>,
    {
        self.join_with(rhs, keyer1, keyer2)
            .ship_hash()
            .local_hash()
            .left()
    }

    /// Given two stream, create a stream with all the pairs (left item from the left stream, right
    /// item from the right), such that the key obtained with `keyer1` on an item from the left is
    /// equal to the key obtained with `keyer2` on an item from the right.
    ///
    /// This is a **full-outer** join, meaning that if an item from the left does not find and element
    /// from the right with which make a pair, an extra pair `(left, None)` is generated. Similarly
    /// if an element from the right does not appear in any pair, a new one is generated with
    /// `(None, right)`.
    ///
    /// This is very similar to `SELECT a, b FROM a FULL OUTER JOIN b ON keyer1(a) = keyer2(b)`.
    ///
    /// This is a shortcut for: `self.join_with(...).ship_hash().local_hash().outer()`.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s1 = env.stream(IteratorSource::new(0..5u8));
    /// let s2 = env.stream(IteratorSource::new(0..5i32));
    /// let res = s1.outer_join(s2, |n| (n % 5) as i32, |n| n % 2).drop_key().collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(Some(0), Some(0)), (Some(0), Some(2)), (Some(0), Some(4)), (Some(1), Some(1)), (Some(1), Some(3)), (Some(2), None), (Some(3), None), (Some(4), None)]);
    /// ```
    pub fn outer_join<Out2: ExchangeData, OperatorChain2, Key, Keyer1, Keyer2>(
        self,
        rhs: Stream<Out2, OperatorChain2>,
        keyer1: Keyer1,
        keyer2: Keyer2,
    ) -> KeyedStream<Key, OuterJoinTuple<Out, Out2>, impl Operator<(Key, OuterJoinTuple<Out, Out2>)>>
    where
        Key: DataKey,
        OperatorChain2: Operator<Out2> + 'static,
        Keyer1: Fn(&Out) -> Key + KeyerFn<Key, Out>,
        Keyer2: Fn(&Out2) -> Key + KeyerFn<Key, Out2>,
    {
        self.join_with(rhs, keyer1, keyer2)
            .ship_hash()
            .local_hash()
            .outer()
    }

    /// Given two streams, start building a join operator.
    ///
    /// The returned type allows you to customize the behaviour of the join. You can select which
    /// ship strategy and which local strategy to use.
    ///
    /// **Ship strategies**
    ///
    /// - _hash_: the hash of the key is used to select where to send the elements
    /// - _broadcast right_: the left stream is left locally, while the right stream is broadcasted
    ///
    /// **Local strategies**
    ///
    /// - _hash_: build an hashmap to match the tuples
    /// - _sort and merge_: collect all the tuples, sort them by key and merge them
    ///
    /// The first strategy to select is the _ship strategy_. After choosing that you have to select
    /// the local strategy.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s1 = env.stream(IteratorSource::new(0..5u8));
    /// let s2 = env.stream(IteratorSource::new(0..5i32));
    /// let j = s1.join_with(s2, |n| (n % 5) as i32, |n| n % 2).ship_hash();
    /// ```
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s1 = env.stream(IteratorSource::new(0..5u8));
    /// let s2 = env.stream(IteratorSource::new(0..5i32));
    /// let j = s1.join_with(s2, |n| (n % 5) as i32, |n| n % 2).ship_broadcast_right();
    /// ```
    pub fn join_with<Out2: ExchangeData, OperatorChain2, Key, Keyer1, Keyer2>(
        self,
        rhs: Stream<Out2, OperatorChain2>,
        keyer1: Keyer1,
        keyer2: Keyer2,
    ) -> JoinStream<Key, Out, Out2, OperatorChain, OperatorChain2, Keyer1, Keyer2>
    where
        OperatorChain2: Operator<Out2>,
        Keyer1: Fn(&Out) -> Key + KeyerFn<Key, Out>,
        Keyer2: Fn(&Out2) -> Key + KeyerFn<Key, Out2>,
    {
        JoinStream {
            lhs: self,
            rhs,
            keyer1,
            keyer2,
            _key: PhantomData::default(),
        }
    }
}

impl<
        Key: Data,
        Out1: ExchangeData,
        Out2: ExchangeData,
        OperatorChain1,
        OperatorChain2,
        Keyer1,
        Keyer2,
    > JoinStream<Key, Out1, Out2, OperatorChain1, OperatorChain2, Keyer1, Keyer2>
where
    OperatorChain1: Operator<Out1> + 'static,
    OperatorChain2: Operator<Out2> + 'static,
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    /// Use the Hash Repartition strategy.
    ///
    /// With this strategy the two streams are shuffled (like a group-by), pointing the message with
    /// the same key to the same replica. The key must be hashable.
    pub fn ship_hash(self) -> JoinStreamShipHash<Key, Out1, Out2, Keyer1, Keyer2>
    where
        Key: DataKey,
    {
        JoinStreamShipHash::new(self)
    }

    /// Use the Broadcast-Forward strategy.
    ///
    /// The left side won't be sent to the network, while the right side is broadcasted. This is
    /// recommended when the left side is really big and the left side really small.
    ///
    /// This does not require the key to be hashable.
    pub fn ship_broadcast_right(
        self,
    ) -> JoinStreamShipBroadcastRight<Key, Out1, Out2, Keyer1, Keyer2> {
        JoinStreamShipBroadcastRight::new(self)
    }
}
