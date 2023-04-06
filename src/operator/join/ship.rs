#![allow(clippy::type_complexity)]

use std::marker::PhantomData;

use crate::block::NextStrategy;
use crate::operator::join::local_hash::JoinStreamLocalHash;
use crate::operator::join::local_sort_merge::JoinStreamLocalSortMerge;
use crate::operator::join::JoinStream;
use crate::operator::start::{BinaryElement, BinaryStartOperator, Start};
use crate::operator::{Data, DataKey, ExchangeData, KeyerFn, Operator};
use crate::stream::Stream;

/// Marker type for remembering that hash is the selected ship strategy.
#[derive(Clone, Copy)]
pub struct ShipHash;

/// Marker type for remembering that broadcast_right is the selected ship strategy.
#[derive(Clone, Copy)]
pub struct ShipBroadcastRight;

/// Marker trait for the ship strategy marker types.
pub trait ShipStrategy: Clone + Send {}

impl ShipStrategy for ShipHash {}
impl ShipStrategy for ShipBroadcastRight {}

/// This is an intermediate type for building a join operator.
///
/// The ship strategy has been selected as hash, and now the local strategy has to be selected.
pub struct JoinStreamShipHash<Key: DataKey, Out1: ExchangeData, Out2: ExchangeData, Keyer1, Keyer2>
where
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    inner: Stream<BinaryElement<Out1, Out2>, BinaryStartOperator<Out1, Out2>>,
    keyer1: Keyer1,
    keyer2: Keyer2,
    _key: PhantomData<Key>,
}

/// This is an intermediate type for building a join operator.
///
/// The ship strategy has been selected as broadcast_right, and now the local strategy has to be
/// selected.
pub struct JoinStreamShipBroadcastRight<
    Key: Data,
    Out1: ExchangeData,
    Out2: ExchangeData,
    Keyer1,
    Keyer2,
> where
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    inner: Stream<BinaryElement<Out1, Out2>, BinaryStartOperator<Out1, Out2>>,
    keyer1: Keyer1,
    keyer2: Keyer2,
    _key: PhantomData<Key>,
}

impl<Key: DataKey, Out1: ExchangeData, Out2: ExchangeData, Keyer1, Keyer2>
    JoinStreamShipHash<Key, Out1, Out2, Keyer1, Keyer2>
where
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    pub(crate) fn new<OperatorChain1, OperatorChain2>(
        prev: JoinStream<Key, Out1, Out2, OperatorChain1, OperatorChain2, Keyer1, Keyer2>,
    ) -> Self
    where
        OperatorChain1: Operator<Out1> + 'static,
        OperatorChain2: Operator<Out2> + 'static,
    {
        let keyer1 = prev.keyer1;
        let keyer2 = prev.keyer2;
        let next_strategy1 = NextStrategy::group_by(keyer1.clone());
        let next_strategy2 = NextStrategy::group_by(keyer2.clone());
        let inner =
            prev.lhs
                .binary_connection(prev.rhs, Start::multiple, next_strategy1, next_strategy2);
        JoinStreamShipHash {
            inner,
            keyer1,
            keyer2,
            _key: Default::default(),
        }
    }

    /// Select _local hash_ as local strategy.
    ///
    /// An hash-table will be used to generate the join tuples.
    pub fn local_hash(self) -> JoinStreamLocalHash<Key, Out1, Out2, Keyer1, Keyer2, ShipHash> {
        JoinStreamLocalHash::new(self.inner, self.keyer1, self.keyer2)
    }

    /// Select _sort-merge_ as local strategy.
    ///
    /// The tuples will be collected and sorted, then the tuples are generated.
    pub fn local_sort_merge(
        self,
    ) -> JoinStreamLocalSortMerge<Key, Out1, Out2, Keyer1, Keyer2, ShipHash>
    where
        Key: Ord,
    {
        JoinStreamLocalSortMerge::new(self.inner, self.keyer1, self.keyer2)
    }
}

impl<Key: Data, Out1: ExchangeData, Out2: ExchangeData, Keyer1, Keyer2>
    JoinStreamShipBroadcastRight<Key, Out1, Out2, Keyer1, Keyer2>
where
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    pub(crate) fn new<OperatorChain1, OperatorChain2>(
        prev: JoinStream<Key, Out1, Out2, OperatorChain1, OperatorChain2, Keyer1, Keyer2>,
    ) -> Self
    where
        OperatorChain1: Operator<Out1> + 'static,
        OperatorChain2: Operator<Out2> + 'static,
    {
        let keyer1 = prev.keyer1;
        let keyer2 = prev.keyer2;
        let inner = prev.lhs.binary_connection(
            prev.rhs,
            Start::multiple,
            NextStrategy::only_one(),
            NextStrategy::all(),
        );
        JoinStreamShipBroadcastRight {
            inner,
            keyer1,
            keyer2,
            _key: Default::default(),
        }
    }

    /// Select _local hash_ as local strategy.
    ///
    /// An hash-table will be used to generate the join tuples.
    pub fn local_hash(
        self,
    ) -> JoinStreamLocalHash<Key, Out1, Out2, Keyer1, Keyer2, ShipBroadcastRight>
    where
        Key: DataKey,
    {
        JoinStreamLocalHash::new(self.inner, self.keyer1, self.keyer2)
    }

    /// Select _sort-merge_ as local strategy.
    ///
    /// The tuples will be collected and sorted, then the tuples are generated.
    pub fn local_sort_merge(
        self,
    ) -> JoinStreamLocalSortMerge<Key, Out1, Out2, Keyer1, Keyer2, ShipBroadcastRight>
    where
        Key: Ord,
    {
        JoinStreamLocalSortMerge::new(self.inner, self.keyer1, self.keyer2)
    }
}
