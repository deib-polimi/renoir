#![allow(clippy::type_complexity)]

use crate::block::NextStrategy;
use crate::operator::join::local_hash::JoinStreamLocalHash;
use crate::operator::join::local_sort_merge::JoinStreamLocalSortMerge;
use crate::operator::join::start::{JoinElement, JoinStartBlock};
use crate::operator::{Data, DataKey, JoinStream, KeyerFn, Operator};
use crate::stream::Stream;

#[derive(Clone, Copy)]
pub struct ShipHash;
#[derive(Clone, Copy)]
pub struct ShipBroadcastRight;

pub trait ShipStrategy: Clone + Send {}

impl ShipStrategy for ShipHash {}
impl ShipStrategy for ShipBroadcastRight {}

pub struct JoinStreamShipHash<Key: DataKey, Out1: Data, Out2: Data, Keyer1, Keyer2>
where
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    inner: Stream<JoinElement<Key, Out1, Out2>, JoinStartBlock<Key, Out1, Out2, Keyer1, Keyer2>>,
}

pub struct JoinStreamShipBroadcastRight<Key: Data, Out1: Data, Out2: Data, Keyer1, Keyer2>
where
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    inner: Stream<JoinElement<Key, Out1, Out2>, JoinStartBlock<Key, Out1, Out2, Keyer1, Keyer2>>,
}

impl<Key: DataKey, Out1: Data, Out2: Data, Keyer1, Keyer2>
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
        let inner = prev.lhs.add_y_connection(
            prev.rhs,
            move |prev1, prev2, state_lock| {
                JoinStartBlock::new(keyer1, keyer2, prev1, prev2, state_lock)
            },
            next_strategy1,
            next_strategy2,
        );
        JoinStreamShipHash { inner }
    }

    pub fn local_hash(self) -> JoinStreamLocalHash<Key, Out1, Out2, Keyer1, Keyer2, ShipHash> {
        JoinStreamLocalHash::new(self.inner)
    }

    pub fn local_sort_merge(self) -> JoinStreamLocalSortMerge<Key, Out1, Out2, ShipHash>
    where
        Key: Ord,
    {
        todo!()
    }
}

impl<Key: Data, Out1: Data, Out2: Data, Keyer1, Keyer2>
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
        let inner = prev.lhs.add_y_connection(
            prev.rhs,
            move |prev1, prev2, state_lock| {
                JoinStartBlock::new(keyer1, keyer2, prev1, prev2, state_lock)
            },
            NextStrategy::OnlyOne,
            NextStrategy::All,
        );
        JoinStreamShipBroadcastRight { inner }
    }

    pub fn local_hash(
        self,
    ) -> JoinStreamLocalHash<Key, Out1, Out2, Keyer1, Keyer2, ShipBroadcastRight>
    where
        Key: DataKey,
    {
        JoinStreamLocalHash::new(self.inner)
    }

    pub fn local_sort_merge(self) -> JoinStreamLocalSortMerge<Key, Out1, Out2, ShipBroadcastRight>
    where
        Key: Ord,
    {
        todo!()
    }
}
