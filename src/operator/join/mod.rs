use std::marker::PhantomData;

use crate::operator::join::ship::{JoinStreamShipBroadcastRight, JoinStreamShipHash};
use crate::operator::{Data, DataKey, KeyerFn, Operator};
use crate::stream::Stream;

mod local_hash;
mod local_sort_merge;
mod ship;
mod start;

pub type OuterJoinTuple<Out1, Out2> = (Option<Out1>, Option<Out2>);
pub type LeftJoinTuple<Out1, Out2> = (Out1, Option<Out2>);
pub type InnerJoinTuple<Out1, Out2> = (Out1, Out2);

pub struct JoinStream<Key, Out1: Data, Out2: Data, OperatorChain1, OperatorChain2, Keyer1, Keyer2>
where
    OperatorChain1: Operator<Out1>,
    OperatorChain2: Operator<Out2>,
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    pub(crate) lhs: Stream<Out1, OperatorChain1>,
    pub(crate) rhs: Stream<Out2, OperatorChain2>,
    pub(crate) keyer1: Keyer1,
    pub(crate) keyer2: Keyer2,
    _key: PhantomData<Key>,
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    pub fn join_with<Out2: Data, OperatorChain2, Key, Keyer1, Keyer2>(
        self,
        rhs: Stream<Out2, OperatorChain2>,
        keyer1: Keyer1,
        keyer2: Keyer2,
    ) -> JoinStream<Key, Out, Out2, OperatorChain, OperatorChain2, Keyer1, Keyer2>
    where
        OperatorChain2: Operator<Out2>,
        Keyer1: KeyerFn<Key, Out>,
        Keyer2: KeyerFn<Key, Out2>,
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

impl<Key: Data, Out1: Data, Out2: Data, OperatorChain1, OperatorChain2, Keyer1, Keyer2>
    JoinStream<Key, Out1, Out2, OperatorChain1, OperatorChain2, Keyer1, Keyer2>
where
    OperatorChain1: Operator<Out1> + 'static,
    OperatorChain2: Operator<Out2> + 'static,
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    pub fn ship_hash(self) -> JoinStreamShipHash<Key, Out1, Out2, Keyer1, Keyer2>
    where
        Key: DataKey,
    {
        JoinStreamShipHash::new(self)
    }

    pub fn ship_broadcast_right(
        self,
    ) -> JoinStreamShipBroadcastRight<Key, Out1, Out2, Keyer1, Keyer2> {
        JoinStreamShipBroadcastRight::new(self)
    }
}
