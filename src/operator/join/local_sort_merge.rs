#![allow(clippy::type_complexity)]

use std::marker::PhantomData;

use crate::operator::join::ship::{ShipBroadcastRight, ShipHash, ShipStrategy};
use crate::operator::join::start::JoinStartBlock;
use crate::operator::{
    Data, ExchangeData, InnerJoinTuple, LeftJoinTuple, OuterJoinTuple, StartBlock,
};
use crate::stream::{KeyValue, KeyedStream, Stream};

pub struct JoinStreamLocalSortMerge<
    Key: Data + Ord,
    Out1: ExchangeData,
    Out2: ExchangeData,
    ShipStrat: ShipStrategy,
> {
    _k: PhantomData<Key>,
    _o1: PhantomData<Out1>,
    _o2: PhantomData<Out2>,
    _s: PhantomData<ShipStrat>,
}

impl<Key: Data + Ord, Out1: ExchangeData, Out2: ExchangeData>
    JoinStreamLocalSortMerge<Key, Out1, Out2, ShipHash>
{
    pub fn inner(
        self,
        // ) -> KeyedStream<Key, InnerJoinTuple<Out1, Out2>, impl Operator<KeyValue<Key, InnerJoinTuple<Out1, Out2>>>> {
    ) -> KeyedStream<
        Key,
        InnerJoinTuple<Out1, Out2>,
        StartBlock<KeyValue<Key, InnerJoinTuple<Out1, Out2>>>,
    > {
        todo!()
    }

    pub fn left(
        self,
        // ) -> KeyedStream<Key, LeftJoinTuple<Out1, Out2>, impl Operator<KeyValue<Key, LeftJoinTuple<Out1, Out2>>>>
    ) -> KeyedStream<
        Key,
        LeftJoinTuple<Out1, Out2>,
        StartBlock<KeyValue<Key, LeftJoinTuple<Out1, Out2>>>,
    > {
        todo!()
    }

    pub fn outer(
        self,
    ) -> KeyedStream<
        Key,
        OuterJoinTuple<Out1, Out2>,
        // impl Operator<KeyValue<Key, OuterJoinTuple<Out1, Out2>>>,
        StartBlock<KeyValue<Key, OuterJoinTuple<Out1, Out2>>>,
    > {
        todo!()
    }
}

impl<Key: Data + Ord, Out1: ExchangeData, Out2: ExchangeData>
    JoinStreamLocalSortMerge<Key, Out1, Out2, ShipBroadcastRight>
{
    pub fn inner(
        self,
        // ) -> KeyedStream<Key, InnerJoinTuple<Out1, Out2>, impl Operator<KeyValue<Key, InnerJoinTuple<Out1, Out2>>>> {
    ) -> Stream<
        KeyValue<Key, InnerJoinTuple<Out1, Out2>>,
        StartBlock<KeyValue<Key, InnerJoinTuple<Out1, Out2>>>,
    > {
        todo!()
    }

    pub fn left(
        self,
        // ) -> KeyedStream<Key, LeftJoinTuple<Out1, Out2>, impl Operator<KeyValue<Key, LeftJoinTuple<Out1, Out2>>>>
    ) -> Stream<
        KeyValue<Key, LeftJoinTuple<Out1, Out2>>,
        StartBlock<KeyValue<Key, LeftJoinTuple<Out1, Out2>>>,
    > {
        todo!()
    }
}
