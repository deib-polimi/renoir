use serde::{Deserialize, Serialize};

use crate::block::NextStrategy;
use crate::operator::start::{StartBlock, TwoSidesItem};
use crate::operator::{ExchangeData, ExchangeDataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};

#[derive(Clone, Serialize, Deserialize)]
pub enum ConcatElement<A, B> {
    Left(A),
    Right(B),
}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn concat<OperatorChain2>(
        self,
        oth: Stream<Out, OperatorChain2>,
    ) -> Stream<Out, impl Operator<Out>>
    where
        OperatorChain2: Operator<Out> + 'static,
    {
        self.add_y_connection(
            oth,
            StartBlock::multiple,
            NextStrategy::only_one(),
            NextStrategy::only_one(),
        )
        .flat_map(|e| match e {
            TwoSidesItem::Left(item) => Some(item),
            TwoSidesItem::Right(item) => Some(item),
            _ => None,
        })
    }

    pub(crate) fn map_concat<Out2, OperatorChain2>(
        self,
        right: Stream<Out2, OperatorChain2>,
    ) -> Stream<ConcatElement<Out, Out2>, impl Operator<ConcatElement<Out, Out2>>>
    where
        Out2: ExchangeData,
        OperatorChain2: Operator<Out2> + 'static,
    {
        // map the left and right streams to the same type
        let left = self.map(ConcatElement::Left);
        let right = right.map(ConcatElement::Right);

        left.concat(right)
    }
}

impl<Key: ExchangeDataKey, Out: ExchangeData, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    pub fn concat<OperatorChain2>(
        self,
        oth: KeyedStream<Key, Out, OperatorChain2>,
    ) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>>
    where
        OperatorChain2: Operator<KeyValue<Key, Out>> + 'static,
    {
        KeyedStream(self.0.concat(oth.0))
    }

    pub(crate) fn map_concat<Out2, OperatorChain2>(
        self,
        right: KeyedStream<Key, Out2, OperatorChain2>,
    ) -> KeyedStream<
        Key,
        ConcatElement<Out, Out2>,
        impl Operator<KeyValue<Key, ConcatElement<Out, Out2>>>,
    >
    where
        Out2: ExchangeData,
        OperatorChain2: Operator<KeyValue<Key, Out2>> + 'static,
    {
        // map the left and right streams to the same type
        let left = self.map(|(_, x)| ConcatElement::Left(x));
        let right = right.map(|(_, x)| ConcatElement::Right(x));

        left.concat(right)
    }
}
