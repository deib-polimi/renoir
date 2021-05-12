use crate::operator::{DataKey, ExchangeData, Operator, StartBlock};
use crate::stream::{KeyValue, KeyedStream, Stream};

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
        self.add_y_connection(oth, |id1, id2, state_lock| {
            StartBlock::concat(vec![id1, id2], state_lock)
        })
    }
}

impl<Key: DataKey, Out: ExchangeData, OperatorChain> KeyedStream<Key, Out, OperatorChain>
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
}
