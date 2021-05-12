use crate::block::NextStrategy;
use crate::operator::{Data, DataKey, EndBlock};
use crate::operator::{KeyBy, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn group_by<Key: DataKey, Keyer>(
        self,
        keyer: Keyer,
    ) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>>
    where
        Keyer: Fn(&Out) -> Key + Send + Clone + Sync + 'static,
    {
        let next_strategy = NextStrategy::group_by(keyer.clone());
        let new_stream = self
            .add_block(EndBlock::new, next_strategy)
            .add_operator(|prev| KeyBy::new(prev, keyer));
        KeyedStream(new_stream)
    }
}
