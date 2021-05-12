use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::sync::Arc;

use crate::block::NextStrategy;
use crate::operator::{DataKey, EndBlock, ExchangeData};
use crate::operator::{KeyBy, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
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
        let keyer2 = keyer.clone();

        let next_strategy = NextStrategy::GroupBy(Arc::new(move |out| {
            let mut s = DefaultHasher::new();
            keyer2(out).hash(&mut s);
            s.finish() as usize
        }));

        let new_stream = self
            .add_block(EndBlock::new, next_strategy)
            .add_operator(|prev| KeyBy::new(prev, keyer));
        KeyedStream(new_stream)
    }
}
