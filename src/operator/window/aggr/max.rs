use super::{super::*, FoldFirst};
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, WindowedStream};

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowBuilder,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
    Key: DataKey,
    Out: Data + Ord,
{
    pub fn max(
        self,
    ) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>> {
        let acc = FoldFirst::<Out, _>::new(|max, x| if x > *max { *max = x });
        self.add_window_operator("WindowMax", acc)
    }
}
