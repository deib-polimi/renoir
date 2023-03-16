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
    pub fn min(self) -> KeyedStream<Key, Option<Out>, impl Operator<KeyValue<Key, Option<Out>>>> {
        let acc = FoldFirst::<Out, _>::new(|min, x| {
            if x < *min {
                *min = x
            }
        });
        self.add_window_operator("WindowMin", acc)
    }
}
