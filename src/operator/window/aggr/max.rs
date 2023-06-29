use std::cmp::Ordering;

use super::{super::*, FoldFirst};
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyedStream, WindowedStream};

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Out>,
    OperatorChain: Operator<(Key, Out)> + 'static,
    Key: DataKey,
    Out: Data + Ord,
{
    pub fn max(self) -> KeyedStream<Key, Out, impl Operator<(Key, Out)>> {
        let acc = FoldFirst::<Out, _>::new(|max, x| {
            if x > *max {
                *max = x
            }
        });
        self.add_window_operator("WindowMax", acc)
    }
}

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Out>,
    OperatorChain: Operator<(Key, Out)> + 'static,
    Key: DataKey,
    Out: Data,
{
    pub fn max_by_key<K: Ord, F: Fn(&Out) -> K + Clone + Send + 'static>(
        self,
        get_key: F,
    ) -> KeyedStream<Key, Out, impl Operator<(Key, Out)>> {
        let acc = FoldFirst::<Out, _>::new(move |max, x| {
            if (get_key)(&x) > (get_key)(max) {
                *max = x
            }
        });
        self.add_window_operator("WindowMax", acc)
    }

    pub fn max_by<F: Fn(&Out, &Out) -> Ordering + Clone + Send + 'static>(
        self,
        compare: F,
    ) -> KeyedStream<Key, Out, impl Operator<(Key, Out)>> {
        let acc = FoldFirst::<Out, _>::new(move |max, x| {
            if (compare)(&x, max).is_gt() {
                *max = x
            }
        });
        self.add_window_operator("WindowMax", acc)
    }
}
