use std::cmp::Ordering;

use super::{super::*, FoldFirst};
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyedStream, WindowedStream};

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Out>,
    OperatorChain: Operator<Out = (Key, Out)> + 'static,
    Key: DataKey,
    Out: Data + Ord,
{
    pub fn min(self) -> KeyedStream<impl Operator<Out = (Key, Out)>> {
        let acc = FoldFirst::<Out, _>::new(|min, x| {
            if *x < *min {
                *min = x.clone()
            }
        });
        self.add_window_operator("WindowMin", acc)
    }
}

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Out>,
    OperatorChain: Operator<Out = (Key, Out)> + 'static,
    Key: DataKey,
    Out: Data,
{
    pub fn min_by_key<K: Ord, F: Fn(&Out) -> K + Clone + Send + 'static>(
        self,
        get_key: F,
    ) -> KeyedStream<impl Operator<Out = (Key, Out)>> {
        let acc = FoldFirst::<Out, _>::new(move |min, x| {
            if (get_key)(x) < (get_key)(min) {
                *min = x.clone()
            }
        });
        self.add_window_operator("WindowMin", acc)
    }

    pub fn min_by<F: Fn(&Out, &Out) -> Ordering + Clone + Send + 'static>(
        self,
        compare: F,
    ) -> KeyedStream<impl Operator<Out = (Key, Out)>> {
        let acc = FoldFirst::<Out, _>::new(move |min, x| {
            if (compare)(x, min).is_lt() {
                *min = x.clone()
            }
        });
        self.add_window_operator("WindowMin", acc)
    }
}
