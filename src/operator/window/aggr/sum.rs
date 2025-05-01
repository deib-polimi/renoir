use std::ops::AddAssign;

use super::{super::*, Fold};
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyedStream, WindowedStream};

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Out>,
    OperatorChain: Operator<Out = (Key, Out)> + 'static,
    Key: DataKey,
    Out: Data,
{
    pub fn sum<NewOut: Data + Default + AddAssign<Out>>(
        self,
    ) -> KeyedStream<impl Operator<Out = (Key, NewOut)>> {
        let acc = Fold::new(NewOut::default(), |sum, x: &Out| *sum += x.clone());
        self.add_window_operator("WindowSum", acc)
    }
}
