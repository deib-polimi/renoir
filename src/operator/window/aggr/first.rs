use super::super::*;
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyedStream, WindowedStream};

#[derive(Clone)]
pub(crate) struct First<T>(Option<T>);

impl<T: Data> WindowAccumulator for First<T> {
    type In = T;
    type Out = T;

    #[inline]
    fn process(&mut self, el: Self::In) {
        if self.0.is_none() {
            self.0 = Some(el);
        }
    }

    #[inline]
    fn output(self) -> Self::Out {
        self.0.expect("First::output() called before any element was processed")
    }
}

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
    Key: DataKey,
    Out: Data,
{
    pub fn first(self) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>> {
        let acc = First(None);
        self.add_window_operator("WindowFirst", acc)
    }
}
