use super::super::*;
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyedStream, WindowedStream};

#[derive(Clone)]
pub(crate) struct Last<T>(Option<T>);

impl<T: Data> WindowAccumulator for Last<T> {
    type In = T;
    type Out = Option<T>;

    #[inline]
    fn process(&mut self, el: Self::In) {
        self.0 = Some(el);
    }

    #[inline]
    fn output(self) -> Self::Out {
        self.0
    }
}

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
    Key: DataKey,
    Out: Data,
{
    pub fn last(self) -> KeyedStream<Key, Option<Out>, impl Operator<KeyValue<Key, Option<Out>>>> {
        let acc = Last(None);
        self.add_window_operator("WindowLast", acc)
    }
}
