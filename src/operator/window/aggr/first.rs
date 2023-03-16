use super::super::*;
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, WindowedStream};

#[derive(Clone)]
pub(crate) struct First<T>(Option<T>);

impl<T: Data> WindowAccumulator for First<T>
{
    type In = T;
    type Out = T;

    fn process(&mut self, el: Self::In) {
        if let None = self.0 {
            self.0 = Some(el);
        }
    }

    fn output(self) -> Self::Out {
        self.0.expect("First window accumulator empty! Should contain something if output is called")
    }
}

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowBuilder,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
    Key: DataKey,
    Out: Data,
{
    pub fn first(
        self,
    ) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>> {
        let acc = First(None);
        self.add_window_operator("WindowSum", acc)
    }
}
