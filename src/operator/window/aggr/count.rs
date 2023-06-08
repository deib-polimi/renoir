use super::super::*;
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyedStream, WindowedStream};

#[derive(Clone)]
pub(crate) struct Count<T>(usize, PhantomData<T>);

impl<T: Data> WindowAccumulator for Count<T> {
    type In = T;
    type Out = usize;

    #[inline]
    fn process(&mut self, _: Self::In) {
        self.0 += 1;
    }

    #[inline]
    fn output(self) -> Self::Out {
        self.0
    }
}

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Out>,
    OperatorChain: Operator<(Key, Out)> + 'static,
    Key: DataKey,
    Out: Data,
{
    pub fn count(self) -> KeyedStream<Key, usize, impl Operator<(Key, usize)>> {
        let acc = Count(0, PhantomData);
        self.add_window_operator("WindowCount", acc)
    }
}
