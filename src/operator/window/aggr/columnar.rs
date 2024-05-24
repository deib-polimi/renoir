use arrow::array::ArrayBuilder;

/// TODO
use super::super::*;
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyedStream, WindowedStream};

#[derive(Clone)]
struct Columnar<I, O>
where
    O: Extend<I> + Default,
{
    array_builder: ArrayBuilder,
    _i: PhantomData<I>,
}


impl<I, O> WindowAccumulator for Columnar<I, O>
where
    O:,
    I: Clone + Send + 'static,
    O: Extend<I> + Default + Clone+ Send + 'static,
{
    type In = I;

    type Out = O;

    #[inline]
    fn process(&mut self, el: Self::In) {
        self.collection.extend(std::iter::once(el));
    }

    #[inline]
    fn output(self) -> Self::Out {
        self.collection
    }

    #[inline]
    fn size_hint(&mut self, size: usize) {
        
    }
}

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Out>,
    OperatorChain: Operator<Out = (Key, Out)> + 'static,
    Key: DataKey,
    Out: Data + Ord,
{
    pub fn to_arrow<C: Extend<Out> + Default + Clone + Send + 'static>(
        self,
    ) -> KeyedStream<impl Operator<Out = (Key, C)>> {
        let acc = Columnar::<Out, C> {
            collection: Default::default(),
            _i: PhantomData,
        };
        self.add_window_operator("WindowCollect", acc)
    }
}
