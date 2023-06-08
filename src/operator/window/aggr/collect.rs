/// TODO


use super::super::*;
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyedStream, WindowedStream};

#[derive(Clone)]
struct CollectVec<I, O, F>
where
    F: Fn(Vec<I>) -> O,
{
    vec: Vec<I>,
    f: F,
    _o: PhantomData<O>,
}

impl<I, O, F> WindowAccumulator for CollectVec<I, O, F>
where
    F: Fn(Vec<I>) -> O + Send + Clone + 'static,
    I: Clone + Send + 'static,
    O: Clone + Send + 'static,
{
    type In = I;

    type Out = O;

    #[inline]
    fn process(&mut self, el: Self::In) {
        self.vec.push(el);
    }

    #[inline]
    fn output(self) -> Self::Out {
        (self.f)(self.vec)
    }
}

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
    Key: DataKey,
    Out: Data + Ord,
{
    /// Prefer other aggregators if possible as they don't save all elements
    pub fn map<NewOut: Data, F: Fn(Vec<Out>) -> NewOut + Send + Clone + 'static>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>> {
        let acc = CollectVec::<Out, NewOut, _> {
            vec: Default::default(),
            f,
            _o: PhantomData,
        };
        self.add_window_operator("WindowMap", acc)
    }
}
