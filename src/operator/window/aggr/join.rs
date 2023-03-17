use super::super::*;
use crate::operator::merge::MergeElement;
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, WindowedStream};

#[derive(Clone)]
struct Join<L, R> {
    left: Vec<L>,
    right: Vec<R>,
}

impl<L: Data, R: Data> WindowAccumulator for Join<L, R> {
    type In = MergeElement<L, R>;
    type Out = Vec<(L, R)>; // TODO: may have more efficient formulations

    fn process(&mut self, el: Self::In) {
        match el {
            MergeElement::Left(l) => self.left.push(l),
            MergeElement::Right(r) => self.right.push(r),
        }
    }

    fn output(mut self) -> Self::Out {
        self.left
            .drain(..)
            .flat_map(|l| self.right.iter().cloned().map(move |r| (l.clone(), r)))
            .collect()
    }
}

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowBuilder + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
    Key: ExchangeData + DataKey,
    Out: ExchangeData,
{
    pub fn join<Out2, OperatorChain2>(
        self,
        right: KeyedStream<Key, Out2, OperatorChain2>,
    ) -> KeyedStream<Key, (Out, Out2), impl Operator<KeyValue<Key, (Out, Out2)>>>
    where
        OperatorChain2: Operator<(Key, Out2)> + 'static,
        Out2: ExchangeData,
    {
        let acc = Join::<Out, Out2> {
            left: Default::default(),
            right: Default::default(),
        };
        let WindowedStream { inner, descr, .. } = self;

        inner
            .merge_distinct(right)
            .window(descr)
            .add_window_operator("WindowJoin", acc)
            .flatten()
    }
}
