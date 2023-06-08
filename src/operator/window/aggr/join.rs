use super::super::*;
use crate::operator::merge::MergeElement;
use crate::operator::{Data, DataKey, Operator};
use crate::stream::KeyedStream;

#[derive(Clone)]
struct Join<L, R> {
    left: Vec<L>,
    right: Vec<R>,
}

impl<L: Data, R: Data> WindowAccumulator for Join<L, R> {
    type In = MergeElement<L, R>;
    type Out = ProductIterator<L, R>; // TODO: may have more efficient formulations

    #[inline]
    fn process(&mut self, el: Self::In) {
        match el {
            MergeElement::Left(l) => self.left.push(l),
            MergeElement::Right(r) => self.right.push(r),
        }
    }

    #[inline]
    fn output(mut self) -> Self::Out {
        ProductIterator::new(
            std::mem::take(&mut self.left),
            std::mem::take(&mut self.right),
        )
    }
}

#[derive(Clone)]
struct ProductIterator<L, R> {
    left: Vec<L>,
    right: Vec<R>,
    i: usize,
    j: usize,
}

impl<L, R> ProductIterator<L, R> {
    fn new(left: Vec<L>, right: Vec<R>) -> Self {
        Self {
            left,
            right,
            i: 0,
            j: 0,
        }
    }
}

impl<L: Clone, R: Clone> Iterator for ProductIterator<L, R> {
    type Item = (L, R);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.left.len() || self.j >= self.right.len() {
            return None;
        }

        let ret = (self.left[self.i].clone(), self.right[self.j].clone());

        self.j += 1;
        if self.j >= self.right.len() {
            self.j = 0;
            self.i += 1;
        }

        Some(ret)
    }
}

impl<Key, Out, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<(Key, Out)> + 'static,
    Key: ExchangeData + DataKey,
    Out: ExchangeData,
{
    pub fn window_join<Out2, OperatorChain2, WindowDescr>(
        self,
        descr: WindowDescr,
        right: KeyedStream<Key, Out2, OperatorChain2>,
    ) -> KeyedStream<Key, (Out, Out2), impl Operator<(Key, (Out, Out2))>>
    where
        OperatorChain2: Operator<(Key, Out2)> + 'static,
        Out2: ExchangeData,
        WindowDescr: WindowDescription<MergeElement<Out, Out2>> + 'static,
    {
        let acc = Join::<Out, Out2> {
            left: Default::default(),
            right: Default::default(),
        };

        self.merge_distinct(right)
            .window(descr)
            .add_window_operator("WindowJoin", acc)
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn product_iterator() {
        let t = ProductIterator::new(vec![1], vec!["asd"]).collect::<Vec<_>>();
        let expected = vec![(1, "asd")];

        assert_eq!(expected, t);

        let t = ProductIterator::new(vec![1, 3, 5], vec![2, 4]).collect::<Vec<_>>();
        let expected = vec![(1, 2), (1, 4), (3, 2), (3, 4), (5, 2), (5, 4)];

        assert_eq!(expected, t);

        let t = ProductIterator::new(vec![1, 3, 5], vec![]).collect::<Vec<(usize, usize)>>();
        let expected: Vec<(usize, usize)> = vec![];

        assert_eq!(expected, t);

        let t = ProductIterator::new(vec![], vec![1, 3, 5]).collect::<Vec<(usize, usize)>>();
        let expected: Vec<(usize, usize)> = vec![];

        assert_eq!(expected, t);
    }
}
