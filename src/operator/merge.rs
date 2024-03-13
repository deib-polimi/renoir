use serde::{Deserialize, Serialize};

use crate::block::NextStrategy;
use crate::operator::start::{BinaryElement, Start};
use crate::operator::{ExchangeData, Operator};
use crate::stream::Stream;

#[derive(Clone, Serialize, Deserialize)]
pub enum MergeElement<A, B> {
    Left(A),
    Right(B),
}

impl<Op> Stream<Op>
where
    Op: Operator + 'static,
    Op::Out: ExchangeData,
{
    /// Merge the items of this stream with the items of another stream with the same type.
    ///
    /// **Note**: the order of the resulting items is not specified.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamContext, RuntimeConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamContext::new(RuntimeConfig::local(1));
    /// let s1 = env.stream(IteratorSource::new((0..10)));
    /// let s2 = env.stream(IteratorSource::new((10..20)));
    /// let res = s1.merge(s2).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, (0..20).collect::<Vec<_>>());
    /// ```
    pub fn merge<Op2>(self, oth: Stream<Op2>) -> Stream<impl Operator<Out = Op::Out>>
    where
        Op: 'static,
        Op2: Operator<Out = Op::Out> + 'static,
    {
        self.binary_connection(
            oth,
            Start::multiple,
            NextStrategy::only_one(),
            NextStrategy::only_one(),
        )
        .filter_map(|e| match e {
            BinaryElement::Left(item) => Some(item),
            BinaryElement::Right(item) => Some(item),
            _ => None,
        })
    }

    pub(crate) fn merge_distinct<Op2>(
        self,
        right: Stream<Op2>,
    ) -> Stream<impl Operator<Out = MergeElement<Op::Out, Op2::Out>>>
    where
        Op: 'static,
        Op2: Operator + 'static,
        Op2::Out: ExchangeData,
    {
        // map the left and right streams to the same type
        let left = self.map(MergeElement::Left);
        let right = right.map(MergeElement::Right);

        left.merge(right)
    }
}
