use serde::{Deserialize, Serialize};

use crate::block::NextStrategy;
use crate::operator::start::{BinaryElement, Start};
use crate::operator::{ExchangeData, ExchangeDataKey, Operator};
use crate::stream::{KeyedStream, Stream};

#[derive(Clone, Serialize, Deserialize)]
pub enum MergeElement<A, B> {
    Left(A),
    Right(B),
}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
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
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s1 = env.stream(IteratorSource::new((0..10)));
    /// let s2 = env.stream(IteratorSource::new((10..20)));
    /// let res = s1.merge(s2).collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, (0..20).collect::<Vec<_>>());
    /// ```
    pub fn merge<OperatorChain2>(
        self,
        oth: Stream<Out, OperatorChain2>,
    ) -> Stream<Out, impl Operator<Out>>
    where
        OperatorChain2: Operator<Out> + 'static,
    {
        self.add_y_connection(
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

    #[cfg(feature = "timestamp")]
    pub(crate) fn merge_distinct<Out2, OperatorChain2>(
        self,
        right: Stream<Out2, OperatorChain2>,
    ) -> Stream<MergeElement<Out, Out2>, impl Operator<MergeElement<Out, Out2>>>
    where
        Out2: ExchangeData,
        OperatorChain2: Operator<Out2> + 'static,
    {
        // map the left and right streams to the same type
        let left = self.map(MergeElement::Left);
        let right = right.map(MergeElement::Right);

        left.merge(right)
    }
}

impl<Key: ExchangeDataKey, Out: ExchangeData, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<(Key, Out)> + 'static,
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
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s1 = env.stream(IteratorSource::new((0..3))).group_by(|&n| n % 2);
    /// let s2 = env.stream(IteratorSource::new((3..5))).group_by(|&n| n % 2);
    /// let res = s1.merge(s2).collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable(); // the output order is nondeterministic
    /// assert_eq!(res, vec![(0, 0), (0, 2), (0, 4), (1, 1), (1, 3)]);
    /// ```
    pub fn merge<OperatorChain2>(
        self,
        oth: KeyedStream<Key, Out, OperatorChain2>,
    ) -> KeyedStream<Key, Out, impl Operator<(Key, Out)>>
    where
        OperatorChain2: Operator<(Key, Out)> + 'static,
    {
        KeyedStream(self.0.merge(oth.0))
    }

    pub(crate) fn merge_distinct<Out2, OperatorChain2>(
        self,
        right: KeyedStream<Key, Out2, OperatorChain2>,
    ) -> KeyedStream<Key, MergeElement<Out, Out2>, impl Operator<(Key, MergeElement<Out, Out2>)>>
    where
        Out2: ExchangeData,
        OperatorChain2: Operator<(Key, Out2)> + 'static,
    {
        // map the left and right streams to the same type
        let left = self.map(|(_, x)| MergeElement::Left(x));
        let right = right.map(|(_, x)| MergeElement::Right(x));

        left.merge(right)
    }
}
