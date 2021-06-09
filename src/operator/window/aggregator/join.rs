use crate::operator::concat::ConcatElement;
use crate::operator::window::WindowDescription;
use crate::operator::{ExchangeData, ExchangeDataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, KeyedWindowedStream, Stream, WindowedStream};

impl<Key: ExchangeDataKey, Out: ExchangeData, Out2: ExchangeData, WindowDescr, OperatorChain>
    KeyedWindowedStream<Key, Out, OperatorChain, ConcatElement<Out, Out2>, WindowDescr>
where
    WindowDescr: WindowDescription<Key, ConcatElement<Out, Out2>> + Clone + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Joins the elements coming from two streams into pairs.
    ///
    /// Joins on windows are equivalent to:
    /// 1. Merging the two streams into a single one.
    /// 2. Grouping the elements in windows, following the rules of the passed window description.
    /// 3. For each window, generating a tuple for each pair of elements coming from the two streams.
    ///
    /// This join behaves as an inner join.
    ///
    /// Also see [`KeyedStream::interval_join`].
    ///
    /// ## Example
    /// ```
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::IteratorSource;
    /// # use rstream::operator::window::{CountWindow, EventTimeWindow};
    /// # use std::time::Duration;
    /// # use rstream::operator::Timestamp;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s1 = env
    ///     .stream(IteratorSource::new((0..4)))
    ///     .add_timestamps(
    ///         |&n| Timestamp::from_millis(n),
    ///         |&n, &ts| Some(ts)
    ///     );
    /// let s2 = env
    ///     .stream(IteratorSource::new((4..8)))
    ///     .add_timestamps(
    ///         |&n| Timestamp::from_millis(n - 4),
    ///         |&n, &ts| Some(ts)
    ///     );
    /// let res = s1
    ///     .group_by(|&n| n % 2)
    ///     .window(EventTimeWindow::tumbling(Duration::from_millis(2)))
    ///     .join(s2.group_by(|&n| n % 2))
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, (0, 4)), (0, (2, 6)), (1, (1, 5)), (1, (3, 7))]);
    /// ```
    pub fn join<OperatorChain2>(
        self,
        right: KeyedStream<Key, Out2, OperatorChain2>,
    ) -> KeyedStream<Key, (Out, Out2), impl Operator<KeyValue<Key, (Out, Out2)>>>
    where
        OperatorChain2: Operator<KeyValue<Key, Out2>> + 'static,
    {
        // concatenate the two streams and apply the window
        self.inner
            .map_concat(right)
            .window(self.descr)
            .add_generic_window_operator("WindowJoin", move |window| {
                // divide the elements coming from the left stream from the elements
                // coming from the right stream
                let (left, right) = window
                    .items()
                    .partition::<Vec<_>, _>(|x| matches!(x, ConcatElement::Left(_)));

                // calculate all the pairs of elements in the current window
                let mut res = Vec::new();
                for l in left.into_iter() {
                    for r in right.iter() {
                        match (l, r) {
                            (ConcatElement::Left(l), ConcatElement::Right(r)) => {
                                res.push((l.clone(), r.clone()))
                            }
                            _ => {
                                unreachable!("Items of left and right streams are partitioned")
                            }
                        }
                    }
                }
                res
            })
            .flatten()
    }
}

impl<Out: ExchangeData, Out2: ExchangeData, WindowDescr, OperatorChain>
    WindowedStream<Out, OperatorChain, ConcatElement<Out, Out2>, WindowDescr>
where
    WindowDescr: WindowDescription<(), ConcatElement<Out, Out2>> + Clone + 'static,
    OperatorChain: Operator<KeyValue<(), Out>> + 'static,
{
    /// Joins the elements coming from two streams into pairs.
    ///
    /// Joins on windows are equivalent to:
    /// 1. Merging the two streams into a single one.
    /// 2. Grouping the elements in windows, following the rules of the passed window description.
    /// 3. For each window, generating a tuple for each pair of elements coming from the two streams.
    ///
    /// This join behaves as an inner join.
    ///
    /// Also see [`Stream::interval_join`].
    ///
    /// ## Example
    /// ```
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::IteratorSource;
    /// # use rstream::operator::window::{CountWindow, EventTimeWindow};
    /// # use std::time::Duration;
    /// # use rstream::operator::Timestamp;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s1 = env
    ///     .stream(IteratorSource::new((0..4)))
    ///     .add_timestamps(
    ///         |&n| Timestamp::from_millis(n),
    ///         |&n, &ts| Some(ts)
    ///     );
    /// let s2 = env
    ///     .stream(IteratorSource::new((4..8)))
    ///     .add_timestamps(
    ///         |&n| Timestamp::from_millis(n - 4),
    ///         |&n, &ts| Some(ts)
    ///     );
    /// let res = s1
    ///     .window_all(EventTimeWindow::tumbling(Duration::from_millis(2)))
    ///     .join(s2)
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 4), (0, 5), (1, 4), (1, 5), (2, 6), (2, 7), (3, 6), (3, 7)]);
    /// ```
    pub fn join<OperatorChain2>(
        self,
        right: Stream<Out2, OperatorChain2>,
    ) -> Stream<(Out, Out2), impl Operator<(Out, Out2)>>
    where
        OperatorChain2: Operator<Out2> + 'static,
    {
        self.inner
            .join(right.max_parallelism(1).key_by(|_| ()))
            .drop_key()
    }
}
