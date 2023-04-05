use crate::operator::{Data, DataKey, ExchangeData, ExchangeDataKey, Operator};
use crate::stream::{KeyedStream, Stream};

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Perform the reduction operation separately for each key.
    ///
    /// This is equivalent of partitioning the stream using the `keyer` function, and then applying
    /// [`Stream::reduce_assoc`] to each partition separately.
    ///
    /// Note however that there is a difference between `stream.group_by(keyer).reduce(...)` and
    /// `stream.group_by_reduce(keyer, ...)`. The first performs the network shuffle of every item in
    /// the stream, and **later** performs the reduction (i.e. nearly all the elements will be sent to
    /// the network). The latter avoids sending the items by performing first a local reduction on
    /// each host, and then send only the locally reduced results (i.e. one message per replica, per
    /// key); then the global step is performed aggregating the results.
    ///
    /// The resulting stream will still be keyed and will contain only a single message per key (the
    /// final result).
    ///
    /// Note that the output type must be the same as the input type, if you need a different type
    /// consider using [`Stream::group_by_fold`].
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .group_by_reduce(|&n| n % 2, |acc, value| *acc += value)
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0 + 2 + 4), (1, 1 + 3)]);
    /// ```
    pub fn group_by_reduce<Key: ExchangeDataKey, Keyer, F>(
        self,
        keyer: Keyer,
        f: F,
    ) -> KeyedStream<Key, Out, impl Operator<(Key, Out)>>
    where
        Keyer: Fn(&Out) -> Key + Send + Clone + 'static,
        F: Fn(&mut Out, Out) + Send + Clone + 'static,
    {
        let f2 = f.clone();

        self.group_by_fold(
            keyer,
            None,
            move |acc, value| match acc {
                None => *acc = Some(value),
                Some(acc) => f(acc, value),
            },
            move |acc1, acc2| match acc1 {
                None => *acc1 = acc2,
                Some(acc1) => {
                    if let Some(acc2) = acc2 {
                        f2(acc1, acc2)
                    }
                }
            },
        )
        .map(|(_, value)| value.unwrap())
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<(Key, Out)> + 'static,
{
    /// Perform the reduction operation separately for each key.
    ///
    /// Note that there is a difference between `stream.group_by(keyer).reduce(...)` and
    /// `stream.group_by_reduce(keyer, ...)`. The first performs the network shuffle of every item in
    /// the stream, and **later** performs the reduction (i.e. nearly all the elements will be sent to
    /// the network). The latter avoids sending the items by performing first a local reduction on
    /// each host, and then send only the locally reduced results (i.e. one message per replica, per
    /// key); then the global step is performed aggregating the results.
    ///
    /// The resulting stream will still be keyed and will contain only a single message per key (the
    /// final result).
    ///
    /// Note that the output type must be the same as the input type, if you need a different type
    /// consider using [`KeyedStream::fold`].
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5))).group_by(|&n| n % 2);
    /// let res = s
    ///     .reduce(|acc, value| *acc += value)
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0 + 2 + 4), (1, 1 + 3)]);
    /// ```
    pub fn reduce<F>(self, f: F) -> KeyedStream<Key, Out, impl Operator<(Key, Out)>>
    where
        F: Fn(&mut Out, Out) + Send + Clone + 'static,
    {
        self.fold(None, move |acc, value| match acc {
            None => *acc = Some(value),
            Some(acc) => f(acc, value),
        })
        .map(|(_, value)| value.unwrap())
    }
}
