use crate::operator::{ExchangeData, Operator};
use crate::stream::Stream;

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Reduce the stream into a stream that emits a single value.
    ///
    /// The reducing operator consists in adding to the current accumulation value  the value of the
    /// current item in the stream.
    ///
    /// The reducing function is provided with a mutable reference to the current accumulator and the
    /// owned item of the stream. The function should modify the accumulator without returning
    /// anything.
    ///
    /// Note that the output type must be the same as the input type, if you need a different type
    /// consider using [`Stream::fold`].
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator is not parallelized, it creates a bottleneck where all the stream
    /// elements are sent to and the folding is done using a single thread.
    ///
    /// **Note**: this is very similar to [`Iteartor::reduce`](std::iter::Iterator::reduce).
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.reduce(|acc, value| *acc += value).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0 + 1 + 2 + 3 + 4]);
    /// ```
    pub fn reduce<F>(self, f: F) -> Stream<Out, impl Operator<Out>>
    where
        F: Fn(&mut Out, Out) + Send + Clone + 'static,
    {
        self.fold(None, move |acc, value| match acc {
            None => *acc = Some(value),
            Some(acc) => f(acc, value),
        })
        .map(|value| value.unwrap())
    }

    /// Reduce the stream into a stream that emits a single value.
    ///
    /// The reducing operator consists in adding to the current accumulation value the value of the
    /// current item in the stream.
    ///
    /// This method is very similary to [`Stream::reduce`], but performs the reduction distributely.
    /// To do so the reducing function must be _associative_, in particular the reducing process is
    /// performed in 2 steps:
    ///
    /// - local: the reducing function is used to reduce the elements present in each replica of
    ///   the stream independently.
    /// - global: all the partial results (the elements produced by the local step) have to be
    ///   aggregated into a single result.
    ///
    /// Note that the output type must be the same as the input type, if you need a different type
    /// consider using [`Stream::fold_assoc`].
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.reduce_assoc(|acc, value| *acc += value).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0 + 1 + 2 + 3 + 4]);
    /// ```
    pub fn reduce_assoc<F>(self, f: F) -> Stream<Out, impl Operator<Out>>
    where
        F: Fn(&mut Out, Out) + Send + Clone + 'static,
    {
        let f2 = f.clone();

        self.fold_assoc(
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
        .map(|value| value.unwrap())
    }
}
