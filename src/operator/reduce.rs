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
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream_iter(0..5);
    /// let res = s.reduce(|a, b| a + b).collect::<Vec<_>>();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0 + 1 + 2 + 3 + 4]);
    /// ```
    pub fn reduce<F>(self, f: F) -> Stream<Out, impl Operator<Out>>
    where
        F: Fn(Out, Out) -> Out + Send + Clone + 'static,
    {
        self.fold(None, move |acc, b| {
            *acc = Some(if let Some(a) = acc.take() { f(a, b) } else { b })
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
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.reduce_assoc(|a, b| a + b).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0 + 1 + 2 + 3 + 4]);
    /// ```
    pub fn reduce_assoc<F>(self, f: F) -> Stream<Out, impl Operator<Out>>
    where
        F: Fn(Out, Out) -> Out + Send + Clone + 'static,
    {
        let f2 = f.clone();

        self.fold_assoc(
            None,
            move |acc, b| *acc = Some(if let Some(a) = acc.take() { f(a, b) } else { b }),
            move |acc1, mut acc2| {
                *acc1 = match (acc1.take(), acc2.take()) {
                    (Some(a), Some(b)) => Some(f2(a, b)),
                    (None, Some(a)) | (Some(a), None) => Some(a),
                    (None, None) => None,
                }
            },
        )
        .map(|value| value.unwrap())
    }
}
