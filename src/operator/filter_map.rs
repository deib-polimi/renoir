use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Remove from the stream all the elements for which the provided function returns `None` and
    /// keep the elements that returned `Some(_)`.
    ///
    /// **Note**: this is very similar to [`Iteartor::filter_map`](std::iter::Iterator::filter_map)
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// let res = s.filter_map(|n| if n % 2 == 0 { Some(n * 3) } else { None }).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0, 6, 12, 18, 24])
    /// ```
    pub fn filter_map<NewOut: Data, F>(self, f: F) -> Stream<NewOut, impl Operator<NewOut>>
    where
        F: Fn(Out) -> Option<NewOut> + Send + Clone + 'static,
    {
        self.map(f).filter(|x| x.is_some()).map(|x| x.unwrap())
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Remove from the stream all the elements for which the provided function returns `None` and
    /// keep the elements that returned `Some(_)`.
    ///
    /// **Note**: this is very similar to [`Iteartor::filter_map`](std::iter::Iterator::filter_map)
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10))).group_by(|&n| n % 2);
    /// let res = s.filter_map(|(_key, n)| if n % 3 == 0 { Some(n * 4) } else { None }).collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (0, 24), (1, 12), (1, 36)]);
    /// ```
    pub fn filter_map<NewOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        F: Fn(KeyValue<&Key, Out>) -> Option<NewOut> + Send + Clone + 'static,
    {
        self.map(f)
            .filter(|(_, x)| x.is_some())
            .map(|(_, x)| x.unwrap())
    }
}
