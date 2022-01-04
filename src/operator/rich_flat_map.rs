use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Apply a mapping operation to each element of the stream, the resulting stream will be the
    /// flattened values of the result of the mapping. The mapping function can be stateful.
    ///
    /// This is equivalent to [`Stream::flat_map`] but with a stateful function.
    ///
    /// Since the mapping function can be stateful, it is a `FnMut`. This allows expressing simple
    /// algorithms with very few lines of code (see examples).
    ///
    /// The mapping function is _cloned_ inside each replica, and they will not share state between
    /// each other. If you want that only a single replica handles all the items you may want to
    /// change the parallelism of this operator with [`Stream::max_parallelism`].
    ///
    /// ## Examples
    ///
    /// This will emit only the _positive prefix-sums_.
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..=3)));
    /// let res = s.rich_flat_map({
    ///     let mut elements = Vec::new();
    ///     move |y| {
    ///         let new_pairs = elements
    ///             .iter()
    ///             .map(|&x: &u32| (x, y))
    ///             .collect::<Vec<_>>();
    ///         elements.push(y);
    ///         new_pairs
    ///     }
    /// }).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![(0, 1), (0, 2), (1, 2), (0, 3), (1, 3), (2, 3)]);
    /// ```
    pub fn rich_flat_map<MapOut: Data, NewOut: Data, F>(
        self,
        f: F,
    ) -> Stream<NewOut, impl Operator<NewOut>>
    where
        MapOut: IntoIterator<Item = NewOut>,
        <MapOut as IntoIterator>::IntoIter: Clone + Send + 'static,
        F: FnMut(Out) -> MapOut + Send + Clone + 'static,
    {
        self.rich_map(f).flatten()
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Apply a mapping operation to each element of the stream, the resulting stream will be the
    /// flattened values of the result of the mapping. The mapping function can be stateful.
    ///
    /// This is exactly like [`Stream::rich_flat_map`], but the function is cloned for each key.
    /// This means that each key will have a unique mapping function (and therefore a unique state).
    pub fn rich_flat_map<NewOut: Data, MapOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        MapOut: IntoIterator<Item = NewOut>,
        <MapOut as IntoIterator>::IntoIter: Clone + Send + 'static,
        F: FnMut(KeyValue<&Key, Out>) -> MapOut + Clone + Send + 'static,
    {
        self.rich_map(f).flatten()
    }
}
