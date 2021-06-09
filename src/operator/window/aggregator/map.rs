use crate::operator::window::WindowDescription;
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, KeyedWindowedStream, Stream, WindowedStream};

impl<Key: DataKey, Out: Data, WindowDescr, OperatorChain>
    KeyedWindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Key, Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Takes a closure used to transform each window into a new element.
    /// The closure is called once for each window, passing an iterator over the elements of
    /// the window as argument.
    ///
    /// Returns a [`KeyedStream`] containing all the values returned by the closure.
    ///
    /// ## Example
    /// ```
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::IteratorSource;
    /// # use rstream::operator::window::CountWindow;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .group_by(|&n| n % 2)
    ///     .window(CountWindow::tumbling(2))
    ///     .map(|window| *window.last().unwrap())
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 2), (0, 4), (1, 3)]);
    /// ```
    pub fn map<NewOut: Data, F>(
        self,
        map_func: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        F: Fn(&mut dyn ExactSizeIterator<Item = &Out>) -> NewOut + Clone + Send + 'static,
    {
        self.add_generic_window_operator("WindowMap", move |window| (map_func)(&mut window.items()))
    }
}

impl<Out: Data, WindowDescr, OperatorChain> WindowedStream<Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<(), Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<(), Out>> + 'static,
{
    /// Takes a closure used to transform each window into a new element.
    /// The closure is called once for each window, passing an iterator over the elements of
    /// the window as argument.
    ///
    /// Returns a [`Stream`] containing all the values returned by the closure.
    ///
    /// ## Example
    /// ```
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::IteratorSource;
    /// # use rstream::operator::window::CountWindow;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .window_all(CountWindow::sliding(3, 2))
    ///     .map(|window| *window.last().unwrap())
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// assert_eq!(res, vec![2, 4, 4]);
    /// ```
    pub fn map<NewOut: Data, F>(self, map_func: F) -> Stream<NewOut, impl Operator<NewOut>>
    where
        F: Fn(&mut dyn ExactSizeIterator<Item = &Out>) -> NewOut + Clone + Send + 'static,
    {
        self.inner.map(map_func).drop_key()
    }
}
