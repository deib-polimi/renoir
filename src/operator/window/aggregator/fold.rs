use crate::operator::window::WindowDescription;
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, KeyedWindowedStream, Stream, WindowedStream};

impl<Key: DataKey, Out: Data, WindowDescr, OperatorChain>
    KeyedWindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Key, Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Folds the elements of each window into an accumulator value
    ///
    /// `fold()` takes two arguments: the initial value of the accumulator and a closure used to
    /// accumulate the elements of each window.
    ///
    /// The closure is called once for each element of each window with two arguments: a mutable
    /// reference to the accumulator and the element of the window. The closure should modify
    /// the accumulator, without returning anything.
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
    ///     .fold(1, |acc, &n| *acc *= n)
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0 * 2), (0, 4), (1, 1 * 3)]);
    /// ```
    pub fn fold<NewOut: Data, F>(
        self,
        init: NewOut,
        fold: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        F: Fn(&mut NewOut, &Out) + Clone + Send + 'static,
    {
        self.add_generic_window_operator("WindowFold", move |window| {
            let mut res = init.clone();
            for value in window.items() {
                (fold)(&mut res, value);
            }
            res
        })
    }
}

impl<Out: Data, WindowDescr, OperatorChain> WindowedStream<Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<(), Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<(), Out>> + 'static,
{
    /// Folds the elements of each window into an accumulator value
    ///
    /// `fold()` takes two arguments: the initial value of the accumulator and a closure used to
    /// accumulate the elements of each window.
    ///
    /// The closure is called once for each element of each window with two arguments: a mutable
    /// reference to the accumulator and the element of the window. The closure should modify
    /// the accumulator, without returning anything.
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
    ///     .fold(1, |acc, &n| *acc *= n)
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// assert_eq!(res, vec![(0 * 1 * 2), (2 * 3 * 4), (4)]);
    /// ```
    pub fn fold<NewOut: Data, F>(
        self,
        init: NewOut,
        fold: F,
    ) -> Stream<NewOut, impl Operator<NewOut>>
    where
        F: Fn(&mut NewOut, &Out) + Clone + Send + 'static,
    {
        self.inner.fold(init, fold).drop_key()
    }
}
