use crate::operator::window::WindowDescription;
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, KeyedWindowedStream, Stream, WindowedStream};

impl<Key: DataKey, Out: Data + Ord, WindowDescr, OperatorChain>
    KeyedWindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Key, Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Returns the maximum element of each window.
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
    ///     .max()
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0.max(2)), (0, 4), (1, 1.max(3))]);
    /// ```
    pub fn max(self) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>> {
        self.add_generic_window_operator("WindowMax", |window| {
            window.items().max().unwrap().clone()
        })
    }
}

impl<Out: Data + Ord, WindowDescr, OperatorChain>
    WindowedStream<Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<(), Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<(), Out>> + 'static,
{
    pub fn max(self) -> Stream<Out, impl Operator<Out>> {
        self.inner.max().drop_key()
    }
}
