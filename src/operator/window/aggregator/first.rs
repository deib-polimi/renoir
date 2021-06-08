use crate::operator::window::WindowDescription;
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, KeyedWindowedStream, Stream, WindowedStream};

impl<Key: DataKey, Out: Data, WindowDescr, OperatorChain>
    KeyedWindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Key, Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Returns the first element of each window.
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
    ///     .first()
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (0, 4), (1, 1)]);
    /// ```
    pub fn first(self) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>> {
        self.add_generic_window_operator("WindowFirst", |window| {
            window.items().next().unwrap().clone()
        })
    }
}

impl<Out: Data, WindowDescr, OperatorChain> WindowedStream<Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<(), Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<(), Out>> + 'static,
{
    pub fn first(self) -> Stream<Out, impl Operator<Out>> {
        self.inner.first().drop_key()
    }
}
