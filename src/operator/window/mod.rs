//! The types related to the windowed streams.

use std::collections::VecDeque;
use std::fmt::Display;
use std::marker::PhantomData;

pub use descr::*;
// pub use aggregator::*;
// pub use description::*;
use hashbrown::HashMap;

use crate::block::OperatorStructure;
use crate::operator::{Data, DataKey, ExchangeData, Operator, StreamElement, Timestamp};
use crate::stream::{KeyValue, KeyedStream, WindowedStream, Stream};

// mod aggregator;
mod descr;
mod aggr;
// mod generic_operator;
// #[cfg(feature = "timestamp")]
// mod processing_time;
// mod window_manage

pub trait WindowBuilder {
    type Manager<A: WindowAccumulator>: WindowManager<In = A::In, Out = A::Out> + 'static;

    fn build<A: WindowAccumulator>(&self, accumulator: A) -> Self::Manager<A>;
}

pub trait WindowAccumulator: Clone + Send + 'static {
    type In: Data;
    type Out: Data;

    fn process(&mut self, el: Self::In);
    fn output(self) -> Self::Out;
}

#[derive(Clone)]
pub(crate) struct KeyedWindowManager<Key, In, Out, W: WindowManager> {
    windows: HashMap<Key, W>,
    init: W,
    _in: PhantomData<In>,
    _out: PhantomData<Out>,
}

pub trait WindowManager: Clone + Send {
    type In: Data;
    type Out: Data;
    type Output: IntoIterator<Item = WindowResult<Self::Out>>;
    fn process(&mut self, el: StreamElement<Self::In>) -> Self::Output;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowResult<T> {
    Item(T),
    Timestamped(T, Timestamp),
}

impl<T> WindowResult<T> {
    pub fn new(item: T, timestamp: Option<Timestamp>) -> Self {
        match timestamp {
            Some(ts) => WindowResult::Timestamped(item, ts),
            None => WindowResult::Item(item),
        }
    }
}

impl<T> From<WindowResult<T>> for StreamElement<T> {
    fn from(value: WindowResult<T>) -> Self {
        match value {
            WindowResult::Item(item) => StreamElement::Item(item),
            WindowResult::Timestamped(item, ts) => StreamElement::Timestamped(item, ts),
        }
    }
}

/// This operator abstracts the window logic as an operator and delegates to the
/// `KeyedWindowManager` and a `ProcessFunc` the job of building and processing the windows,
/// respectively.
#[derive(Clone)]
pub(crate) struct WindowOperator<Key, In, Out, Prev, W>
where
    W: WindowManager,
{
    /// The previous operators in the chain.
    prev: Prev,
    /// The name of the actual operator that this one abstracts.
    ///
    /// It is used only for tracing purposes.
    name: String,
    /// The manager that will build the windows.
    manager: KeyedWindowManager<Key, In, Out, W>,
    /// A buffer for storing ready items.
    output_buffer: VecDeque<StreamElement<KeyValue<Key, Out>>>,
}

impl<Key, In, Out, Prev, W> Display for WindowOperator<Key, In, Out, Prev, W>
where
    W: WindowManager,
    Prev: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> {} -> WindowOperator[{}]<{}>",
            self.prev,
            std::any::type_name::<W>(),
            self.name,
            std::any::type_name::<Out>(),
        )
    }
}

impl<Key, In, Out, Prev, W> Operator<KeyValue<Key, Out>> for WindowOperator<Key, In, Out, Prev, W>
where
    W: WindowManager<In = In, Out = Out> + Send,
    Prev: Operator<KeyValue<Key, In>>,
    Key: DataKey,
    In: Data,
    Out: Data,
{
    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<KeyValue<Key, Out>> {
        loop {
            if let Some(item) = self.output_buffer.pop_front() {
                return item;
            }

            let el = self.prev.next();
            match el {
                el @ (StreamElement::Item(_) | StreamElement::Timestamped(_, _)) => {
                    let (key, el) = el.take_key();
                    let key = key.unwrap();
                    let mgr = self
                        .manager
                        .windows
                        .entry(key.clone())
                        .or_insert_with(|| self.manager.init.clone());

                    let ret = mgr.process(el);
                    self.output_buffer.extend(
                        ret.into_iter()
                            .map(|e| StreamElement::from(e).add_key(key.clone())),
                    );
                }
                el => {
                    let (_, el) = el.take_key();
                    for (key, mgr) in &mut self.manager.windows {
                        let ret = mgr.process(el.clone());
                        self.output_buffer.extend(
                            ret.into_iter()
                                .map(|e| StreamElement::from(e).add_key(key.clone())),
                        );
                    }
                }
            }
        }
    }

    fn structure(&self) -> crate::block::BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<KeyValue<Key, Out>, _>(&self.name))
    }
}

impl<Key, In, Out, Prev, W> WindowOperator<Key, In, Out, Prev, W>
where
    W: WindowManager,
{
    pub(crate) fn new(
        prev: Prev,
        name: String,
        manager: KeyedWindowManager<Key, In, Out, W>,
    ) -> Self {
        Self {
            prev,
            name,
            manager,
            output_buffer: Default::default(),
        }
    }
}

impl<Key, Out, WindowDescr, OperatorChain>
    WindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowBuilder,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
    Key: DataKey,
    Out: Data,
{
    /// Add a new generic window operator to a `KeyedWindowedStream`,
    /// after adding a Reorder operator.
    /// This should be used by every custom window aggregator.
    pub(crate) fn add_window_operator<A, NewOut>(
        self,
        name: &str,
        accumulator: A,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        NewOut: Data,
        A: WindowAccumulator<In = Out, Out = NewOut>,
    {
        let stream = self.inner;
        let init = self.descr.build::<A>(accumulator);

        let manager: KeyedWindowManager<Key, Out, NewOut, WindowDescr::Manager<A>> =
            KeyedWindowManager {
                windows: HashMap::default(),
                init,
                _in: PhantomData,
                _out: PhantomData,
            };

        stream // .add_operator(Reorder::new)
            .add_operator(|prev| WindowOperator::new(prev, name.into(), manager))
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Apply a window to the stream.
    ///
    /// Returns a [`KeyedWindowedStream`], with windows created following the behavior specified
    /// by the passed [`WindowDescription`].
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::operator::window::CountWindow;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .group_by(|&n| n % 2)
    ///     .window(CountWindow::sliding(3, 2))
    ///     .sum()
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 4), (0, 0 + 2 + 4), (1, 1 + 3)]);
    /// ```
    pub fn window<WinOut: Data, WinDescr: WindowBuilder>(
        self,
        descr: WinDescr,
    ) -> WindowedStream<Key, Out, impl Operator<KeyValue<Key, Out>>, WinOut, WinDescr> {
        WindowedStream {
            inner: self,
            descr,
            _win_out: PhantomData,
        }
    }
}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Apply a window to the stream.
    ///
    /// Returns a [`WindowedStream`], with windows created following the behavior specified
    /// by the passed [`WindowDescription`].
    ///
    /// **Note**: this operator cannot be parallelized, so all the stream elements are sent to a
    /// single node where the creation and aggregation of the windows are done.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::operator::window::CountWindow;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .window_all(CountWindow::tumbling(2))
    ///     .sum()
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// assert_eq!(res, vec![0 + 1, 2 + 3, 4]);
    /// ```
    pub fn window_all<WinOut: Data, WinDescr: WindowBuilder>(
        self,
        descr: WinDescr,
    ) -> WindowedStream<(), Out, impl Operator<KeyValue<(), Out>>, WinOut, WinDescr> {
        // max_parallelism and key_by are used instead of group_by so that there is exactly one
        // replica, since window_all cannot be parallelized
        self.max_parallelism(1).key_by(|_| ()).window(descr)
    }
}
