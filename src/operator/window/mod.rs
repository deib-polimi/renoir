//! The types related to the windowed streams.

use std::collections::{HashMap, VecDeque};
use std::fmt::Display;
use std::marker::PhantomData;

pub use descr::*;
// pub use aggregator::*;
// pub use description::*;

use crate::block::{GroupHasherBuilder, OperatorStructure, Replication};
use crate::operator::{Data, DataKey, ExchangeData, Operator, StreamElement, Timestamp};
use crate::stream::{KeyedStream, Stream, WindowedStream};

mod aggr;
mod descr;

/// Trait for a window description that can be used to instantiate windows.
/// The struct implementing this trait specifies the kind of [`WindowManager`] that will be instantiated by
/// it and provides a method through which the
/// Convention: WindowAccumulator expects output to be called after at least one element has been processed.
/// Violating this convention may result in panics.
pub trait WindowDescription<T> {
    /// WindowManager corresponding to the WindowDescription
    type Manager<A: WindowAccumulator<In = T>>: WindowManager<In = T, Out = A::Out> + 'static;
    /// Build a window manager that dispatches elements of each window to a clone of the
    /// accumulator passed as parameter
    fn build<A: WindowAccumulator<In = T>>(&self, accumulator: A) -> Self::Manager<A>;
}

/// Trait for operations that can be performed on windows. Operations must be incremental
/// processing on element at a time. If the operation requires accessing elements in random
/// order, they should first be collected then the operation can be finizalized on the collection
///
/// Convention: output will always be called after at least one element has been processed
pub trait WindowAccumulator: Clone + Send + 'static {
    type In: Data;
    type Out: Data;

    /// Process a single input element updating the state of the accumulator
    fn process(&mut self, el: &Self::In);
    /// Finalize the accumulator and produce a result
    fn output(self) -> Self::Out;
}

#[derive(Clone)]
pub(crate) struct KeyedWindowManager<Key, In, Out, W: WindowManager> {
    windows: HashMap<Key, W, GroupHasherBuilder>,
    init: W,
    _in: PhantomData<In>,
    _out: PhantomData<Out>,
}

/// Window Managers handle the windowing logic for a single partition (group) of the stream.
///
/// Elements passing on the stream partition will be fed to the manager through the `process`
/// method, the manager should then instantiate any new window if needed depending on its
/// logic and on the element passed as input. The input elements should be forwarded to any
/// relevant active window and the outputs for any window that has been closed after the
/// input event should be output as return value of the `process` function.
pub trait WindowManager: Clone + Send {
    /// Type of the input elements
    type In: Data;
    /// Type of the output produced by each window
    type Out: Data;
    /// Type of the output of a call to `process`, it may be
    /// a single [`WindowResult`] or an iterable collection, depending
    /// on the windowing logic. (A single input may trigger the simultanous closure of
    /// multiple windows)
    type Output: IntoIterator<Item = WindowResult<Self::Out>>;
    /// Process an input element updating any interest window.
    /// Output the results that have become ready after processing this element.
    fn process(&mut self, el: StreamElement<Self::In>) -> Self::Output;
    /// Return true if the manager has no active windows and can be dropped
    fn recycle(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowResult<T> {
    Item(T),
    Timestamped(T, Timestamp),
}

impl<T> WindowResult<T> {
    #[inline]
    pub fn new(item: T, timestamp: Option<Timestamp>) -> Self {
        match timestamp {
            Some(ts) => WindowResult::Timestamped(item, ts),
            None => WindowResult::Item(item),
        }
    }

    #[inline]
    pub fn item(&self) -> &T {
        match self {
            WindowResult::Item(item) => item,
            WindowResult::Timestamped(item, _) => item,
        }
    }

    #[inline]
    pub fn unwrap_item(self) -> T {
        match self {
            WindowResult::Item(item) => item,
            WindowResult::Timestamped(item, _) => item,
        }
    }
}

impl<T> From<WindowResult<T>> for StreamElement<T> {
    #[inline]
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
    output_buffer: VecDeque<StreamElement<(Key, Out)>>,
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

impl<Key, In, Out, Prev, W> Operator for WindowOperator<Key, In, Out, Prev, W>
where
    W: WindowManager<In = In, Out = Out> + Send,
    Prev: Operator<Out = (Key, In)>,
    Key: DataKey,
    In: Data,
    Out: Data,
{
    type Out = (Key, Out);

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<(Key, Out)> {
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
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                el => {
                    let (_, el) = el.take_key();

                    self.manager.windows.retain(|key, mgr| {
                        let ret = mgr.process(el.clone());
                        self.output_buffer.extend(
                            ret.into_iter()
                                .map(|e| StreamElement::from(e).add_key(key.clone())),
                        );
                        !mgr.recycle()
                    });

                    // Forward system messages and watermarks
                    let msg = match el {
                        StreamElement::Watermark(w) => StreamElement::Watermark(w),
                        StreamElement::Terminate => StreamElement::Terminate,
                        StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
                        _ => unreachable!(),
                    };
                    self.output_buffer.push_back(msg);
                }
            }
        }
    }

    fn structure(&self) -> crate::block::BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<(Key, Out), _>(&self.name))
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

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Out>,
    OperatorChain: Operator<Out = (Key, Out)> + 'static,
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
    ) -> KeyedStream<impl Operator<Out = (Key, NewOut)>>
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

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<OperatorChain>
where
    OperatorChain: Operator<Out = (Key, Out)> + 'static,
{
    /// Apply a window to the stream.
    ///
    /// Returns a [`WindowedStream`], with windows created following the behavior specified
    /// by the passed [`WindowDescription`].
    ///
    /// ## Example
    /// ```
    /// # use renoir::{StreamContext, RuntimeConfig};
    /// # use renoir::operator::source::IteratorSource;
    /// # use renoir::operator::window::CountWindow;
    /// # let mut env = StreamContext::new_local();
    /// let s = env.stream_iter(0..9);
    /// let res = s
    ///     .group_by(|&n| n % 2)
    ///     .window(CountWindow::sliding(3, 2))
    ///     .sum()
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0 + 2 + 4), (0, 4 + 6 + 8), (1, 1 + 3 + 5)]);
    /// ```
    pub fn window<WinOut: Data, WinDescr: WindowDescription<Out>>(
        self,
        descr: WinDescr,
    ) -> WindowedStream<impl Operator<Out = (Key, Out)>, WinOut, WinDescr> {
        WindowedStream {
            inner: self,
            descr,
            _win_out: PhantomData,
        }
    }
}

impl<Out: ExchangeData, OperatorChain> Stream<OperatorChain>
where
    OperatorChain: Operator<Out = Out> + 'static,
{
    /// Send all elements to a single node and apply a window to the stream.
    ///
    /// Returns a [`WindowedStream`], with key `()` with windows created following the behavior specified
    /// by the passed [`WindowDescription`].
    ///
    /// **Note**: this operator cannot be parallelized, so all the stream elements are sent to a
    /// single node where the creation and aggregation of the windows are done.
    ///
    /// ## Example
    /// ```
    /// # use renoir::{StreamContext, RuntimeConfig};
    /// # use renoir::operator::source::IteratorSource;
    /// # use renoir::operator::window::CountWindow;
    /// # let mut env = StreamContext::new_local();
    /// let s = env.stream_iter(0..5usize);
    /// let res = s
    ///     .window_all(CountWindow::tumbling(2))
    ///     .sum::<usize>()
    ///     .drop_key()
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// assert_eq!(res, vec![0 + 1, 2 + 3]);
    /// ```
    pub fn window_all<WinOut: Data, WinDescr: WindowDescription<Out>>(
        self,
        descr: WinDescr,
    ) -> WindowedStream<impl Operator<Out = ((), Out)>, WinOut, WinDescr> {
        // replication and key_by are used instead of group_by so that there is exactly one
        // replica, since window_all cannot be parallelized
        self.replication(Replication::new_one())
            .key_by(|_| ())
            .window(descr)
    }
}
