//! The types related to the windowed streams.

use std::collections::VecDeque;
use std::fmt::Display;
use std::marker::PhantomData;

// pub use aggregator::*;
// pub use description::*;
use hashbrown::HashMap;

use crate::block::OperatorStructure;
use crate::operator::{Data, DataKey, ExchangeData, Operator, StreamElement, Timestamp};
use crate::stream::{KeyValue, KeyedStream, KeyedWindowedStream, Stream, WindowedStream};

use super::super::*;

pub(crate) struct FoldWrap<I, S, F>
where
    F: FnMut(&mut S, I),
{
    state: S,
    f: F,
    _in: PhantomData<I>,
}

impl<I, S, F> FoldWrap<I, S, F>
where
    F: FnMut(&mut S, I),
{
    pub(crate) fn new(state: S, f: F) -> Self {
        Self {
            state,
            f,
            _in: PhantomData,
        }
    }
}

impl<I, S: Clone, F: Clone> Clone for FoldWrap<I, S, F>
where
    F: FnMut(&mut S, I),
{
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            f: self.f.clone(),
            _in: PhantomData,
        }
    }
}

impl<I, S, F> WindowAccumulator for FoldWrap<I, S, F>
where
    I: Clone + Send + 'static,
    F: FnMut(&mut S, I) + Clone + Send + 'static,
    S: Clone + Send + 'static,
{
    type In = I;

    type Out = S;

    fn process(&mut self, el: Self::In) {
        (self.f)(&mut self.state, el);
    }

    fn output(self) -> Self::Out {
        self.state
    }
}

impl<Key, Out, WindowDescr, OperatorChain>
    KeyedWindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowBuilder,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
    Key: DataKey,
    Out: Data,
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
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::operator::window::CountWindow;
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
        F: FnMut(&mut NewOut, Out) + Clone + Send + 'static,
    {
        let acc = FoldWrap::new(init, fold);
        self.add_window_operator("WindowFold", acc)
    }
}
