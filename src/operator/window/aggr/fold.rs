use std::marker::PhantomData;

use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyedStream, WindowedStream};

use super::super::*;

#[derive(Clone)]
pub(crate) struct Fold<I, S, F>
where
    F: FnMut(&mut S, I),
{
    state: S,
    f: F,
    _in: PhantomData<I>,
}

impl<I, S, F> Fold<I, S, F>
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

impl<I, S, F> WindowAccumulator for Fold<I, S, F>
where
    I: Clone + Send + 'static,
    S: Clone + Send + 'static,
    F: FnMut(&mut S, I) + Clone + Send + 'static,
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

#[derive(Clone)]
pub(crate) struct FoldFirst<I, F>
where
    F: FnMut(&mut I, I),
{
    state: Option<I>,
    f: F,
}

impl<I, F> FoldFirst<I, F>
where
    F: FnMut(&mut I, I),
{
    pub(crate) fn new(f: F) -> Self {
        Self { state: None, f }
    }
}

impl<I, F> WindowAccumulator for FoldFirst<I, F>
where
    I: Clone + Send + 'static,
    F: FnMut(&mut I, I) + Clone + Send + 'static,
{
    type In = I;
    type Out = I;

    #[inline]
    fn process(&mut self, el: Self::In) {
        match self.state.as_mut() {
            None => self.state = Some(el),
            Some(s) => (self.f)(s, el),
        }
    }

    #[inline]
    fn output(self) -> Self::Out {
        self.state
            .expect("FoldFirst output called when it has received no elements!")
    }
}

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Out>,
    OperatorChain: Operator<(Key, Out)> + 'static,
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
    ///     .fold(1, |acc, n| *acc *= n)
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0 * 2), (1, 1 * 3)]);
    /// ```
    pub fn fold<NewOut: Data, F>(
        self,
        init: NewOut,
        fold: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<(Key, NewOut)>>
    where
        F: FnMut(&mut NewOut, Out) + Clone + Send + 'static,
    {
        let acc = Fold::new(init, fold);
        self.add_window_operator("WindowFold", acc)
    }

    /// Folds the elements of each window into an accumulator value, starting with the first value
    ///
    /// TODO DOCS
    ///
    pub fn fold_first<F>(self, fold: F) -> KeyedStream<Key, Out, impl Operator<(Key, Out)>>
    where
        F: FnMut(&mut Out, Out) + Clone + Send + 'static,
    {
        let acc = FoldFirst::new(fold);
        self.add_window_operator("WindowFoldFirst", acc)
    }
}
