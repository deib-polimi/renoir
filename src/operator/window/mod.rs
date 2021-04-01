use std::collections::VecDeque;

pub use count_window::*;

use crate::operator::{Data, Operator, Reorder, StreamElement, Timestamp};
use crate::stream::{Stream, WindowedStream};

mod count_window;
mod first;

pub trait WindowDescription<Out: Data> {
    type GeneratorType: WindowGenerator<Out> + Clone + 'static;
    fn into_generator(self) -> Self::GeneratorType;
}

pub trait WindowGenerator<Out: Data> {
    fn add(&mut self, item: StreamElement<Out>);
    fn next_window(&mut self) -> Option<Window<Out>>;
    fn advance(&mut self);
    fn buffer(&self) -> &VecDeque<Out>;
    fn to_string(&self) -> String;
}

pub struct Window<'a, Out: Data> {
    gen: &'a mut dyn WindowGenerator<Out>,
    extra_items: Vec<StreamElement<Out>>,
    size: usize,
    timestamp: Option<Timestamp>,
}

impl<'a, Out: Data> Window<'a, Out> {
    fn items(&self) -> impl Iterator<Item = &Out> {
        self.gen.buffer().iter().take(self.size)
    }
}

impl<'a, Out: Data> Drop for Window<'a, Out> {
    fn drop(&mut self) {
        self.gen.advance();
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn window<WinDescr: WindowDescription<Out>>(
        self,
        window: WinDescr,
    ) -> WindowedStream<Out, impl Operator<Out>, WinDescr> {
        WindowedStream {
            inner: self.add_operator(Reorder::new),
            window,
        }
    }
}
