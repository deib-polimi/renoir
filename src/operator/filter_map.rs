use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, Operator};

use crate::ExecutionMetadata;

use super::StreamElement;

#[derive(Clone)]
pub struct FilterMap<In: Data, Out: Data, PreviousOperator, Predicate>
where
    Predicate: Fn(In) -> Option<Out> + Send + Clone + 'static,
    PreviousOperator: Operator<In> + 'static,
{
    prev: PreviousOperator,
    predicate: Predicate,
    _in: PhantomData<In>,
    _out: PhantomData<Out>,
}

impl<In: Data, Out: Data, PreviousOperator, Predicate> Display
    for FilterMap<In, Out, PreviousOperator, Predicate>
where
    Predicate: Fn(In) -> Option<Out> + Send + Clone + 'static,
    PreviousOperator: Operator<In> + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> FilterMap<{}>",
            self.prev,
            std::any::type_name::<Out>()
        )
    }
}

impl<In: Data, Out: Data, PreviousOperator, Predicate>
    FilterMap<In, Out, PreviousOperator, Predicate>
where
    Predicate: Fn(In) -> Option<Out> + Send + Clone + 'static,
    PreviousOperator: Operator<In> + 'static,
{
    pub(super) fn new(prev: PreviousOperator, predicate: Predicate) -> Self {
        Self {
            prev,
            predicate,
            _in: Default::default(),
            _out: Default::default(),
        }
    }
}

impl<In: Data, Out: Data, PreviousOperator, Predicate> Operator<Out>
    for FilterMap<In, Out, PreviousOperator, Predicate>
where
    Predicate: Fn(In) -> Option<Out> + Send + Clone + 'static,
    PreviousOperator: Operator<In> + 'static,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Out> {
        loop {
            match self.prev.next() {
                StreamElement::Item(item) => {
                    if let Some(el) = (self.predicate)(item) {
                        return StreamElement::Item(el);
                    }
                }
                StreamElement::Timestamped(item, ts) => {
                    if let Some(el) = (self.predicate)(item) {
                        return StreamElement::Timestamped(el, ts);
                    }
                }
                StreamElement::Watermark(w) => return StreamElement::Watermark(w),
                StreamElement::Terminate => return StreamElement::Terminate,
                StreamElement::FlushAndRestart => return StreamElement::FlushAndRestart,
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Out, _>("FilterMap"))
    }
}
