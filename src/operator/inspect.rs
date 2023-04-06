use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Inspect<Out: Data, F, PreviousOperators>
where
    F: FnMut(&Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    #[derivative(Debug = "ignore")]
    f: F,
    _out: PhantomData<Out>,
}

impl<Out: Data, F, PreviousOperators> Inspect<Out, F, PreviousOperators>
where
    F: FnMut(&Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    pub fn new(prev: PreviousOperators, f: F) -> Self {
        Self {
            prev,
            f,
            _out: PhantomData,
        }
    }
}

impl<Out: Data, F, PreviousOperators> Display for Inspect<Out, F, PreviousOperators>
where
    F: FnMut(&Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> Inspect", self.prev)
    }
}

impl<Out: Data, F, PreviousOperators> Operator<Out> for Inspect<Out, F, PreviousOperators>
where
    F: FnMut(&Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Out> {
        let el = self.prev.next();
        match &el {
            StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                (self.f)(t);
            }
            _ => {}
        }
        el
    }

    fn structure(&self) -> BlockStructure {
        let operator = OperatorStructure::new::<Out, _>("Inspect");
        self.prev.structure().add_operator(operator)
    }
}
