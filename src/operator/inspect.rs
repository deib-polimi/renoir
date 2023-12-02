use std::fmt::Display;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Inspect<F, Op>
where
    F: FnMut(&Op::Out) + Send + Clone,
    Op: Operator,
{
    prev: Op,
    #[derivative(Debug = "ignore")]
    f: F,
}

impl<F, Op> Inspect<F, Op>
where
    F: FnMut(&Op::Out) + Send + Clone,
    Op: Operator,
{
    pub fn new(prev: Op, f: F) -> Self {
        Self { prev, f }
    }
}

impl<F, Op> Display for Inspect<F, Op>
where
    F: FnMut(&Op::Out) + Send + Clone,
    Op: Operator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> Inspect", self.prev)
    }
}

impl<F, Op> Operator for Inspect<F, Op>
where
    F: FnMut(&Op::Out) + Send + Clone,
    Op: Operator,
{
    type Out = Op::Out;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Self::Out> {
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
        let operator = OperatorStructure::new::<Op::Out, _>("Inspect");
        self.prev.structure().add_operator(operator)
    }
}
