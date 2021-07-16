use std::collections::VecDeque;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::source::Source;
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

/// A fake operator that can be used to unit-test the operators.
#[derive(Debug, Clone)]
pub struct FakeOperator<Out: Data> {
    /// The data to return from `next()`.
    buffer: VecDeque<StreamElement<Out>>,
}

impl<Out: Data> FakeOperator<Out> {
    /// Create an empty `FakeOperator`.
    pub fn empty() -> Self {
        Self {
            buffer: Default::default(),
        }
    }

    /// Create a `FakeOperator` with the specified data.
    pub fn new<I: Iterator<Item = Out>>(data: I) -> Self {
        Self {
            buffer: data.map(StreamElement::Item).collect(),
        }
    }

    /// Add an element to the end of the list of elements to return from `next`.
    pub fn push(&mut self, item: StreamElement<Out>) {
        self.buffer.push_back(item);
    }
}

impl<Out: Data> Operator<Out> for FakeOperator<Out> {
    fn setup(&mut self, _metadata: ExecutionMetadata) {}

    fn next(&mut self) -> StreamElement<Out> {
        if let Some(item) = self.buffer.pop_front() {
            item
        } else {
            StreamElement::Terminate
        }
    }

    fn to_string(&self) -> String {
        format!("FakeOperator<{}>", std::any::type_name::<Out>())
    }

    fn structure(&self) -> BlockStructure {
        BlockStructure::default().add_operator(OperatorStructure::new::<Out, _>("FakeOperator"))
    }
}

impl<Out: Data> Source<Out> for FakeOperator<Out> {
    fn get_max_parallelism(&self) -> Option<usize> {
        None
    }
}
