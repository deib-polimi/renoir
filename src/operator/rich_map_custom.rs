use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

pub struct ElementGenerator<'a, Op> {
    inner: &'a mut Op,
}

impl<'a, Op: Operator> ElementGenerator<'a, Op> {
    pub fn new(inner: &'a mut Op) -> Self {
        Self { inner }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> StreamElement<Op::Out> {
        self.inner.next()
    }
}

#[derive(Debug)]
pub struct RichMapCustom<O: Send, F, Op>
where
    F: FnMut(ElementGenerator<Op>) -> StreamElement<O> + Clone + Send,
    Op: Operator,
{
    prev: Op,
    map_fn: F,
    _new_out: PhantomData<O>,
}

impl<O: Send, F: Clone, Op: Clone> Clone for RichMapCustom<O, F, Op>
where
    F: FnMut(ElementGenerator<Op>) -> StreamElement<O> + Clone + Send,
    Op: Operator,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            map_fn: self.map_fn.clone(),
            _new_out: PhantomData,
        }
    }
}

impl<O: Send, F, Op> Display for RichMapCustom<O, F, Op>
where
    F: FnMut(ElementGenerator<Op>) -> StreamElement<O> + Clone + Send,
    Op: Operator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> RichMapCustom<{} -> {}>",
            self.prev,
            std::any::type_name::<Op::Out>(),
            std::any::type_name::<O>()
        )
    }
}

impl<O: Send, F, Op> RichMapCustom<O, F, Op>
where
    F: FnMut(ElementGenerator<Op>) -> StreamElement<O> + Clone + Send,
    Op: Operator,
{
    pub(super) fn new(prev: Op, f: F) -> Self {
        Self {
            prev,
            map_fn: f,
            _new_out: Default::default(),
        }
    }
}

impl<O: Send, F, Op> Operator for RichMapCustom<O, F, Op>
where
    F: FnMut(ElementGenerator<Op>) -> StreamElement<O> + Clone + Send,
    Op: Operator,
{
    type Out = O;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<O> {
        let eg = ElementGenerator::new(&mut self.prev);
        (self.map_fn)(eg)
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<O, _>("RichMapCustom"))
    }
}
