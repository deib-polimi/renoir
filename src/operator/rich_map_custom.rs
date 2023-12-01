use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

pub struct ElementGenerator<'a, Out, Op> {
    inner: &'a mut Op,
    _out: PhantomData<Out>,
}

impl<'a, Out: Data, Op: Operator<Out = Out>> ElementGenerator<'a, Out, Op> {
    pub fn new(inner: &'a mut Op) -> Self {
        Self {
            inner,
            _out: PhantomData,
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> StreamElement<Out> {
        self.inner.next()
    }
}

#[derive(Clone, Debug)]
pub struct RichMapCustom<Out: Data, NewOut: Data, F, OperatorChain>
where
    F: FnMut(ElementGenerator<Out, OperatorChain>) -> StreamElement<NewOut> + Clone + Send,
    OperatorChain: Operator<Out = Out>,
{
    prev: OperatorChain,
    map_fn: F,
    _out: PhantomData<Out>,
    _new_out: PhantomData<NewOut>,
}

impl<Out: Data, NewOut: Data, F, OperatorChain> Display
    for RichMapCustom<Out, NewOut, F, OperatorChain>
where
    F: FnMut(ElementGenerator<Out, OperatorChain>) -> StreamElement<NewOut> + Clone + Send,
    OperatorChain: Operator<Out = Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> RichMapCustom<{} -> {}>",
            self.prev,
            std::any::type_name::<Out>(),
            std::any::type_name::<NewOut>()
        )
    }
}

impl<Out: Data, NewOut: Data, F, OperatorChain> RichMapCustom<Out, NewOut, F, OperatorChain>
where
    F: FnMut(ElementGenerator<Out, OperatorChain>) -> StreamElement<NewOut> + Clone + Send,
    OperatorChain: Operator<Out = Out>,
{
    pub(super) fn new(prev: OperatorChain, f: F) -> Self {
        Self {
            prev,
            map_fn: f,
            _out: Default::default(),
            _new_out: Default::default(),
        }
    }
}

impl<Out: Data, NewOut: Data, F, OperatorChain> Operator
    for RichMapCustom<Out, NewOut, F, OperatorChain>
where
    F: FnMut(ElementGenerator<Out, OperatorChain>) -> StreamElement<NewOut> + Clone + Send,
    OperatorChain: Operator<Out = Out>,
{
    type Out = NewOut;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<NewOut> {
        let eg = ElementGenerator::new(&mut self.prev);
        (self.map_fn)(eg)
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<NewOut, _>("RichMapCustom"))
    }
}
