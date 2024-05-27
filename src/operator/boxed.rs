use std::fmt::Display;

use dyn_clone::DynClone;

use crate::{
    block::BlockStructure,
    operator::{Data, Operator, StreamElement},
    ExecutionMetadata, Stream,
};

pub(crate) trait DynOperator: DynClone {
    type Out: Data;
    /// Setup the operator chain. This is called before any call to `next` and it's used to
    /// initialize the operator. When it's called the operator has already been cloned and it will
    /// never be cloned again. Therefore it's safe to store replica-specific metadata inside of it.
    ///
    /// It's important that each operator (except the start of a chain) calls `.setup()` recursively
    /// on the previous operators.
    fn setup(&mut self, metadata: &mut ExecutionMetadata);

    /// Take a value from the previous operator, process it and return it.
    fn next(&mut self) -> StreamElement<Self::Out>;

    /// A more refined representation of the operator and its predecessors.
    fn structure(&self) -> BlockStructure;
}

dyn_clone::clone_trait_object!(<O> DynOperator<Out=O>);

impl<Op> DynOperator for Op
where
    Op: Operator,
    <Op as Operator>::Out: Clone + Send + 'static,
{
    type Out = Op::Out;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.setup(metadata)
    }

    fn next(&mut self) -> StreamElement<Op::Out> {
        self.next()
    }

    fn structure(&self) -> BlockStructure {
        self.structure()
    }
}

pub struct BoxedOperator<O> {
    pub(crate) op: Box<dyn DynOperator<Out = O> + 'static + Send>,
}

impl<T> Clone for BoxedOperator<T> {
    fn clone(&self) -> Self {
        Self {
            op: self.op.clone(),
        }
    }
}

impl<T> Display for BoxedOperator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BoxedOperator")
    }
}

impl<O: Data> BoxedOperator<O> {
    pub fn new<Op: Operator<Out = O> + 'static>(op: Op) -> Self {
        Self { op: Box::new(op) }
    }
}

impl<O: Data> Operator for BoxedOperator<O> {
    type Out = O;

    fn next(&mut self) -> StreamElement<O> {
        self.op.next()
    }

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.op.setup(metadata)
    }

    fn structure(&self) -> BlockStructure {
        self.op.structure()
    }
}

impl<Op> Stream<Op>
where
    Op: Operator + 'static,
    Op::Out: Clone + Send + 'static,
{
    /// Erase operator type using dynamic dispatching.
    ///
    /// Use only when strictly necessary as it is decrimental for performance.
    pub fn into_boxed(self) -> Stream<BoxedOperator<Op::Out>> {
        self.add_operator(|prev| BoxedOperator::new(prev))
    }
}
