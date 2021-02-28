use std::cell::RefCell;
use std::marker::PhantomData;

use async_std::sync::Arc;
use once_cell::sync::OnceCell;

use crate::operator::Operator;
use crate::worker::ExecutionMetadata;

pub enum NextStrategy {
    OnlyOne,
    Random,
    GroupBy,
}

pub type ExecutionMetadataRef = Arc<OnceCell<ExecutionMetadata>>;

pub struct InnerBlock<In, Out, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    pub operators: OperatorChain,
    pub next_strategy: NextStrategy,
    pub execution_metadata: ExecutionMetadataRef,
    pub _in_type: PhantomData<In>,
    pub _out_type: PhantomData<Out>,
}

impl<In, Out, OperatorChain> InnerBlock<In, Out, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    pub fn new(operators: OperatorChain, metadata: ExecutionMetadataRef) -> Self {
        Self {
            operators,
            next_strategy: NextStrategy::OnlyOne,
            execution_metadata: metadata,
            _in_type: Default::default(),
            _out_type: Default::default(),
        }
    }
}
