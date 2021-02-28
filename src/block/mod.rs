use std::marker::PhantomData;

use async_std::sync::Arc;
use once_cell::sync::OnceCell;

use crate::operator::Operator;
use crate::scheduler::ExecutionMetadata;
use crate::stream::BlockId;

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
    pub id: BlockId,
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
    pub fn new(id: BlockId, operators: OperatorChain, metadata: ExecutionMetadataRef) -> Self {
        Self {
            id,
            operators,
            next_strategy: NextStrategy::OnlyOne,
            execution_metadata: metadata,
            _in_type: Default::default(),
            _out_type: Default::default(),
        }
    }

    pub fn to_string(&self) -> String {
        match self.next_strategy {
            NextStrategy::Random => format!("Shuffle<{}>", self.operators.to_string()),
            _ => self.operators.to_string().to_string(),
        }
    }
}
