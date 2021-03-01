use std::marker::PhantomData;

use crate::operator::Operator;
use crate::stream::BlockId;

#[derive(Debug, Clone, Copy)]
pub enum NextStrategy {
    OnlyOne,
    Random,
    GroupBy,
}

pub struct InnerBlock<In, Out, OperatorChain>
where
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out>,
{
    pub id: BlockId,
    pub operators: OperatorChain,
    pub next_strategy: NextStrategy,
    pub max_parallelism: Option<usize>,
    pub _in_type: PhantomData<In>,
    pub _out_type: PhantomData<Out>,
}

impl<In, Out, OperatorChain> InnerBlock<In, Out, OperatorChain>
where
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out>,
{
    pub fn new(id: BlockId, operators: OperatorChain) -> Self {
        Self {
            id,
            operators,
            next_strategy: NextStrategy::OnlyOne,
            max_parallelism: None,
            _in_type: Default::default(),
            _out_type: Default::default(),
        }
    }

    pub fn to_string(&self) -> String {
        self.operators.to_string()
    }
}

impl<In, Out, OperatorChain> Clone for InnerBlock<In, Out, OperatorChain>
where
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out>,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            operators: self.operators.clone(),
            next_strategy: self.next_strategy.clone(),
            max_parallelism: self.max_parallelism,
            _in_type: Default::default(),
            _out_type: Default::default(),
        }
    }
}
