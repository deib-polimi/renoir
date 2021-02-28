use std::marker::PhantomData;

use async_trait::async_trait;

use crate::operator::{Operator, StreamElement};

pub enum NextStrategy {
    OnlyOne,
    Random,
    GroupBy,
}

pub struct InnerBlock<In, Out, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    pub operators: OperatorChain,
    pub next_strategy: NextStrategy,
    pub _in_type: PhantomData<In>,
    pub _out_type: PhantomData<Out>,
}

impl<In, Out, OperatorChain> InnerBlock<In, Out, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    pub fn new(operators: OperatorChain) -> Self {
        Self {
            operators,
            next_strategy: NextStrategy::OnlyOne,
            _in_type: Default::default(),
            _out_type: Default::default(),
        }
    }
}
