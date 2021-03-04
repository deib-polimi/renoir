use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::block::NextStrategy;
use crate::operator::StartBlock;
use crate::operator::{EndBlock, Operator};
use crate::stream::Stream;

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    In: Clone + Serialize + DeserializeOwned + Send + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn shuffle(mut self) -> Stream<Out, Out, impl Operator<Out>> {
        self.block.next_strategy = NextStrategy::Random;
        self.add_block(EndBlock::new)
    }
}
