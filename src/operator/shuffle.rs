use crate::block::NextStrategy;
use crate::operator::{EndBlock, ExchangeData, Operator};
use crate::stream::Stream;

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn shuffle(self) -> Stream<Out, impl Operator<Out>> {
        self.add_block(EndBlock::new, NextStrategy::random())
    }
}
