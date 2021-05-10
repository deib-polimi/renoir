use crate::block::NextStrategy;
use crate::operator::{Data, EndBlock, Operator};
use crate::stream::Stream;

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn broadcast(self) -> Stream<Out, impl Operator<Out>> {
        self.add_block(EndBlock::new, NextStrategy::All)
    }
}
