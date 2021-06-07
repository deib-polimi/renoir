use crate::block::NextStrategy;
use crate::operator::end::EndBlock;
use crate::operator::{ExchangeData, Operator};
use crate::stream::Stream;

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn broadcast(self) -> Stream<Out, impl Operator<Out>> {
        self.add_block(EndBlock::new, NextStrategy::all())
    }
}
