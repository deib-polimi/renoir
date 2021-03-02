use crate::block::NextStrategy;
use crate::operator::Operator;
use crate::operator::StartBlock;
use crate::stream::Stream;

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    In: Clone + Send + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn shuffle(mut self) -> Stream<Out, Out, StartBlock<Out>> {
        self.block.next_strategy = NextStrategy::Random;
        self.add_block()
    }
}
