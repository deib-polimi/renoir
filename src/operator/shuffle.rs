use crate::block::NextStrategy;
use crate::operator::source::StartBlock;
use crate::operator::Operator;
use crate::stream::Stream;

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn shuffle(mut self) -> Stream<Out, Out, StartBlock<Out>>
    where
        In: Send + 'static,
        Out: Send + 'static,
    {
        self.block.next_strategy = NextStrategy::Random;
        self.add_block()
    }
}
