use crate::block::NextStrategy;
use crate::operator::{Data, EndBlock, Operator};
use crate::stream::Stream;

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn split(self, splits: usize) -> Vec<Stream<Out, impl Operator<Out>>> {
        // This is needed to maintain the same parallelism of the split block
        let scheduler_requirements = self.block.scheduler_requirements.clone();
        let mut new_stream = self.add_block(EndBlock::new, NextStrategy::OnlyOne);
        new_stream.block.scheduler_requirements = scheduler_requirements;

        let mut streams = Vec::with_capacity(splits);
        for _ in 0..splits - 1 {
            streams.push(new_stream.clone());
        }
        streams.push(new_stream);

        streams
    }
}
