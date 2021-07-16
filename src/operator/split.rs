use crate::block::NextStrategy;
use crate::operator::end::EndBlock;
use crate::operator::{ExchangeData, Operator};
use crate::stream::Stream;

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Split the stream into `splits` streams, each with all the elements of the first one.
    ///
    /// This will effectively duplicate every item in the stream into the newly created streams.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let mut splits = s.split(3);
    /// let a = splits.pop().unwrap();
    /// let b = splits.pop().unwrap();
    /// let c = splits.pop().unwrap();
    /// ```
    pub fn split(self, splits: usize) -> Vec<Stream<Out, impl Operator<Out>>> {
        // This is needed to maintain the same parallelism of the split block
        let scheduler_requirements = self.block.scheduler_requirements.clone();
        let mut new_stream = self.add_block(EndBlock::new, NextStrategy::only_one());
        new_stream.block.scheduler_requirements = scheduler_requirements;

        let mut streams = Vec::with_capacity(splits);
        for _ in 0..splits - 1 {
            streams.push(new_stream.clone());
        }
        streams.push(new_stream);

        streams
    }
}
