use crate::block::NextStrategy;
use crate::operator::end::EndBlock;
use crate::operator::{ExchangeData, Operator};
use crate::stream::Stream;

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Duplicate each element of the stream and forward it to all the replicas of the next block.
    ///
    /// **Note**: this will duplicate the elements of the stream, this is potentially a very
    /// expensive operation.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// s.broadcast();
    /// ```
    pub fn broadcast(self) -> Stream<Out, impl Operator<Out>> {
        self.add_block(EndBlock::new, NextStrategy::all())
    }
}
