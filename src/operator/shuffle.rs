use crate::block::NextStrategy;
use crate::operator::end::EndBlock;
use crate::operator::{ExchangeData, Operator};
use crate::stream::Stream;

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Perform a network shuffle sending the messages to a random replica.
    ///
    /// This can be useful if for some reason the load is very unbalanced (e.g. after a very
    /// unbalanced [`Stream::group_by`]).
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.shuffle();
    /// ```
    pub fn shuffle(self) -> Stream<Out, impl Operator<Out>> {
        self.add_block(EndBlock::new, NextStrategy::random())
    }
}
