use crate::block::NextStrategy;
use crate::operator::{EndBlock, ExchangeData, Operator};
use crate::stream::Stream;

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn max_parallelism(self, max_parallelism: usize) -> Stream<Out, impl Operator<Out>> {
        assert!(max_parallelism > 0, "Cannot set the parallelism to zero");

        let mut new_stream = self.add_block(EndBlock::new, NextStrategy::only_one());
        new_stream
            .block
            .scheduler_requirements
            .max_parallelism(max_parallelism);
        new_stream
    }
}

#[cfg(test)]
mod tests {
    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::test::FakeOperator;

    #[test]
    fn test_max_parallelims() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let operator = FakeOperator::<u8>::empty();
        let stream = env.stream(operator);
        let old_block_id = stream.block.id;
        let new_stream = stream.max_parallelism(42);
        let new_block_id = new_stream.block.id;
        assert_eq!(
            new_stream.block.scheduler_requirements.max_parallelism,
            Some(42)
        );
        assert_ne!(old_block_id, new_block_id);
    }
}
