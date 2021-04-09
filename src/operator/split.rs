use crate::block::NextStrategy;
use crate::operator::{Data, EndBlock, Operator};
use crate::stream::Stream;

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send + 'static,
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

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[test]
    fn split_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..5u8);
        let mut splits = env.stream(source).map(|n| n.to_string()).split(2);
        let v1 = splits.pop().unwrap().map(|x| x.clone() + &x).collect_vec();
        let v2 = splits.pop().unwrap().map(|x| x + "a").collect_vec();
        env.execute();

        assert_eq!(
            v1.get().unwrap().into_iter().sorted().collect_vec(),
            &["00", "11", "22", "33", "44"]
        );
        assert_eq!(
            v2.get().unwrap().into_iter().sorted().collect_vec(),
            &["0a", "1a", "2a", "3a", "4a"]
        );
    }
}
