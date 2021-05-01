use crate::block::BatchMode;
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn batch_mode(mut self, batch_mode: BatchMode) -> Self {
        self.block.batch_mode = batch_mode;
        self
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    pub fn batch_mode(mut self, batch_mode: BatchMode) -> Self {
        self.0.block.batch_mode = batch_mode;
        self
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::block::BatchMode;
    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::test::FakeOperator;

    #[test]
    fn batch_mode_fixed() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = FakeOperator::<u8>::empty();
        let batch_mode = BatchMode::fixed(42);
        let stream = env.stream(source).batch_mode(batch_mode);
        assert_eq!(stream.block.batch_mode, batch_mode);
    }

    #[test]
    fn batch_mode_adaptive() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = FakeOperator::<u8>::empty();
        let batch_mode = BatchMode::adaptive(42, Duration::from_secs(42));
        let stream = env.stream(source).batch_mode(batch_mode);
        assert_eq!(stream.block.batch_mode, batch_mode);
    }

    #[test]
    fn batch_inherit_from_previous() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = FakeOperator::<u8>::empty();
        let batch_mode = BatchMode::adaptive(42, Duration::from_secs(42));
        let stream = env.stream(source).batch_mode(batch_mode).group_by(|_| 0);
        assert_eq!(stream.0.block.batch_mode, batch_mode);
    }
}
