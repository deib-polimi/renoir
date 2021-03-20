use crate::block::BatchMode;
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};

impl<In: Data, Out: Data, OperatorChain> Stream<In, Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn batch_mode(mut self, batch_mode: BatchMode) -> Self {
        self.block.batch_mode = batch_mode;
        self
    }
}

impl<In: Data, Key: DataKey, Out: Data, OperatorChain> KeyedStream<In, Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
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
    use crate::operator::source;

    #[test]
    fn batch_mode_fixed() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(0..10u8);
        let batch_mode = BatchMode::fixed(42);
        let stream = env.stream(source).batch_mode(batch_mode);
        assert_eq!(stream.block.batch_mode, batch_mode);
    }

    #[test]
    fn batch_mode_adaptive() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(0..10u8);
        let batch_mode = BatchMode::adaptive(42, Duration::from_secs(42));
        let stream = env.stream(source).batch_mode(batch_mode);
        assert_eq!(stream.block.batch_mode, batch_mode);
    }
}
