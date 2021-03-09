use std::hash::Hash;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::block::BatchMode;
use crate::operator::Operator;
use crate::stream::{KeyValue, KeyedStream, Stream};

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    In: Clone + Serialize + DeserializeOwned + Send + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn batch_mode(mut self, batch_mode: BatchMode) -> Self {
        self.block.batch_mode = batch_mode;
        self
    }
}

impl<In, Key, Out, OperatorChain> KeyedStream<In, Key, Out, OperatorChain>
where
    Key: Clone + Serialize + DeserializeOwned + Send + Hash + Eq + 'static,
    In: Clone + Serialize + DeserializeOwned + Send + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
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

    use std::stream::from_iter;

    use crate::block::BatchMode;
    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[std::test]
    fn batch_mode_fixed() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(from_iter(0..10u8));
        let batch_mode = BatchMode::fixed(42);
        let stream = env.stream(source).batch_mode(batch_mode);
        assert_eq!(stream.block.batch_mode, batch_mode);
    }

    #[std::test]
    fn batch_mode_adaptive() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(from_iter(0..10u8));
        let batch_mode = BatchMode::adaptive(42, Duration::from_secs(42));
        let stream = env.stream(source).batch_mode(batch_mode);
        assert_eq!(stream.block.batch_mode, batch_mode);
    }
}
