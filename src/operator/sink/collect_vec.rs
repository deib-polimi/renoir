use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::operator::sink::{Sink, StreamOutput, StreamOutputRef};
use crate::operator::{ExchangeData, ExchangeDataKey, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

#[derive(Debug)]
pub struct CollectVecSink<Out: ExchangeData, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    result: Option<Vec<Out>>,
    output: StreamOutputRef<Vec<Out>>,
}

impl<Out: ExchangeData, PreviousOperators> Operator<()> for CollectVecSink<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<()> {
        match self.prev.next() {
            StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                // cloned CollectVecSink or already ended stream
                if let Some(result) = self.result.as_mut() {
                    result.push(t);
                }
                StreamElement::Item(())
            }
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::Terminate => {
                if let Some(result) = self.result.take() {
                    *self.output.lock().unwrap() = Some(result);
                }
                StreamElement::Terminate
            }
            StreamElement::FlushBatch => StreamElement::FlushBatch,
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
        }
    }

    fn to_string(&self) -> String {
        format!("{} -> CollectVecSink", self.prev.to_string())
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("CollectVecSink");
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }
}

impl<Out: ExchangeData, PreviousOperators> Sink for CollectVecSink<Out, PreviousOperators> where
    PreviousOperators: Operator<Out>
{
}

impl<Out: ExchangeData, PreviousOperators> Clone for CollectVecSink<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn clone(&self) -> Self {
        panic!("CollectVecSink cannot be cloned, max_parallelism should be 1");
    }
}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn collect_vec(self) -> StreamOutput<Vec<Out>> {
        let output = StreamOutputRef::default();
        self.max_parallelism(1)
            .add_operator(|prev| CollectVecSink {
                prev,
                result: Some(Vec::new()),
                output: output.clone(),
            })
            .finalize_block();
        StreamOutput { result: output }
    }
}

impl<Key: ExchangeDataKey, Out: ExchangeData, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    pub fn collect_vec(self) -> StreamOutput<Vec<(Key, Out)>> {
        self.unkey().collect_vec()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[test]
    fn collect_vec() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let res = env.stream(source).collect_vec();
        env.execute();
        assert_eq!(res.get().unwrap(), (0..10).collect_vec());
    }
}
