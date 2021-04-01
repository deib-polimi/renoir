use crate::block::NextStrategy;
use crate::operator::sink::{Sink, StreamOutput, StreamOutputRef};
use crate::operator::{Data, EndBlock, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::Stream;

#[derive(Debug)]
pub struct CollectVecSink<Out: Data, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    result: Option<Vec<Out>>,
    output: StreamOutputRef<Vec<Out>>,
}

impl<Out: Data, PreviousOperators> Operator<()> for CollectVecSink<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out> + Send,
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
            StreamElement::End => {
                if let Some(result) = self.result.take() {
                    *self.output.lock().unwrap() = Some(result);
                }
                StreamElement::End
            }
            StreamElement::FlushBatch => StreamElement::FlushBatch,
        }
    }

    fn to_string(&self) -> String {
        format!("{} -> CollectVecSink", self.prev.to_string())
    }
}

impl<Out: Data, PreviousOperators> Sink for CollectVecSink<Out, PreviousOperators> where
    PreviousOperators: Operator<Out> + Send
{
}

impl<Out: Data, PreviousOperators> Clone for CollectVecSink<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out> + Send,
{
    fn clone(&self) -> Self {
        panic!("CollectVecSink cannot be cloned, max_parallelism should be 1");
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn collect_vec(self) -> StreamOutput<Vec<Out>> {
        let output = StreamOutputRef::default();
        let mut new_stream = self.add_block(EndBlock::new, NextStrategy::OnlyOne);
        // FIXME: when implementing Stream::max_parallelism use that here
        new_stream.block.scheduler_requirements.max_parallelism(1);
        new_stream
            .add_operator(|prev| CollectVecSink {
                prev,
                result: Some(Vec::new()),
                output: output.clone(),
            })
            .finalize_block();
        StreamOutput { result: output }
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
