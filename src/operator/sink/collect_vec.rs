use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::operator::sink::{Sink, StreamOutput, StreamOutputRef};
use crate::operator::{EndBlock, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::Stream;

#[derive(Debug)]
pub struct CollectVecSink<Out, PreviousOperators>
where
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    result: Option<Vec<Out>>,
    output: StreamOutputRef<Vec<Out>>,
}

#[async_trait]
impl<Out, PreviousOperators> Operator<()> for CollectVecSink<Out, PreviousOperators>
where
    Out: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    PreviousOperators: Operator<Out> + Send,
{
    async fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata).await;
    }

    async fn next(&mut self) -> StreamElement<()> {
        match self.prev.next().await {
            StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                // cloned CollectVecSink or already ended stream
                if let Some(result) = self.result.as_mut() {
                    result.push(t);
                }
                debug!("Received element at collect_vec");
                StreamElement::Item(())
            }
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::End => {
                if let Some(result) = self.result.take() {
                    *self.output.lock().await = Some(result);
                }
                StreamElement::End
            }
        }
    }

    fn to_string(&self) -> String {
        format!("{} -> CollectVecSink", self.prev.to_string())
    }
}

impl<Out, PreviousOperators> Sink for CollectVecSink<Out, PreviousOperators>
where
    Out: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    PreviousOperators: Operator<Out> + Send,
{
}

impl<Out, PreviousOperators> Clone for CollectVecSink<Out, PreviousOperators>
where
    Out: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    PreviousOperators: Operator<Out> + Send,
{
    fn clone(&self) -> Self {
        panic!("CollectVecSink cannot be cloned, max_parallelism should be 1");
    }
}

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    In: Clone + Serialize + DeserializeOwned + Send + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn collect_vec(self) -> StreamOutput<Vec<Out>> {
        let output = StreamOutputRef::default();
        let mut new_stream = self.add_block(EndBlock::new);
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
