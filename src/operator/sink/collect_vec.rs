use async_trait::async_trait;

use crate::block::ExecutionMetadataRef;
use crate::operator::sink::{Sink, StreamOutput, StreamOutputRef};
use crate::operator::{Operator, StreamElement};
use crate::stream::Stream;

pub struct CollectVecSink<Out, PreviousOperators>
where
    Out: Clone + Send + 'static,
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    result: Option<Vec<Out>>,
    output: StreamOutputRef<Vec<Out>>,
}

#[async_trait]
impl<Out, PreviousOperators> Operator<()> for CollectVecSink<Out, PreviousOperators>
where
    Out: Clone + Send + Sync + 'static,
    PreviousOperators: Operator<Out> + Send,
{
    fn block_init(&mut self, metadata: ExecutionMetadataRef) {
        self.prev.block_init(metadata);
    }

    async fn start(&mut self) {
        self.prev.start().await;
    }

    async fn next(&mut self) -> StreamElement<()> {
        match self.prev.next().await {
            StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                // cloned CollectVecSink or already ended stream
                if let Some(result) = self.result.as_mut() {
                    result.push(t);
                }
                info!("Received element at collect_vec");
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
        format!("CollectVecSink<{}>", self.prev.to_string())
    }
}

impl<Out, PreviousOperators> Sink for CollectVecSink<Out, PreviousOperators>
where
    Out: Clone + Send + Sync + 'static,
    PreviousOperators: Operator<Out> + Send,
{
}
impl<Out, PreviousOperators> Clone for CollectVecSink<Out, PreviousOperators>
where
    Out: Clone + Send + Sync + 'static,
    PreviousOperators: Operator<Out> + Send,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            result: None, // disable the new sink
            output: self.output.clone(),
        }
    }
}

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    In: Clone + Send + 'static,
    Out: Clone + Send + Sync + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn collect_vec(self) -> StreamOutput<Vec<Out>> {
        let output = StreamOutputRef::default();
        self.add_operator(|prev| CollectVecSink {
            prev,
            result: Some(Vec::new()),
            output: output.clone(),
        })
        .finalize_block();
        StreamOutput { result: output }
    }
}
