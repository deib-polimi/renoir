use async_trait::async_trait;

use crate::operator::sink::{Sink, StreamOutput, StreamOutputRef};
use crate::operator::{Operator, StreamElement};
use crate::stream::Stream;

pub struct CollectVecSink<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    result: Option<Vec<Out>>,
    output: StreamOutputRef<Vec<Out>>,
}

#[async_trait]
impl<Out, PreviousOperators> Operator<()> for CollectVecSink<Out, PreviousOperators>
where
    Out: Send + Sync,
    PreviousOperators: Operator<Out> + Send,
{
    async fn next(&mut self) -> StreamElement<()> {
        match self.prev.next().await {
            StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                self.result.as_mut().unwrap().push(t);
                StreamElement::Item(())
            }
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::End => {
                *self.output.lock().await = Some(self.result.take().expect("Double End"));
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
    Out: Send + Sync,
    PreviousOperators: Operator<Out> + Send,
{
}

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    In: Send + 'static,
    Out: Send + Sync + 'static,
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
