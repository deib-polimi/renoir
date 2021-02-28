use crate::operator::{Operator, StreamElement};
use async_trait::async_trait;

pub struct CollectVecSink<In, PreviousOperators>
where
    PreviousOperators: Operator<In>,
{
    prev: PreviousOperators,
    result: Vec<In>,
}

#[async_trait]
impl<In, PreviousOperators> Operator<()> for CollectVecSink<In, PreviousOperators>
where
    In: Send,
    PreviousOperators: Operator<In> + Send,
{
    async fn next(&mut self) -> StreamElement<()> {
        match self.prev.next().await {
            StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                self.result.push(t);
                StreamElement::Item(())
            }
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::End => StreamElement::End,
        }
    }
}
