use async_std::sync::Arc;
use async_trait::async_trait;

use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::Stream;

pub struct Map<Out, NewOut, PreviousOperators>
where
    Out: Clone + Send + 'static,
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    f: Arc<dyn Fn(Out) -> NewOut + Send + Sync>,
}

#[async_trait]
impl<Out, NewOut, PreviousOperators> Operator<NewOut> for Map<Out, NewOut, PreviousOperators>
where
    Out: Clone + Send + 'static,
    NewOut: Clone + Send + 'static,
    PreviousOperators: Operator<Out> + Send,
{
    async fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata).await;
    }

    async fn next(&mut self) -> StreamElement<NewOut> {
        match self.prev.next().await {
            StreamElement::Item(t) => StreamElement::Item((self.f)(t)),
            StreamElement::Timestamped(t, ts) => StreamElement::Timestamped((self.f)(t), ts),
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::End => StreamElement::End,
        }
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> Map<{} -> {}>",
            self.prev.to_string(),
            std::any::type_name::<Out>(),
            std::any::type_name::<NewOut>()
        )
    }
}

impl<Out, NewOut, PreviousOperators> Clone for Map<Out, NewOut, PreviousOperators>
where
    Out: Clone + Send + 'static,
    NewOut: Clone + Send + 'static,
    PreviousOperators: Operator<Out> + Send,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            f: self.f.clone(),
        }
    }
}

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    In: Clone + Send + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn map<NewOut, F>(self, f: F) -> Stream<In, NewOut, Map<Out, NewOut, OperatorChain>>
    where
        NewOut: Clone + Send + 'static,
        F: Fn(Out) -> NewOut + Send + Sync + 'static,
    {
        self.add_operator(|prev| Map {
            prev,
            f: Arc::new(f),
        })
    }
}
