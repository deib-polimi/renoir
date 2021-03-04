use async_std::sync::Arc;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::block::NextStrategy;
use crate::operator::{EndBlock, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;
use crate::stream::Stream;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Fold<Out, NewOut, PreviousOperators>
where
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    PreviousOperators: Operator<Out>,
    NewOut: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    prev: PreviousOperators,
    #[derivative(Debug = "ignore")]
    fold: Arc<dyn Fn(NewOut, Out) -> NewOut + Send + Sync>,
    init: NewOut,
    accumulator: Option<NewOut>,
    timestamp: Option<Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
}

#[async_trait]
impl<Out, NewOut, PreviousOperators> Operator<NewOut> for Fold<Out, NewOut, PreviousOperators>
where
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    NewOut: Clone + Serialize + DeserializeOwned + Send + 'static,
    PreviousOperators: Operator<Out> + Send,
{
    async fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata).await;
    }

    async fn next(&mut self) -> StreamElement<NewOut> {
        while !self.received_end {
            match self.prev.next().await {
                StreamElement::End => self.received_end = true,
                StreamElement::Watermark(ts) => {
                    self.max_watermark = Some(self.max_watermark.unwrap_or(ts).max(ts))
                }
                StreamElement::Item(item) => {
                    self.accumulator = Some((self.fold)(
                        self.accumulator.take().unwrap_or_else(|| self.init.clone()),
                        item,
                    ));
                }
                StreamElement::Timestamped(item, ts) => {
                    self.timestamp = Some(self.timestamp.unwrap_or(ts).max(ts));
                    self.accumulator = Some((self.fold)(
                        self.accumulator.take().unwrap_or_else(|| self.init.clone()),
                        item,
                    ));
                }
            }
        }

        // If there is an accumulated value, return it
        if let Some(acc) = self.accumulator.take() {
            if let Some(ts) = self.timestamp.take() {
                return StreamElement::Timestamped(acc, ts);
            } else {
                return StreamElement::Item(acc);
            }
        }

        // If watermark were received, send one downstream
        if let Some(ts) = self.max_watermark.take() {
            return StreamElement::Watermark(ts);
        }

        StreamElement::End
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> Fold<{} -> {}>",
            self.prev.to_string(),
            std::any::type_name::<Out>(),
            std::any::type_name::<NewOut>()
        )
    }
}

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    In: Clone + Serialize + DeserializeOwned + Send + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn fold<NewOut, Local, Global>(
        mut self,
        init: NewOut,
        local: Local,
        global: Global,
    ) -> Stream<NewOut, NewOut, impl Operator<NewOut>>
    where
        NewOut: Clone + Serialize + DeserializeOwned + Send + 'static,
        Local: Fn(NewOut, Out) -> NewOut + Send + Sync + 'static,
        Global: Fn(NewOut, NewOut) -> NewOut + Send + Sync + 'static,
    {
        // Local fold
        self.block.next_strategy = NextStrategy::OnlyOne;
        let mut second_part = self
            .add_operator(|prev| Fold {
                prev,
                fold: Arc::new(local),
                init: init.clone(),
                accumulator: None,
                timestamp: None,
                max_watermark: None,
                received_end: false,
            })
            .add_block(EndBlock::new);

        // Global fold (which is done on only one node)
        second_part.block.scheduler_requirements.max_parallelism(1);
        second_part.add_operator(|prev| Fold {
            prev,
            fold: Arc::new(global),
            init,
            accumulator: None,
            timestamp: None,
            max_watermark: None,
            received_end: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use async_std::stream::from_iter;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[async_std::test]
    async fn fold_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(from_iter(0..10u8));
        let res = env
            .stream(source)
            .fold("".to_string(), |s, n| s + &n.to_string(), |s1, s2| s1 + &s2)
            .collect_vec();
        env.execute().await;
        let res = res.get().unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], "0123456789");
    }
}
