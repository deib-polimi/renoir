use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::block::{NextStrategy, SenderList};
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

#[derive(Debug, Clone)]
pub struct EndBlock<Out, OperatorChain>
where
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    metadata: Option<ExecutionMetadata>,
    next_strategy: NextStrategy<Out>,
    senders: Vec<SenderList<Out>>,
}

impl<Out, OperatorChain> EndBlock<Out, OperatorChain>
where
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out>,
{
    pub(crate) fn new(prev: OperatorChain, next_strategy: NextStrategy<Out>) -> Self {
        Self {
            prev,
            metadata: None,
            next_strategy,
            senders: Default::default(),
        }
    }
}

#[async_trait]
impl<Out, OperatorChain> Operator<()> for EndBlock<Out, OperatorChain>
where
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out> + Send,
{
    async fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata.clone()).await;

        let senders = metadata.network.lock().await.get_senders(metadata.coord);
        self.senders = self.next_strategy.group_senders(&metadata, senders);
        self.metadata = Some(metadata);
    }

    async fn next(&mut self) -> StreamElement<()> {
        let message = self.prev.next().await;
        let to_return = message.take();
        let mut to_send = Vec::new();
        match &message {
            StreamElement::Watermark(_) | StreamElement::End => {
                for senders in self.senders.iter() {
                    for sender in senders.0.iter() {
                        to_send.push((message.clone(), sender))
                    }
                }
            }
            StreamElement::Item(item) | StreamElement::Timestamped(item, _) => {
                let index = self.next_strategy.index(&item);
                for sender in self.senders.iter() {
                    let index = index % sender.0.len();
                    to_send.push((message.clone(), &sender.0[index]));
                }
            }
        };
        for (message, sender) in to_send {
            sender.send(vec![message]).await.unwrap();
        }

        if matches!(to_return, StreamElement::End) {
            let metadata = self.metadata.as_ref().unwrap();
            info!("EndBlock at {} received End", metadata.coord);
        }
        to_return
    }

    fn to_string(&self) -> String {
        match self.next_strategy {
            NextStrategy::Random => format!("{} -> Shuffle", self.prev.to_string()),
            NextStrategy::OnlyOne => format!("{} -> OnlyOne", self.prev.to_string()),
            _ => self.prev.to_string(),
        }
    }
}
