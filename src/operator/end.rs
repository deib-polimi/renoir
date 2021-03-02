use std::collections::HashMap;

use async_trait::async_trait;
use rand::{thread_rng, Rng};

use crate::block::NextStrategy;
use crate::network::{NetworkMessage, NetworkSender};
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

pub type SenderList<Out> = Vec<Vec<NetworkSender<NetworkMessage<Out>>>>;

#[derive(Debug, Clone)]
pub struct EndBlock<Out, OperatorChain>
where
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    metadata: Option<ExecutionMetadata>,
    next_strategy: NextStrategy,
    senders: SenderList<Out>,
}

impl<Out, OperatorChain> EndBlock<Out, OperatorChain>
where
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out>,
{
    pub(crate) fn new(prev: OperatorChain, next_strategy: NextStrategy) -> Self {
        Self {
            prev,
            metadata: None,
            next_strategy,
            senders: Default::default(),
        }
    }
}

pub async fn broadcast<Out>(senders: &SenderList<Out>, message: NetworkMessage<Out>)
where
    Out: Clone + Send + 'static,
{
    for senders in senders.iter() {
        for sender in senders.iter() {
            let message = message.clone();
            if let Err(e) = sender.send(message).await {
                error!("Failed to send message to {:?}: {:?}", sender, e);
            }
        }
    }
}

async fn send<Out>(senders: &SenderList<Out>, message: StreamElement<Out>)
where
    Out: Clone + Send + 'static,
{
    for senders in senders.iter() {
        let out_buf = vec![message.clone()];
        let index = thread_rng().gen_range(0..senders.len());
        // TODO: batching
        let sender = &senders[index];
        if let Err(e) = sender.send(out_buf).await {
            error!("Failed to send message to {:?}: {:?}", sender, e);
        }
    }
}

#[async_trait]
impl<Out, OperatorChain> Operator<()> for EndBlock<Out, OperatorChain>
where
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out> + Send,
{
    async fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata.clone()).await;

        let network = metadata.network.lock().await;
        let senders = network.get_senders(metadata.coord);
        drop(network);
        let mut by_block_id: HashMap<_, Vec<_>> = HashMap::new();
        for (coord, sender) in senders {
            by_block_id.entry(coord.block_id).or_default().push(sender);
        }
        for (block_id, senders) in by_block_id {
            match self.next_strategy {
                NextStrategy::OnlyOne => {
                    assert!(
                        senders.len() == 1 || senders.len() == metadata.num_replicas,
                        "OnlyOne cannot mix the number of replicas: block={} current={}, next={}",
                        block_id,
                        senders.len(),
                        metadata.num_replicas
                    );
                    if senders.len() == 1 {
                        self.senders.push(senders);
                    } else {
                        let mut found = false;
                        for sender in senders {
                            if sender.coord.replica_id == metadata.coord.replica_id {
                                found = true;
                                self.senders.push(vec![sender]);
                                break;
                            }
                        }
                        assert!(
                            found,
                            "Cannot found next sender for the block with the same replica_id: {}",
                            metadata.coord
                        );
                    }
                }
                NextStrategy::Random => {
                    self.senders.push(senders);
                }
                NextStrategy::GroupBy => todo!("GroupBy is not supported yet"),
            }
        }
        info!(
            "EndBlock of {} has these senders: {:?}",
            metadata.coord, self.senders
        );
        self.metadata = Some(metadata);
    }

    async fn next(&mut self) -> StreamElement<()> {
        let message = self.prev.next().await;
        let to_return = message.take();
        match message {
            StreamElement::Watermark(_) | StreamElement::End => {
                broadcast(&self.senders, vec![message]).await
            }
            _ => send(&self.senders, message).await,
        };
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
            _ => self.prev.to_string().to_string(),
        }
    }
}
