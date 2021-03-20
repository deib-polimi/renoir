use std::collections::HashMap;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::block::{BatchMode, Batcher, NextStrategy, SenderList};
use crate::network::Coord;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

#[derive(Derivative)]
#[derivative(Clone, Debug)]
pub struct EndBlock<Out, OperatorChain>
where
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    metadata: Option<ExecutionMetadata>,
    next_strategy: NextStrategy<Out>,
    batch_mode: BatchMode,
    sender_groups: Vec<SenderList>,
    #[derivative(Debug = "ignore", Clone(clone_with = "clone_default"))]
    senders: HashMap<Coord, Batcher<Out>>,
}

impl<Out, OperatorChain> EndBlock<Out, OperatorChain>
where
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out>,
{
    pub(crate) fn new(
        prev: OperatorChain,
        next_strategy: NextStrategy<Out>,
        batch_mode: BatchMode,
    ) -> Self {
        Self {
            prev,
            metadata: None,
            next_strategy,
            batch_mode,
            sender_groups: Default::default(),
            senders: Default::default(),
        }
    }
}

impl<Out, OperatorChain> Operator<()> for EndBlock<Out, OperatorChain>
where
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out> + Send,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata.clone());

        let senders = metadata.network.lock().unwrap().get_senders(metadata.coord);
        self.sender_groups = self.next_strategy.group_senders(&metadata, &senders);
        self.senders = senders
            .into_iter()
            .map(|(coord, sender)| (coord, Batcher::new(sender, self.batch_mode)))
            .collect();
        self.metadata = Some(metadata);
    }

    fn next(&mut self) -> StreamElement<()> {
        let message = self.prev.next();
        let to_return = message.take();
        match &message {
            StreamElement::Watermark(_) | StreamElement::End => {
                for senders in self.sender_groups.iter() {
                    for &sender in senders.0.iter() {
                        self.senders
                            .get_mut(&sender)
                            .unwrap()
                            .enqueue(message.clone());
                    }
                }
            }
            StreamElement::Item(item) | StreamElement::Timestamped(item, _) => {
                let index = self.next_strategy.index(&item);
                for sender in self.sender_groups.iter() {
                    let index = index % sender.0.len();
                    self.senders
                        .get_mut(&sender.0[index])
                        .unwrap()
                        .enqueue(message.clone());
                }
            }
        };

        if matches!(to_return, StreamElement::End) {
            let metadata = self.metadata.as_ref().unwrap();
            debug!("EndBlock at {} received End", metadata.coord);
            for (_, batcher) in self.senders.drain() {
                batcher.end();
            }
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

fn clone_default<T>(_: &T) -> T
where
    T: Default,
{
    T::default()
}
