use std::collections::HashMap;

use crate::block::{
    BatchMode, Batcher, BlockStructure, Connection, NextStrategy, OperatorStructure, SenderList,
};
use crate::network::ReceiverEndpoint;
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

#[derive(Derivative)]
#[derivative(Clone, Debug)]
pub struct EndBlock<Out: Data, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    metadata: Option<ExecutionMetadata>,
    next_strategy: NextStrategy<Out>,
    batch_mode: BatchMode,
    sender_groups: Vec<SenderList>,
    #[derivative(Debug = "ignore", Clone(clone_with = "clone_default"))]
    senders: HashMap<ReceiverEndpoint, Batcher<Out>>,
}

impl<Out: Data, OperatorChain> EndBlock<Out, OperatorChain>
where
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

impl<Out: Data, OperatorChain> Operator<()> for EndBlock<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata.clone());

        let senders = metadata.network.lock().unwrap().get_senders(metadata.coord);
        // Group the senders based on the strategy. Ignore all the senders to this very block.
        // This is needed by the iterations and maybe more complex operators: self-loops inside the
        // job graph are used for sending messages between replicas of the same block. In this case
        // the EndBlock should ignore those destinations and those channels must be manually managed
        // by the operators that use them.
        self.sender_groups = self
            .next_strategy
            .group_senders(&senders, Some(metadata.coord.block_id));
        self.senders = senders
            .into_iter()
            .map(|(coord, sender)| (coord, Batcher::new(sender, self.batch_mode, metadata.coord)))
            .collect();
        self.metadata = Some(metadata);
    }

    fn next(&mut self) -> StreamElement<()> {
        let message = self.prev.next();
        let to_return = message.take();
        match &message {
            StreamElement::Watermark(_) | StreamElement::End | StreamElement::IterEnd => {
                for senders in self.sender_groups.iter() {
                    for &sender in senders.0.iter() {
                        let sender = self.senders.get_mut(&sender).unwrap();
                        sender.enqueue(message.clone());
                        // make sure to flush at the end of each iteration
                        if matches!(message, StreamElement::IterEnd) {
                            sender.flush();
                        }
                    }
                }
            }
            StreamElement::Item(item) | StreamElement::Timestamped(item, _) => {
                let index = self.next_strategy.index(&item);
                for sender in self.sender_groups.iter().skip(1) {
                    let index = index % sender.0.len();
                    self.senders
                        .get_mut(&sender.0[index])
                        .unwrap()
                        .enqueue(message.clone());
                }
                // avoid the last message.clone()
                if !self.sender_groups.is_empty() {
                    let sender = &self.sender_groups[0];
                    let index = index % sender.0.len();
                    self.senders
                        .get_mut(&sender.0[index])
                        .unwrap()
                        .enqueue(message);
                }
            }
            StreamElement::FlushBatch => {
                for (_, batcher) in self.senders.iter_mut() {
                    batcher.flush();
                }
            }
        };

        if matches!(to_return, StreamElement::End) {
            let metadata = self.metadata.as_ref().unwrap();
            debug!(
                "EndBlock at {} received End, closing {} channels",
                metadata.coord,
                self.senders.len()
            );
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

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("EndBlock");
        for sender_group in &self.sender_groups {
            if !sender_group.0.is_empty() {
                let block_id = sender_group.0[0].coord.block_id;
                operator
                    .connections
                    .push(Connection::new::<Out>(block_id, &self.next_strategy));
            }
        }
        self.prev.structure().add_operator(operator)
    }
}

fn clone_default<T>(_: &T) -> T
where
    T: Default,
{
    T::default()
}
