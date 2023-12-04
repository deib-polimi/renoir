use std::collections::HashMap;
use std::fmt::Display;

use crate::block::{
    BatchMode, Batcher, BlockStructure, Connection, NextStrategy, OperatorStructure,
};
use crate::network::{Coord, ReceiverEndpoint};
use crate::operator::{ExchangeData, KeyerFn, Operator, StreamElement};
use crate::scheduler::{BlockId, ExecutionMetadata};

/// The list with the interesting senders of a single block.
#[derive(Debug, Clone)]
pub(crate) struct BlockSenders {
    /// Indexes of the senders for all the replicas of this box
    pub indexes: Vec<usize>,
}

impl BlockSenders {
    pub(crate) fn new(indexes: Vec<usize>) -> Self {
        Self { indexes }
    }
}

pub struct End<OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<u64, OperatorChain::Out>,
    OperatorChain: Operator,
    OperatorChain::Out: Send + 'static,
{
    prev: OperatorChain,
    coord: Option<Coord>,
    next_strategy: NextStrategy<OperatorChain::Out, IndexFn>,
    batch_mode: BatchMode,
    block_senders: Vec<BlockSenders>,
    senders: Vec<(ReceiverEndpoint, Batcher<OperatorChain::Out>)>,
    feedback_id: Option<BlockId>,
    ignore_block_ids: Vec<BlockId>,
}

impl<OperatorChain: std::fmt::Debug, IndexFn: std::fmt::Debug> std::fmt::Debug
    for End<OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<u64, OperatorChain::Out>,
    OperatorChain: Operator,
    OperatorChain::Out: Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("End")
            .field("prev", &self.prev)
            .field("coord", &self.coord)
            .field("next_strategy", &self.next_strategy)
            .field("batch_mode", &self.batch_mode)
            .field("block_senders", &self.block_senders)
            .field("feedback_id", &self.feedback_id)
            .field("ignore_block_ids", &self.ignore_block_ids)
            .finish()
    }
}

impl<OperatorChain: Clone, IndexFn: Clone> Clone for End<OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<u64, OperatorChain::Out>,
    OperatorChain: Operator,
    OperatorChain::Out: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            coord: self.coord,
            next_strategy: self.next_strategy.clone(),
            batch_mode: self.batch_mode,
            block_senders: self.block_senders.clone(),
            senders: Default::default(),
            feedback_id: self.feedback_id,
            ignore_block_ids: self.ignore_block_ids.clone(),
        }
    }
}

impl<OperatorChain, IndexFn> Display for End<OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<u64, OperatorChain::Out>,
    OperatorChain: Operator,
    OperatorChain::Out: Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.next_strategy {
            NextStrategy::Random => write!(f, "{} -> Shuffle", self.prev),
            NextStrategy::OnlyOne => write!(f, "{} -> OnlyOne", self.prev),
            _ => self.prev.fmt(f),
        }
    }
}

impl<OperatorChain, IndexFn> End<OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<u64, OperatorChain::Out>,
    OperatorChain: Operator,
    OperatorChain::Out: Send + 'static,
{
    pub(crate) fn new(
        prev: OperatorChain,
        next_strategy: NextStrategy<OperatorChain::Out, IndexFn>,
        batch_mode: BatchMode,
    ) -> Self {
        Self {
            prev,
            coord: None,
            next_strategy,
            batch_mode,
            block_senders: Default::default(),
            senders: Default::default(),
            feedback_id: None,
            ignore_block_ids: Default::default(),
        }
    }

    // group the senders based on the strategy
    fn setup_senders(&mut self) {
        glidesort::sort_by_key(&mut self.senders, |s| s.0);

        self.block_senders = match self.next_strategy {
            NextStrategy::All => (0..self.senders.len())
                .map(|i| vec![i])
                .map(BlockSenders::new)
                .collect(),
            _ => self
                .senders
                .iter()
                .enumerate()
                .fold(HashMap::<_, Vec<_>>::new(), |mut map, (i, (coord, _))| {
                    map.entry(coord.coord.block_id).or_default().push(i);
                    map
                })
                .into_values()
                .map(BlockSenders::new)
                .collect(),
        };

        if matches!(self.next_strategy, NextStrategy::OnlyOne) {
            self.block_senders
                .iter()
                .for_each(|s| assert_eq!(s.indexes.len(), 1));
        }
    }

    /// Mark this `End` as the end of a feedback loop.
    ///
    /// This will avoid this block from sending `Terminate` in the feedback loop, the destination
    /// should be already gone.
    pub(crate) fn mark_feedback(&mut self, block_id: BlockId) {
        self.feedback_id = Some(block_id);
    }

    pub(crate) fn ignore_destination(&mut self, block_id: BlockId) {
        self.ignore_block_ids.push(block_id);
    }
}

impl<OperatorChain, IndexFn> Operator for End<OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<u64, OperatorChain::Out>,
    OperatorChain: Operator,
    OperatorChain::Out: ExchangeData,
{
    type Out = ();

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        // TODO: wrap sender-block assignment logic in a struct
        let senders = metadata.network.get_senders(metadata.coord);
        // remove the ignored destinations
        self.senders = senders
            .into_iter()
            .filter(|(endpoint, _)| !self.ignore_block_ids.contains(&endpoint.coord.block_id))
            .map(|(coord, sender)| (coord, Batcher::new(sender, self.batch_mode, metadata.coord)))
            .collect();

        self.setup_senders();

        self.coord = Some(metadata.coord);
    }

    fn next(&mut self) -> StreamElement<()> {
        let message = self.prev.next();
        let to_return = message.take();
        match &message {
            // Broadcast messages
            StreamElement::Watermark(_)
            | StreamElement::Terminate
            | StreamElement::FlushAndRestart => {
                for block in self.block_senders.iter() {
                    for &sender_idx in block.indexes.iter() {
                        let sender = &mut self.senders[sender_idx];

                        // if this block is the end of the feedback loop it should not forward
                        // `Terminate` since the destination is before us in the termination chain,
                        // and therefore has already left
                        if matches!(message, StreamElement::Terminate)
                            && Some(sender.0.coord.block_id) == self.feedback_id
                        {
                            continue;
                        }
                        sender.1.enqueue(message.clone());
                    }
                }
            }
            // Direct messages
            StreamElement::Item(item) | StreamElement::Timestamped(item, _) => {
                let index = self.next_strategy.index(item);
                for block in self.block_senders.iter() {
                    let index = index % block.indexes.len();
                    let sender_idx = block.indexes[index];
                    self.senders[sender_idx].1.enqueue(message.clone());
                }
            }
            StreamElement::FlushBatch => {}
        };

        // Flushing messages
        match to_return {
            StreamElement::FlushAndRestart | StreamElement::FlushBatch => {
                for (_, batcher) in self.senders.iter_mut() {
                    batcher.flush();
                }
            }
            StreamElement::Terminate => {
                log::debug!(
                    "{} received terminate, closing {} channels",
                    self.coord.unwrap(),
                    self.senders.len()
                );
                for (_, batcher) in self.senders.drain(..) {
                    batcher.end();
                }
            }
            _ => {}
        }

        to_return
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<OperatorChain::Out, _>("End");
        for sender_group in &self.block_senders {
            if !sender_group.indexes.is_empty() {
                let block_id = self.senders[sender_group.indexes[0]].0.coord.block_id;
                operator
                    .connections
                    .push(Connection::new::<OperatorChain::Out, _>(
                        block_id,
                        &self.next_strategy,
                    ));
            }
        }
        self.prev.structure().add_operator(operator)
    }
}
