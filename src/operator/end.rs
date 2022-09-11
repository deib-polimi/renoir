use std::fmt::Display;

use crate::block::{
    BatchMode, Batcher, BlockSenders, BlockStructure, Connection, NextStrategy, OperatorStructure,
};
use crate::network::{Coord, ReceiverEndpoint};
use crate::operator::{ExchangeData, KeyerFn, Operator, StreamElement};
use crate::scheduler::{BlockId, ExecutionMetadata};

#[derive(Derivative)]
#[derivative(Clone, Debug)]
pub struct EndBlock<Out: ExchangeData, OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<u64, Out>,
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    coord: Option<Coord>,
    next_strategy: NextStrategy<Out, IndexFn>,
    batch_mode: BatchMode,
    block_senders: Vec<BlockSenders>,
    #[derivative(Debug = "ignore", Clone(clone_with = "clone_default"))]
    senders: Vec<(ReceiverEndpoint, Batcher<Out>)>,
    feedback_id: Option<BlockId>,
    ignore_block_ids: Vec<BlockId>,
}

impl<Out: ExchangeData, OperatorChain, IndexFn> Display for EndBlock<Out, OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<u64, Out>,
    OperatorChain: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.next_strategy {
            NextStrategy::Random => write!(f, "{} -> Shuffle", self.prev),
            NextStrategy::OnlyOne => write!(f, "{} -> OnlyOne", self.prev),
            _ => self.prev.fmt(f),
        }
    }
}

impl<Out: ExchangeData, OperatorChain, IndexFn> EndBlock<Out, OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<u64, Out>,
    OperatorChain: Operator<Out>,
{
    pub(crate) fn new(
        prev: OperatorChain,
        next_strategy: NextStrategy<Out, IndexFn>,
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

    /// Mark this `EndBlock` as the end of a feedback loop.
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

impl<Out: ExchangeData, OperatorChain, IndexFn> Operator<()>
    for EndBlock<Out, OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<u64, Out>,
    OperatorChain: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        let senders = metadata.network.get_senders(metadata.coord);
        // remove the ignored destinations
        self.senders = senders
            .into_iter()
            .filter(|(endpoint, _)| !self.ignore_block_ids.contains(&endpoint.coord.block_id))
            .map(|(coord, sender)| (coord, Batcher::new(sender, self.batch_mode, metadata.coord)))
            .collect();

        // group the senders based on the strategy
        self.block_senders = self
            .next_strategy
            .block_senders(&self.senders, Some(metadata.coord.block_id));

        self.coord = Some(metadata.coord);
    }

    fn next(&mut self) -> StreamElement<()> {
        let message = self.prev.next();
        let to_return = message.take();
        match &message {
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
                        // make sure to flush at the end of each iteration
                        if matches!(message, StreamElement::FlushAndRestart) {
                            sender.1.flush();
                        }
                    }
                }
            }
            StreamElement::Item(item) | StreamElement::Timestamped(item, _) => {
                let index = self.next_strategy.index(item);
                for block in self.block_senders.iter() {
                    let index = index % block.indexes.len();
                    let sender_idx = block.indexes[index];
                    self.senders[sender_idx].1.enqueue(message.clone());
                }
            }
            StreamElement::FlushBatch => {
                for (_, batcher) in self.senders.iter_mut() {
                    batcher.flush();
                }
            }
        };

        if matches!(to_return, StreamElement::Terminate) {
            let coord = self.coord.unwrap();
            debug!(
                "EndBlock at {} received Terminate, closing {} channels",
                coord,
                self.senders.len()
            );
            for (_, batcher) in self.senders.drain(..) {
                batcher.end();
            }
        }
        to_return
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("EndBlock");
        for sender_group in &self.block_senders {
            if !sender_group.indexes.is_empty() {
                let block_id = self.senders[sender_group.indexes[0]].0.coord.block_id;
                operator
                    .connections
                    .push(Connection::new::<Out, _>(block_id, &self.next_strategy));
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
