use std::collections::HashMap;
use std::sync::Arc;

use rand::{thread_rng, Rng};

use crate::network::{NetworkMessage, NetworkSender, ReceiverEndpoint};
use crate::operator::Data;
use crate::stream::BlockId;

/// The list with the interesting senders of a single block.
#[derive(Debug, Clone)]
pub(crate) struct SenderList(pub Vec<ReceiverEndpoint>);

/// The next strategy used at the end of a block.
///
/// A block in the job graph may have many next blocks. Each of them will receive the message, which
/// of their replica will receive it depends on the value of the next strategy.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub(crate) enum NextStrategy<Out: Data> {
    /// Only one of the replicas will receive the message:
    ///
    /// - if the block is not replicated, the only replica will receive the message
    /// - if the next block is replicated as much as the current block the corresponding replica
    ///   will receive the message
    /// - otherwise the execution graph is malformed  
    OnlyOne,
    /// A random replica will receive the message.
    Random,
    /// Among the next replica, the one is selected based on the hash of the key of the message.
    GroupBy(#[derivative(Debug = "ignore")] Arc<dyn Fn(&Out) -> usize + Send + Sync>),
}

impl<Out: Data> NextStrategy<Out> {
    /// Group the senders from a block using the current next strategy.
    ///
    /// The returned value is a list of `SenderList`s, one for each next block in the execution
    /// graph. The messages will be sent to one replica of each group, according to the strategy.
    ///
    /// If `ignore_block` is specified, no senders to the specified block will be returned.
    pub fn group_senders(
        &self,
        senders: &HashMap<ReceiverEndpoint, NetworkSender<NetworkMessage<Out>>>,
        ignore_block: Option<BlockId>,
    ) -> Vec<SenderList> {
        let mut by_block_id: HashMap<_, Vec<_>> = HashMap::new();
        for (coord, sender) in senders {
            by_block_id
                .entry(coord.coord.block_id)
                .or_default()
                .push(sender.receiver_endpoint);
        }
        if let Some(ignore_block) = ignore_block {
            by_block_id.remove(&ignore_block);
        }
        let mut senders = Vec::new();
        for (_block_id, mut block_senders) in by_block_id {
            block_senders.sort_unstable();
            if matches!(self, NextStrategy::OnlyOne) {
                assert_eq!(block_senders.len(), 1, "OnlyOne must have a single sender");
            }
            senders.push(SenderList(block_senders));
        }
        senders
    }

    /// Compute the index of the replica which this message should be forwarded to.
    pub fn index(&self, message: &Out) -> usize {
        match self {
            NextStrategy::OnlyOne => 0,
            NextStrategy::Random => thread_rng().gen(),
            NextStrategy::GroupBy(keyer) => keyer(message),
        }
    }
}
