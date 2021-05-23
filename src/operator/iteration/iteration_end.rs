use crate::block::{BlockStructure, Connection, NextStrategy, OperatorStructure};
use crate::network::{Coord, NetworkMessage, NetworkSender, ReceiverEndpoint};
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::BlockId;

/// Similar to `EndBlock`, but tied specifically for the iterations.
///
/// This block will receive the data (i.e. the `DeltaUpdate` already reduced) and send back to the
/// leader.
///
/// `EndBlock` cannot be used here since special care should be taken when the input stream is
/// empty.
#[derive(Debug, Clone)]
pub struct IterationEndBlock<DeltaUpdate: ExchangeData, OperatorChain>
where
    OperatorChain: Operator<DeltaUpdate>,
{
    /// The chain of previous operators.
    ///
    /// At the end of this chain there should be the local reduction.
    prev: OperatorChain,
    /// Whether, since the last `IterEnd`, an element has been received.
    ///
    /// If two `IterEnd` are received in a row it means that the local reduction didn't happen since
    /// no item was present in the stream. A delta update should be sent to the leader nevertheless.
    has_received_item: bool,
    /// The block id of the block containing the `IterationLeader` operator.
    leader_block_id: BlockId,
    /// The sender that points to the `IterationLeader` for sending the `DeltaUpdate` messages.
    leader_sender: Option<NetworkSender<DeltaUpdate>>,
    /// The coordinates of this block.
    coord: Coord,
}

impl<DeltaUpdate: ExchangeData, OperatorChain> IterationEndBlock<DeltaUpdate, OperatorChain>
where
    OperatorChain: Operator<DeltaUpdate>,
{
    pub fn new(prev: OperatorChain, leader_block_id: BlockId) -> Self {
        Self {
            prev,
            has_received_item: false,
            leader_block_id,
            leader_sender: None,
            coord: Default::default(),
        }
    }
}

impl<DeltaUpdate: ExchangeData, OperatorChain> Operator<()>
    for IterationEndBlock<DeltaUpdate, OperatorChain>
where
    DeltaUpdate: Default,
    OperatorChain: Operator<DeltaUpdate>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        let mut network = metadata.network.lock().unwrap();

        let replicas = network.replicas(self.leader_block_id);
        assert_eq!(
            replicas.len(),
            1,
            "The IterationEndBlock block should not be replicated"
        );
        let leader = replicas.into_iter().next().unwrap();
        debug!(
            "IterationEndBlock {} has {} as leader",
            metadata.coord, leader
        );

        let sender = network.get_sender(ReceiverEndpoint::new(leader, metadata.coord.block_id));
        self.leader_sender = Some(sender);

        drop(network);

        self.coord = metadata.coord;
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<()> {
        let elem = self.prev.next();
        match &elem {
            StreamElement::Item(_) => {
                let message = NetworkMessage::new(vec![elem], self.coord);
                self.leader_sender.as_ref().unwrap().send(message).unwrap();
                self.has_received_item = true;
                StreamElement::Item(())
            }
            StreamElement::FlushAndRestart => {
                // If two FlushAndRestart have been received in a row it means that no message went
                // through the iteration inside this replica. Nevertheless the DeltaUpdate must be
                // sent to the leader.
                if !self.has_received_item {
                    let update = Default::default();
                    let message =
                        NetworkMessage::new(vec![StreamElement::Item(update)], self.coord);
                    let sender = self.leader_sender.as_ref().unwrap();
                    sender.send(message).unwrap();
                }
                self.has_received_item = false;
                StreamElement::FlushAndRestart
            }
            StreamElement::Terminate => {
                let message = NetworkMessage::new(vec![StreamElement::Terminate], self.coord);
                self.leader_sender.as_ref().unwrap().send(message).unwrap();
                StreamElement::Terminate
            }
            StreamElement::FlushBatch => elem.map(|_| unreachable!()),
            _ => unreachable!(),
        }
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> IterationEndBlock<{}>",
            self.prev.to_string(),
            std::any::type_name::<DeltaUpdate>()
        )
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<DeltaUpdate, _>("IterationEndBlock");
        operator.connections.push(Connection::new::<DeltaUpdate, _>(
            self.leader_block_id,
            &NextStrategy::only_one(),
        ));
        self.prev.structure().add_operator(operator)
    }
}
