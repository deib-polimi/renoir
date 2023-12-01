use std::any::TypeId;
use std::time::Duration;

use crate::block::{BlockStructure, OperatorReceiver, OperatorStructure};
use crate::channel::RecvTimeoutError;
use crate::network::{Coord, NetworkMessage, NetworkReceiver, ReceiverEndpoint};
use crate::operator::start::StartReceiver;
use crate::operator::ExchangeData;
use crate::scheduler::{BlockId, ExecutionMetadata};

/// This will receive the data from a single previous block.
#[derive(Debug)]
pub(crate) struct SimpleStartReceiver<Out: ExchangeData> {
    pub(super) receiver: Option<NetworkReceiver<Out>>,
    previous_replicas: Vec<Coord>,
    pub(super) previous_block_id: BlockId,
}

impl<Out: ExchangeData> SimpleStartReceiver<Out> {
    pub(super) fn new(previous_block_id: BlockId) -> Self {
        Self {
            receiver: None,
            previous_replicas: Default::default(),
            previous_block_id,
        }
    }
}

impl<Out: ExchangeData> StartReceiver for SimpleStartReceiver<Out> {
    type Out = Out;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        let in_type = TypeId::of::<Out>();

        let endpoint = ReceiverEndpoint::new(metadata.coord, self.previous_block_id);
        self.receiver = Some(metadata.network.get_receiver(endpoint));

        for &(prev, typ) in metadata.prev.iter() {
            // ignore this connection because it refers to a different type, another Start
            // in this block will handle it
            if in_type != typ {
                continue;
            }
            if prev.block_id == self.previous_block_id {
                self.previous_replicas.push(prev);
            }
        }
    }

    fn prev_replicas(&self) -> Vec<Coord> {
        self.previous_replicas.clone()
    }

    fn cached_replicas(&self) -> usize {
        0
    }

    fn recv_timeout(&mut self, timeout: Duration) -> Result<NetworkMessage<Out>, RecvTimeoutError> {
        let receiver = self.receiver.as_mut().unwrap();
        receiver.recv_timeout(timeout)
    }

    fn recv(&mut self) -> NetworkMessage<Out> {
        let receiver = self.receiver.as_mut().unwrap();
        receiver.recv().expect("Network receiver failed")
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("Start");
        operator
            .receivers
            .push(OperatorReceiver::new::<Out>(self.previous_block_id));
        BlockStructure::default().add_operator(operator)
    }
}

impl<Out: ExchangeData> Clone for SimpleStartReceiver<Out> {
    fn clone(&self) -> Self {
        Self {
            receiver: None,
            previous_block_id: self.previous_block_id,
            previous_replicas: self.previous_replicas.clone(),
        }
    }
}
