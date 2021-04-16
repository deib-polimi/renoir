use std::any::TypeId;
use std::collections::VecDeque;

use crate::block::{BlockStructure, OperatorReceiver, OperatorStructure};
use crate::network::{NetworkMessage, NetworkReceiver, ReceiverEndpoint};
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::BlockId;

#[derive(Debug, Derivative)]
#[derivative(Clone)]
pub struct StartBlock<Out: Data> {
    metadata: Option<ExecutionMetadata>,
    #[derivative(Clone(clone_with = "clone_empty"))]
    receivers: Vec<NetworkReceiver<NetworkMessage<Out>>>,
    buffer: VecDeque<StreamElement<Out>>,
    missing_ends: usize,
    num_prev: usize,
    is_iter_end: bool,
    /// The last call to next caused a timeout, so a FlushBatch has been emitted. In this case this
    /// field is set to true to avoid flooding with FlushBatch.
    already_timed_out: bool,
    /// The id of the previous blocks in the job graph.
    prev_block_ids: Vec<BlockId>,
}

impl<Out: Data> StartBlock<Out> {
    pub(crate) fn new(prev_block_id: BlockId) -> Self {
        Self {
            metadata: Default::default(),
            receivers: Default::default(),
            buffer: Default::default(),
            missing_ends: Default::default(),
            num_prev: Default::default(),
            is_iter_end: Default::default(),
            already_timed_out: Default::default(),
            prev_block_ids: vec![prev_block_id],
        }
    }

    pub(crate) fn concat(prev_block_ids: Vec<BlockId>) -> Self {
        Self {
            metadata: Default::default(),
            receivers: Default::default(),
            buffer: Default::default(),
            missing_ends: Default::default(),
            num_prev: Default::default(),
            is_iter_end: Default::default(),
            already_timed_out: Default::default(),
            prev_block_ids,
        }
    }
}

impl<Out: Data> Operator<Out> for StartBlock<Out> {
    fn setup(&mut self, metadata: ExecutionMetadata) {
        let in_type = TypeId::of::<Out>();
        for &prev_block_id in &self.prev_block_ids {
            let mut network = metadata.network.lock().unwrap();
            let endpoint = ReceiverEndpoint::new(metadata.coord, prev_block_id);
            let receiver = network.get_receiver(endpoint);
            self.receivers.push(receiver);
            drop(network);
            for &(prev, typ) in metadata.prev.iter() {
                // ignore this connection because it refers to a different type, another StartBlock
                // in this block will handle it
                if in_type != typ {
                    continue;
                }
                if prev.block_id == prev_block_id {
                    self.missing_ends += 1;
                    self.num_prev += 1;
                }
            }
        }
        info!(
            "StartBlock {} initialized, {:?} are the previous blocks, a total of {} previous replicas",
            metadata.coord, self.prev_block_ids, self.num_prev
        );
        self.metadata = Some(metadata);
    }

    fn next(&mut self) -> StreamElement<Out> {
        let metadata = self.metadata.as_ref().unwrap();
        // all the previous blocks sent an end: we're done
        if self.missing_ends == 0 {
            return if self.is_iter_end {
                info!("StartBlock for {} has ended an iteration", metadata.coord);
                self.is_iter_end = false;
                self.missing_ends = self.num_prev;
                StreamElement::IterEnd
            } else {
                info!("StartBlock for {} has ended", metadata.coord);
                StreamElement::End
            };
        }
        if self.buffer.is_empty() {
            let max_delay = metadata.batch_mode.max_delay();
            let buf = match (self.already_timed_out, max_delay) {
                // check the timeout only if there is one and the last time we didn't timed out
                (false, Some(max_delay)) => {
                    match NetworkReceiver::select_any_timeout(&self.receivers, max_delay) {
                        Ok(buf) => buf.result.expect("One of the receivers failed"),
                        Err(_) => {
                            // timed out: tell the block to flush the current batch
                            // next time we wait indefinitely without the timeout since the batch is
                            // currently empty
                            self.already_timed_out = true;
                            vec![StreamElement::FlushBatch]
                        }
                    }
                }
                _ => {
                    self.already_timed_out = false;
                    NetworkReceiver::select_any(&self.receivers)
                        .result
                        .expect("One of the receivers failed")
                }
            };
            self.buffer = buf.into();
        }
        let message = self
            .buffer
            .pop_front()
            .expect("Previous block sent an empty message");
        if matches!(message, StreamElement::IterEnd) {
            self.missing_ends -= 1;
            self.is_iter_end = true;
            debug!(
                "{}/{:?} received an IterEnd, {} more to come",
                metadata.coord, self.prev_block_ids, self.missing_ends
            );
            return self.next();
        }
        if matches!(message, StreamElement::End) {
            self.missing_ends -= 1;
            self.is_iter_end = false;
            debug!(
                "{}/{:?} received an end, {} more to come",
                metadata.coord, self.prev_block_ids, self.missing_ends
            );
            return self.next();
        }
        message
    }

    fn to_string(&self) -> String {
        format!("[{}]", std::any::type_name::<Out>())
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("StartBlock");
        for &block_id in &self.prev_block_ids {
            operator
                .receivers
                .push(OperatorReceiver::new::<Out>(block_id));
        }
        BlockStructure::default().add_operator(operator)
    }
}

fn clone_empty<T>(_: &[T]) -> Vec<T> {
    Vec::new()
}
