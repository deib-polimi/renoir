use std::collections::VecDeque;

use crate::network::{NetworkMessage, NetworkReceiver, ReceiverEndpoint};
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::BlockId;

#[derive(Debug, Derivative)]
#[derivative(Clone)]
pub struct StartBlock<Out: Data> {
    metadata: Option<ExecutionMetadata>,
    #[derivative(Clone(clone_with = "clone_none"))]
    receiver: Option<NetworkReceiver<NetworkMessage<Out>>>,
    buffer: VecDeque<StreamElement<Out>>,
    missing_ends: usize,
    /// The last call to next caused a timeout, so a FlushBatch has been emitted. In this case this
    /// field is set to true to avoid flooding with FlushBatch.
    already_timed_out: bool,
    /// The id of the previous block in the job graph. This block is unique since `StartBlock` does
    /// not support multiple inputs.
    prev_block_id: BlockId,
}

impl<Out: Data> StartBlock<Out> {
    pub(crate) fn new(prev_block_id: BlockId) -> Self {
        Self {
            metadata: Default::default(),
            receiver: Default::default(),
            buffer: Default::default(),
            missing_ends: Default::default(),
            already_timed_out: Default::default(),
            prev_block_id,
        }
    }
}

impl<Out: Data> Operator<Out> for StartBlock<Out> {
    fn setup(&mut self, metadata: ExecutionMetadata) {
        let mut network = metadata.network.lock().unwrap();
        let endpoint = ReceiverEndpoint::new(metadata.coord, self.prev_block_id);
        let receiver = network.get_receiver(endpoint);
        self.receiver = Some(receiver);
        drop(network);
        for &prev in metadata.prev.iter() {
            if prev.block_id == self.prev_block_id {
                self.missing_ends += 1;
            }
        }
        info!(
            "StartBlock {} initialized, {} previous blocks, receiver is: {:?}",
            metadata.coord, self.missing_ends, self.receiver
        );
        self.metadata = Some(metadata);
    }

    fn next(&mut self) -> StreamElement<Out> {
        let metadata = self.metadata.as_ref().unwrap();
        // all the previous blocks sent and end: we're done
        if self.missing_ends == 0 {
            info!("StartBlock for {} has ended", metadata.coord);
            return StreamElement::End;
        }
        let receiver = self.receiver.as_ref().unwrap();
        if self.buffer.is_empty() {
            let max_delay = metadata.batch_mode.max_delay();
            let buf = match (self.already_timed_out, max_delay) {
                // check the timeout only if there is one and the last time we didn't timed out
                (false, Some(max_delay)) => match receiver.recv_timeout(max_delay) {
                    Ok(buf) => buf,
                    Err(_) => {
                        // timed out: tell the block to flush the current batch
                        // next time we wait indefinitely without the timeout since the batch is
                        // currently empty
                        self.already_timed_out = true;
                        vec![StreamElement::FlushBatch]
                    }
                },
                _ => {
                    self.already_timed_out = false;
                    receiver.recv().unwrap()
                }
            };
            self.buffer = buf.into();
        }
        let message = self
            .buffer
            .pop_front()
            .expect("Previous block sent an empty message");
        if matches!(message, StreamElement::End) {
            self.missing_ends -= 1;
            debug!(
                "{} received an end, {} more to come",
                metadata.coord, self.missing_ends
            );
            return self.next();
        }
        message
    }

    fn to_string(&self) -> String {
        format!("[{}]", std::any::type_name::<Out>())
    }
}

fn clone_none<T>(_: &Option<T>) -> Option<T> {
    None
}
