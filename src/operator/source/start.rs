use std::collections::VecDeque;

use async_trait::async_trait;

use crate::block::ExecutionMetadataRef;
use crate::network::{NetworkMessage, NetworkReceiver};
use crate::operator::{Operator, StreamElement};

pub struct StartBlock<Out>
where
    Out: Clone + Send + 'static,
{
    metadata: Option<ExecutionMetadataRef>,
    receiver: Option<NetworkReceiver<NetworkMessage<Out>>>,
    buffer: VecDeque<StreamElement<Out>>,
    missing_ends: usize,
}

impl<Out> StartBlock<Out>
where
    Out: Clone + Send + 'static,
{
    pub fn new() -> Self {
        StartBlock {
            metadata: None,
            receiver: None,
            buffer: Default::default(),
            missing_ends: 0,
        }
    }
}

#[async_trait]
impl<Out> Operator<Out> for StartBlock<Out>
where
    Out: Clone + Send + 'static,
{
    fn block_init(&mut self, metadata: ExecutionMetadataRef) {
        self.metadata = Some(metadata.clone());
    }

    async fn start(&mut self) {
        let metadata = self.metadata.as_ref().unwrap().get().unwrap();
        let mut network = metadata.network.lock().await;
        self.receiver = Some(network.get_receiver(metadata.coord));
        self.missing_ends = metadata.num_prev;
        info!(
            "StartBlock {} initialized, {} previous blocks, receiver is: {:?}",
            metadata.coord, metadata.num_prev, self.receiver
        );
    }

    async fn next(&mut self) -> StreamElement<Out> {
        // all the previous blocks sent and end: we're done
        if self.missing_ends == 0 {
            let metadata = self.metadata.as_ref().unwrap().get().unwrap();
            info!("StartBlock for {} has ended", metadata.coord);
            return StreamElement::End;
        }
        let receiver = self.receiver.as_ref().unwrap();
        if self.buffer.is_empty() {
            // receive from any previous block
            let buf = receiver.recv().await.unwrap();
            self.buffer.append(&mut buf.into());
        }
        let message = self
            .buffer
            .pop_front()
            .expect("Previous block sent an empty message");
        if matches!(message, StreamElement::End) {
            let metadata = self.metadata.as_ref().unwrap().get().unwrap();
            self.missing_ends -= 1;
            debug!(
                "{} received an end, {} more to come",
                metadata.coord, self.missing_ends
            );
            return self.next().await;
        }
        message
    }

    fn to_string(&self) -> String {
        format!("[{}]", std::any::type_name::<Out>())
    }
}

impl<Out> Clone for StartBlock<Out>
where
    Out: Clone + Send + 'static,
{
    fn clone(&self) -> Self {
        if self.metadata.is_some() {
            panic!("Cannot clone once initialized");
        }
        Self {
            metadata: None,
            receiver: None,
            buffer: Default::default(),
            missing_ends: 0,
        }
    }
}
