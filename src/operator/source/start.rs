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
        info!("StartBlock {} initialized", metadata.coord);
    }

    async fn next(&mut self) -> StreamElement<Out> {
        if self.buffer.is_empty() {
            let receiver = self.receiver.as_ref().unwrap();
            let buf = receiver.recv().await.unwrap();
            self.buffer.append(&mut buf.into());
        }
        self.buffer.pop_front().unwrap_or(StreamElement::End)
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
        }
    }
}
