use async_std::channel::Receiver;

use crate::block::InnerBlock;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::{ExecutionMetadata, StartHandle};

pub(crate) fn spawn_worker<In, Out, OperatorChain>(
    block: InnerBlock<In, Out, OperatorChain>,
) -> StartHandle
where
    In: Clone + Send + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
    let (sender, receiver) = async_std::channel::bounded(1);
    let join_handle = async_std::task::spawn(async move { worker(block, receiver).await });
    StartHandle::new(sender, join_handle)
}

async fn worker<In, Out, OperatorChain>(
    mut block: InnerBlock<In, Out, OperatorChain>,
    metadata_receiver: Receiver<ExecutionMetadata>,
) where
    In: Clone + Send + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
    let metadata = metadata_receiver.recv().await.unwrap();
    drop(metadata_receiver);
    info!(
        "Starting worker for {}: {}",
        metadata.coord,
        block.to_string(),
    );
    // notify the operators that we are about to start
    block.operators.setup(metadata.clone()).await;
    while !matches!(block.operators.next().await, StreamElement::End) {
        // nothing to do
    }
    info!("Worker {} completed, exiting", metadata.coord);
}
