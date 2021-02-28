use crate::block::InnerBlock;
use crate::environment::StartHandle;
use crate::operator::Operator;

pub struct ExecutionMetadata {
    // TODO: previous nodes, next nodes
}

pub fn spawn_worker<In, Out, OperatorChain>(
    mut block: InnerBlock<In, Out, OperatorChain>,
) -> StartHandle
where
    In: Send + 'static,
    Out: Send + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
    let (sender, receiver) = async_std::channel::bounded(1);
    let join_handle = async_std::task::spawn(async move {
        let metadata = receiver.recv().await.unwrap();
        drop(receiver);
        // TODO: inform the block about the network topology
        // TODO: call .next() and send to the next nodes
        block.operators.next().await;
    });
    StartHandle {
        starter: sender,
        join_handle,
    }
}