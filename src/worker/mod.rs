use crate::block::InnerBlock;
use crate::environment::StartHandle;
use crate::operator::Operator;

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
        let _ = receiver.recv().await.unwrap();
        drop(receiver);
        // TODO: inform the block about the network topology
        block.operators.next().await;
    });
    StartHandle {
        starter: sender,
        join_handle,
    }
}
