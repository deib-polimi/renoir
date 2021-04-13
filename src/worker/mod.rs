use std::cell::RefCell;

use crate::block::InnerBlock;
use crate::channel::BoundedChannelReceiver;
use crate::network::Coord;
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::{ExecutionMetadata, StartHandle};

thread_local! {
    /// Coordinates of the replica the current worker thread is working on.
    ///
    /// Access to this by calling `replica_coord()`.
    static COORD: RefCell<Option<Coord>> = RefCell::new(None);
}

/// Get the coord of the replica the current thread is working on.
///
/// This will return `Some(coord)` only when called from a worker thread of a replica, otherwise
/// `None` is returned.
pub fn replica_coord() -> Option<Coord> {
    COORD.with(|x| *x.borrow())
}

pub(crate) fn spawn_worker<Out: Data, OperatorChain>(
    block: InnerBlock<Out, OperatorChain>,
) -> StartHandle
where
    OperatorChain: Operator<Out> + Send + 'static,
{
    let (sender, receiver) = BoundedChannelReceiver::new(1);
    let join_handle = std::thread::Builder::new()
        .name(format!("Block{}", block.id))
        .spawn(move || worker(block, receiver))
        .unwrap();
    StartHandle::new(sender, join_handle)
}

fn worker<Out: Data, OperatorChain>(
    mut block: InnerBlock<Out, OperatorChain>,
    metadata_receiver: BoundedChannelReceiver<ExecutionMetadata>,
) where
    OperatorChain: Operator<Out> + Send + 'static,
{
    let metadata = metadata_receiver.recv().unwrap();
    drop(metadata_receiver);
    info!(
        "Starting worker for {}: {}",
        metadata.coord,
        block.to_string(),
    );
    // remember in the thread-local the coordinate of this block
    COORD.with(|x| *x.borrow_mut() = Some(metadata.coord));
    // notify the operators that we are about to start
    block.operators.setup(metadata.clone());
    while !matches!(block.operators.next(), StreamElement::End) {
        // nothing to do
    }
    info!("Worker {} completed, exiting", metadata.coord);
}
