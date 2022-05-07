use std::cell::RefCell;

use crate::block::{BlockStructure, InnerBlock};
use crate::channel::{self, Receiver, UnboundedSender};
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

/// Call a function if this struct goes out of scope without calling `defuse`, including during a
/// panic stack-unwinding.
struct CatchPanic<F: FnOnce()> {
    /// True if the function should be called.
    primed: bool,
    /// Function to call.
    ///
    /// The `Drop` implementation will move out the function.
    handler: Option<F>,
}

impl<F: FnOnce()> CatchPanic<F> {
    fn new(handler: F) -> Self {
        Self {
            primed: true,
            handler: Some(handler),
        }
    }

    /// Avoid calling the function on drop.
    fn defuse(&mut self) {
        self.primed = false;
    }
}

impl<F: FnOnce()> Drop for CatchPanic<F> {
    fn drop(&mut self) {
        if self.primed {
            (self.handler.take().unwrap())();
        }
    }
}

pub(crate) fn spawn_worker<Out: Data, OperatorChain>(
    block: InnerBlock<Out, OperatorChain>,
    structure_sender: UnboundedSender<(Coord, BlockStructure)>,
) -> StartHandle
where
    OperatorChain: Operator<Out> + 'static,
{
    let (sender, receiver) = channel::bounded(1);
    let join_handle = std::thread::Builder::new()
        .name(format!("Block{}", block.id))
        .spawn(move || worker(block, receiver, structure_sender))
        .unwrap();
    StartHandle::new(sender, join_handle)
}

fn worker<Out: Data, OperatorChain>(
    mut block: InnerBlock<Out, OperatorChain>,
    metadata_receiver: Receiver<ExecutionMetadata>,
    structure_sender: UnboundedSender<(Coord, BlockStructure)>,
) where
    OperatorChain: Operator<Out> + 'static,
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

    let structure = block.operators.structure();
    structure_sender.send((metadata.coord, structure)).unwrap();
    drop(structure_sender);

    let mut catch_panic = CatchPanic::new(|| {
        error!("Worker {} has crashed!", metadata.coord);
    });
    while !matches!(block.operators.next(), StreamElement::Terminate) {
        // nothing to do
    }
    catch_panic.defuse();
    info!("Worker {} completed, exiting", metadata.coord);
}
