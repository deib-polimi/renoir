use std::cell::RefCell;
use std::thread::JoinHandle;

use crate::block::{Block, BlockStructure};
use crate::network::Coord;
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

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

pub(crate) fn spawn_worker<Out: Data, OperatorChain: Operator<Out> + 'static>(
    mut block: Block<Out, OperatorChain>,
    metadata: &mut ExecutionMetadata,
) -> (JoinHandle<()>, BlockStructure) {
    let coord = metadata.coord;

    debug!("starting worker {}: {}", coord, block.to_string(),);

    block.operators.setup(metadata);
    let structure = block.operators.structure();

    let join_handle = std::thread::Builder::new()
        .name(format!("block-{}", block.id))
        .spawn(move || {
            // remember in the thread-local the coordinate of this block
            COORD.with(|x| *x.borrow_mut() = Some(coord));
            do_work(block, coord)
        })
        .unwrap();

    (join_handle, structure)
}

fn do_work<Out: Data, Op: Operator<Out> + 'static>(mut block: Block<Out, Op>, coord: Coord) {
    let mut catch_panic = CatchPanic::new(|| {
        error!("worker {} crashed!", coord);
    });
    while !matches!(block.operators.next(), StreamElement::Terminate) {
        // nothing to do
    }
    catch_panic.defuse();
    info!("worker {} completed", coord);
}
