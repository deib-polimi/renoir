use std::cell::UnsafeCell;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub use replay::*;

mod iteration_end;
mod leader;
mod replay;

/// The information about the new state of an iteration:
///
/// - a boolean indicating if a new iteration should start
/// - the new state for the next iteration
pub(crate) type NewIterationState<State> = (bool, State);

/// A shared reference to the state of an iteration,
///
/// This state is shared between all the replicas inside the host. Additional synchronization must
/// be put in place for using this reference safely.
#[derive(Clone, Debug)]
pub(crate) struct IterationStateRef<State> {
    /// The storage for the state.
    ///
    /// Access it via `set` and `get` **with additional synchronization** in place.
    state: Arc<UnsafeCell<State>>,
}

impl<State> IterationStateRef<State> {
    fn new(init: State) -> Self {
        Self {
            state: Arc::new(UnsafeCell::new(init)),
        }
    }

    /// Change the value of the state with the one specified.
    ///
    /// ## Safety
    ///
    /// This will just write to a mutable pointer. Additional synchronization should be put in place
    /// before calling this method. All the references obtained with `get` should be dropped before
    /// calling this method, and no 2 thread can call this simultaneously.
    unsafe fn set(&self, new_state: State) {
        let state_ptr = &mut *self.state.get();
        *state_ptr = new_state;
    }

    /// Obtain a reference to the state.
    ///
    /// ## Safety
    ///
    /// This will just unsafely borrow the local state. Additional synchronization should be put in
    /// place before calling this method. The reference returned by this method should not be used
    /// while calling `set`.
    unsafe fn get(&self) -> &State {
        &*self.state.get()
    }
}

/// We grant that the user of this structs has put enough synchronization to avoid undefined
/// behaviour.
unsafe impl<State: Send + Sync> Send for IterationStateRef<State> {}
/// We grant that the user of this structs has put enough synchronization to avoid undefined
/// behaviour.
unsafe impl<State: Send + Sync> Sync for IterationStateRef<State> {}

/// Handle to the state of the iteration.
#[derive(Clone)]
pub struct IterationStateHandle<T> {
    /// A reference to the output state that will be accessible by the user.
    result: IterationStateRef<T>,
}

impl<T: Clone> IterationStateHandle<T> {
    pub(crate) fn new(init: T) -> Self {
        Self {
            result: IterationStateRef::new(init),
        }
    }

    /// Set the new value for the state of the iteration.
    ///
    /// ## Safety
    ///
    /// It's important that the operator that manages this state takes extra care ensuring that no
    /// undefined behaviour due to data-race is present. Calling `set` while there are still some
    /// references from `get` around is forbidden. Calling `set` while another thread is calling
    /// `set` is forbidden.
    pub(crate) unsafe fn set(&self, new_state: T) {
        self.result.set(new_state);
    }

    /// Obtain a reference to the global iteration state.
    ///
    /// ## Safety
    ///
    /// The returned reference must not be stored in any way, especially between iteration
    /// boundaries (when the state is updated).
    pub fn get(&self) -> &T {
        unsafe { self.result.get() }
    }
}

impl<T> Debug for IterationStateHandle<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IterationStateHandle").finish()
    }
}
