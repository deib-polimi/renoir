use std::fmt::{Debug, Formatter};
use std::sync::{Arc, RwLock};

pub use replay::*;

mod replay;

pub type IterationStateRef<Out> = Arc<RwLock<Option<Out>>>;

/// An handle for accessing the state of the computation at the end of the execution.
pub struct IterationState<T> {
    /// A reference shared with the handle that will be used by the iteration block as a way to
    /// output the state.
    result: IterationStateRef<T>,
}

/// Handle to the state of the iteration.
///
/// It has to be passed to an iteration operator for filling the value after each iteration.
pub struct IterationStateHandle<T> {
    /// A reference to the output state that will be accessible by the user.
    result: IterationStateRef<T>,
    /// All the cloned handles won't be leaders, meaning they are read-only.
    is_leader: bool,
}

impl<T> IterationState<T> {
    pub(crate) fn new(init: T) -> (IterationState<T>, IterationStateHandle<T>) {
        let inner = Arc::new(RwLock::new(Some(init)));
        (
            IterationState {
                result: inner.clone(),
            },
            IterationStateHandle {
                result: inner,
                is_leader: true,
            },
        )
    }

    /// Extract the final state of the iteration.
    ///
    /// Calling this before the execution ends will most probably lead to a panic. From the second
    /// call on this function will return `None`.
    pub fn get(self) -> Option<T> {
        self.result
            .try_write()
            .expect("Cannot lock iteration state")
            .take()
    }
}

impl<T: Clone> IterationStateHandle<T> {
    fn set(&self, new_state: &T) {
        if self.is_leader {
            *self.result.write().expect("Cannot lock iteration state") = Some(new_state.clone());
        }
    }

    pub fn get(&self) -> T {
        self.result
            .read()
            .unwrap()
            .clone()
            .expect("corrupted handle")
    }
}

impl<T> Clone for IterationStateHandle<T> {
    fn clone(&self) -> Self {
        Self {
            result: self.result.clone(),
            is_leader: false,
        }
    }
}

impl<T> Debug for IterationStateHandle<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IterationStateHandle")
            .field("is_leader", &self.is_leader)
            .finish()
    }
}
