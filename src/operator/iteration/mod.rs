use std::fmt::{Debug, Formatter};
use std::sync::{Arc, RwLock};

pub use replay::*;

mod replay;

pub type IterationStateRef<Out> = Arc<RwLock<Out>>;

/// Handle to the state of the iteration.
#[derive(Clone)]
pub struct IterationStateHandle<T> {
    /// A reference to the output state that will be accessible by the user.
    result: IterationStateRef<T>,
}

impl<T: Clone> IterationStateHandle<T> {
    pub(crate) fn new(init: T) -> Self {
        Self {
            result: Arc::new(RwLock::new(init)),
        }
    }

    pub(crate) fn set(&self, new_state: T) {
        *self.result.write().unwrap() = new_state;
    }

    pub fn get(&self) -> T {
        // FIXME: this clone should not be necessary, we can return a wrapper to the lock guard
        self.result.read().unwrap().clone()
    }
}

impl<T> Debug for IterationStateHandle<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IterationStateHandle").finish()
    }
}
