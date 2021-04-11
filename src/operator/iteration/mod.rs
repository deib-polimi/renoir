use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

pub type IterationStateRef<Out> = Arc<Mutex<Option<Out>>>;

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
    ///
    /// This is an option letting this handle to be cloned. All the clones will have this field set
    /// to `None`, inhibiting this handle. This is ok since the state of all the replicas will be
    /// identical, allowing only one of them to actually update the state.
    result: Option<IterationStateRef<T>>,
}

impl<T> IterationState<T> {
    pub(crate) fn new(init: T) -> (IterationState<T>, IterationStateHandle<T>) {
        let inner = Arc::new(Mutex::new(Some(init)));
        (
            IterationState {
                result: inner.clone(),
            },
            IterationStateHandle {
                result: Some(inner),
            },
        )
    }

    /// Extract the final state of the iteration.
    ///
    /// Calling this before the execution ends will most probably lead to a panic. From the second
    /// call on this function will return `None`.
    pub fn get(self) -> Option<T> {
        self.result
            .try_lock()
            .expect("Cannot lock iteration state")
            .take()
    }
}

impl<T: Clone> IterationStateHandle<T> {
    fn set(&self, new_state: &T) {
        if let Some(result) = &self.result {
            *result.try_lock().expect("Cannot lock iteartion state") = Some(new_state.clone());
        }
    }
}

impl<T> Clone for IterationStateHandle<T> {
    fn clone(&self) -> Self {
        Self { result: None }
    }
}

impl<T> Debug for IterationStateHandle<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IterationStateHandle")
            .field(
                "result",
                &if self.result.is_some() {
                    "Some(...)"
                } else {
                    "None"
                },
            )
            .finish()
    }
}
