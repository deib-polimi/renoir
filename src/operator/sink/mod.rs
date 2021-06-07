use std::sync::{Arc, Mutex};

pub use collect_vec::*;
pub use for_each::*;

use crate::operator::Operator;

mod collect_vec;
mod for_each;

pub trait Sink: Operator<()> {}

pub(crate) type StreamOutputRef<Out> = Arc<Mutex<Option<Out>>>;

pub struct StreamOutput<Out> {
    result: StreamOutputRef<Out>,
}

impl<Out> StreamOutput<Out> {
    pub fn get(self) -> Option<Out> {
        self.result
            .try_lock()
            .expect("Cannot lock output result")
            .take()
    }
}
