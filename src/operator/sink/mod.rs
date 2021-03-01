use async_std::sync::{Arc, Mutex};

pub use collect_vec::*;
pub use end::*;
pub use group_by::*;

use crate::operator::Operator;

mod collect_vec;
mod end;
mod group_by;

pub trait Sink: Operator<()> {}

pub type StreamOutputRef<Out> = Arc<Mutex<Option<Out>>>;

pub struct StreamOutput<Out> {
    result: Arc<Mutex<Option<Out>>>,
}

impl<Out> StreamOutput<Out> {
    pub fn get(self) -> Option<Out> {
        self.result
            .try_lock()
            .expect("Cannot lock output result")
            .take()
    }
}
