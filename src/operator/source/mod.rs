pub use event_time_stream::*;
pub use file::*;
pub use stream::*;

use crate::operator::{Data, Operator};

mod event_time_stream;
mod file;
mod stream;

pub trait Source<Out: Data>: Operator<Out> {
    fn get_max_parallelism(&self) -> Option<usize>;
}
