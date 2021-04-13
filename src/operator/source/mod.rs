pub use crate::operator::source::csv::*;
pub use event_time_iterator::*;
pub use file::*;
pub use iterator::*;

use crate::operator::{Data, Operator};

mod csv;
mod event_time_iterator;
mod file;
mod iterator;

pub trait Source<Out: Data>: Operator<Out> {
    fn get_max_parallelism(&self) -> Option<usize>;
}
