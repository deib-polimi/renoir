pub use crate::operator::source::csv::*;
pub use channel::*;
pub use file::*;
pub use iterator::*;

use crate::operator::{Data, Operator};

mod channel;
mod csv;
mod file;
mod iterator;

pub trait Source<Out: Data>: Operator<Out> {
    fn get_max_parallelism(&self) -> Option<usize>;
}
