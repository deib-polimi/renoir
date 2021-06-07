pub use channel::*;
pub use file::*;
pub use iterator::*;

pub use crate::operator::source::csv::*;
use crate::operator::{Data, Operator};

mod channel;
mod csv;
mod file;
mod iterator;

pub trait Source<Out: Data>: Operator<Out> {
    fn get_max_parallelism(&self) -> Option<usize>;
}
