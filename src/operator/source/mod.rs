//! Utility traits and structures related to the source operators.

pub use self::csv::*;
pub use file::*;
pub use iterator::*;
pub use parallel_iterator::*;
pub use async_stream::*;
pub use channel::*;

use crate::operator::{Data, Operator};

mod csv;
mod file;
mod iterator;
mod parallel_iterator;
mod async_stream;
mod channel;

/// This trait marks all the operators that can be used as sinks.
pub trait Source<Out: Data>: Operator<Out> {
    /// The maximum parallelism offered by this operator.
    fn get_max_parallelism(&self) -> Option<usize>;
}
