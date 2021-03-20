pub use file::*;
pub use stream::*;

use crate::operator::{Data, Operator};

mod file;
mod stream;

pub trait Source<Out: Data>: Operator<Out> {
    fn get_max_parallelism(&self) -> Option<usize>;
}
