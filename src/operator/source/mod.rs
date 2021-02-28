use crate::operator::Operator;
pub use start::*;
pub use stream::*;

mod start;
mod stream;

pub trait Source<Out>: Operator<Out> {}
