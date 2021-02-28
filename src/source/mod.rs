use crate::operator::Operator;
pub use stream::*;

mod stream;

pub trait Source<Out>: Operator<Out> {}
