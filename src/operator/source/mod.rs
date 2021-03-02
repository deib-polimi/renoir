pub use stream::*;

use crate::operator::Operator;

mod stream;

pub trait Source<Out>: Operator<Out>
where
    Out: Clone + Send + 'static,
{
}
