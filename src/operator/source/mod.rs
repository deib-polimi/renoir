use serde::de::DeserializeOwned;
use serde::Serialize;

pub use file::*;
pub use stream::*;

use crate::operator::Operator;

mod file;
mod stream;

pub trait Source<Out>: Operator<Out>
where
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    fn get_max_parallelism(&self) -> Option<usize>;
}
