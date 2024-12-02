//! Utility traits and structures related to the source operators.

pub use self::csv::*;
#[cfg(feature = "tokio")]
pub use async_stream::*;
#[cfg(feature = "avro")]
pub use avro::*;
pub use channel::*;
pub use file::*;
pub use iterator::*;
#[cfg(feature = "rdkafka")]
pub use kafka::*;
pub use parallel_iterator::*;
#[cfg(feature = "parquet")]
pub use parquet::*;

use crate::{block::Replication, operator::Operator};

#[cfg(feature = "tokio")]
mod async_stream;
#[cfg(feature = "avro")]
mod avro;
mod channel;
mod csv;
mod file;
mod iterator;
#[cfg(feature = "rdkafka")]
mod kafka;
mod parallel_iterator;
#[cfg(feature = "parquet")]
mod parquet;

/// This trait marks all the operators that can be used as source.
pub trait Source: Operator {
    /// The maximum parallelism offered by this operator.
    fn replication(&self) -> Replication;
}
