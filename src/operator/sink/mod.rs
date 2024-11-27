//! Utility traits and structures related to the sink operators.
//!
//! The actual operators can be found in [`Stream`](crate::Stream) and
//! [`KeyedStream`](crate::KeyedStream).

use std::sync::{Arc, Mutex};

#[cfg(feature = "avro")]
pub(super) mod avro;
pub(super) mod collect;
pub(super) mod collect_channel;
pub(super) mod collect_count;
pub(super) mod collect_vec;
pub(super) mod csv;
pub(super) mod for_each;
pub(super) mod kafka;
#[cfg(feature = "parquet")]
pub(super) mod parquet;
pub(super) mod writer;

pub(crate) type StreamOutputRef<Out> = Arc<Mutex<Option<Out>>>;

/// The result of a stream after the execution.
///
/// This will eventually hold the value _after_ the environment has been fully executed. To access
/// the content of the output you have to call [`StreamOutput::get`].
pub struct StreamOutput<Out>(StreamOutputRef<Out>);

impl<Out> From<StreamOutputRef<Out>> for StreamOutput<Out> {
    fn from(value: StreamOutputRef<Out>) -> Self {
        Self(value)
    }
}

impl<Out> StreamOutput<Out> {
    /// Obtain the content of the output.
    ///
    /// This will consume the result and return the owned content. If the content has already been
    /// extracted, or if the content is not ready yet, this will return `None`.
    pub fn get(self) -> Option<Out> {
        self.0.try_lock().expect("Cannot lock output result").take()
    }
}
