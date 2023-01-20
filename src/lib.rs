//! # Noir
//!
//! ### Network of Operators In Rust
//!
//! Noir is a distributed data processing platform based on the dataflow paradigm that provides an ergonomic programming interface, similar to that of Apache Flink, but has much better performance characteristics.
//!
//!
//! Noir converts each job into a dataflow graph of
//! operators and groups them in blocks. Blocks contain a sequence of operors which process the data sequentially without repartitioning it. They are the deployment unit used by the system and can be distributed and executed on multiple systems.
//!
//! The common layout of a Noir program starts with the creation of a `StreamEnvironment`, then one or more `Source`s are initialised creating a `Stream`. The graph of operators is composed using the methods of the `Stream` object, which follow a similar approach to Rust's `Iterator` trait allowing ergonomically define a processing workflow through method chaining.
//!
//! ### Example
//!
//! ```no_run
//! use noir::prelude::*;
//!
//! fn main() {
//!     // Convenience method to parse deployment config from CLI arguments
//!     let (config, args) = EnvironmentConfig::from_args();
//!     let mut env = StreamEnvironment::new(config);
//!     env.spawn_remote_workers();
//!     let source = FileSource::new(&args[0]);
//!     // Create file source
//!     let result = env
//!         .stream(source)
//!         // Split into words
//!         .flat_map(|line| tokenize(&line))
//!         // Partition
//!         .group_by(|word| word.clone())
//!         // Count occurrences
//!         .fold(0, |count, _word| *count += 1)
//!         // Collect result
//!         .collect_vec();
//!     env.execute(); // Start execution (blocking)
//!     if let Some(result) = result.get() {
//!         // Print word counts
//!         println!("{:?}", result);
//!     }
//! }
//!
//! fn tokenize(s: &str) -> Vec<String> {
//!     // Simple tokenisation strategy
//!     s.split_whitespace().map(str::to_lowercase).collect()
//! }
//!
//! // Execute on 6 local hosts `cargo run -- -l 6 input.txt`
//! ```

#[macro_use]
extern crate derivative;
#[macro_use]
extern crate log;

use std::ops::{Add, AddAssign};

use serde::{Deserialize, Serialize};

pub use block::structure;
pub use block::BatchMode;
pub use config::EnvironmentConfig;
pub use environment::StreamEnvironment;
pub use operator::iteration::IterationStateHandle;
pub use scheduler::ExecutionMetadata;
pub use stream::{KeyValue, KeyedStream, KeyedWindowedStream, Stream, WindowedStream};

use crate::block::BlockStructure;
use crate::network::Coord;
use crate::profiler::ProfilerResult;

pub(crate) mod block;
pub(crate) mod channel;
pub mod config;
pub(crate) mod environment;
pub(crate) mod network;
pub mod operator;
mod profiler;
pub(crate) mod runner;
pub(crate) mod scheduler;
pub(crate) mod stream;
#[cfg(test)]
pub(crate) mod test;
pub(crate) mod worker;

pub type CoordUInt = u64;

pub mod prelude {
    pub use super::operator::sink::StreamOutput;
    pub use super::operator::source::*;
    pub use super::operator::window::{CountWindow, EventTimeWindow, ProcessingTimeWindow};
    pub use super::{BatchMode, EnvironmentConfig, StreamEnvironment};
}

/// Tracing information of the current execution.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct TracingData {
    structures: Vec<(Coord, BlockStructure)>,
    profilers: Vec<ProfilerResult>,
}

impl Add for TracingData {
    type Output = TracingData;

    fn add(mut self, rhs: Self) -> Self::Output {
        self += rhs;
        self
    }
}

impl AddAssign for TracingData {
    fn add_assign(&mut self, mut rhs: Self) {
        self.structures.append(&mut rhs.structures);
        self.profilers.append(&mut rhs.profilers);
    }
}
