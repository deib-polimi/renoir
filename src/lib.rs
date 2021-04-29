#[macro_use]
extern crate derivative;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;

use std::ops::{Add, AddAssign};

use serde::{Deserialize, Serialize};

use crate::block::BlockStructure;
use crate::network::Coord;
use crate::profiler::ProfilerResult;

pub mod block;
pub mod channel;
pub mod config;
pub mod environment;
pub mod network;
pub mod operator;
mod profiler;
pub mod runner;
pub mod scheduler;
pub mod stream;
#[doc(hidden)]
pub mod test;
pub mod worker;

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
