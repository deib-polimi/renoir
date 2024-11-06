use serde::{Deserialize, Serialize};
#[cfg(feature = "profiler")]
pub use with_profiler::*;
#[cfg(not(feature = "profiler"))]
pub use without_profiler::*;

use crate::{block::BlockStructure, network::Coord, scheduler::BlockId};

#[cfg(feature = "profiler")]
mod bucket_profiler;

#[cfg(feature = "ssh")]
pub const TRACING_PREFIX: &str = "__renoir_TRACING_DATA__";

/// The available profiling metrics.
///
/// Calling one of those function will store the event inside the current profiler, if any. All of
/// them are no-op if the `profiler` feature is not enabled.
pub trait Profiler {
    /// Increase the number of received items in a block.
    fn items_in(&mut self, from: Coord, to: Coord, amount: usize);
    /// Increase the number of sent items from a block.
    fn items_out(&mut self, from: Coord, to: Coord, amount: usize);
    /// Increase the number of received bytes from the network to a block.
    fn net_bytes_in(&mut self, from: Coord, to: Coord, amount: usize);
    /// Increase the number of sent bytes from the network from a block.
    fn net_bytes_out(&mut self, from: Coord, to: Coord, amount: usize);
    /// Mark the end of an iteration.
    fn iteration_boundary(&mut self, leader_block_id: BlockId);
}

/// Tracing information of the current execution.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct TracingData {
    pub structures: Vec<(Coord, BlockStructure)>,
    pub profilers: Vec<ProfilerResult>,
}

// impl Add for TracingData {
//     type Output = TracingData;

//     fn add(mut self, rhs: Self) -> Self::Output {
//         self += rhs;
//         self
//     }
// }

// impl AddAssign for TracingData {
//     fn add_assign(&mut self, mut rhs: Self) {
//         self.structures.append(&mut rhs.structures);
//         self.profilers.append(&mut rhs.profilers);
//     }
// }

pub fn log_trace(structures: Vec<(Coord, BlockStructure)>, profilers: Vec<ProfilerResult>) {
    if !cfg!(feature = "profiler") {
        return;
    }

    use std::io::Write as _;
    let data = TracingData {
        structures,
        profilers,
    };

    let mut stderr = std::io::stderr().lock();
    writeln!(
        stderr,
        "__renoir_TRACING_DATA__{}",
        serde_json::to_string(&data).unwrap()
    )
    .unwrap();
}

#[cfg(feature = "ssh")]
#[inline]
pub fn try_parse_trace(s: &str) -> Option<TracingData> {
    if let Some(s) = s.strip_prefix(TRACING_PREFIX) {
        match serde_json::from_str::<TracingData>(s) {
            Ok(trace) => Some(trace),
            Err(e) => {
                tracing::error!("Corrupted tracing data ({e}) `{s}`");
                None
            }
        }
    } else {
        None
    }
}

/// The implementation of the profiler when the `profiler` feature is disabled.
#[cfg(not(feature = "profiler"))]
mod without_profiler {
    use std::cell::UnsafeCell;

    use crate::network::Coord;
    use crate::profiler::*;

    /// The fake profiler for when the `profiler` feature is disabled.
    // static PROFILER: UnsafeCell<NoOpProfiler> = UnsafeCell::new(NoOpProfiler);

    /// Fake profiler. This is used when the `profiler` feature is not enabled.
    ///
    /// This struct MUST NOT contain any field and must do absolutely nothing since it is accessed
    /// from a static reference.
    #[derive(Debug, Clone, Copy, Default)]
    pub struct NoOpProfiler;

    #[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
    pub struct ProfilerResult;

    thread_local! {
        static PROFILER: UnsafeCell<NoOpProfiler> = const { UnsafeCell::new(NoOpProfiler) };
    }

    impl Profiler for NoOpProfiler {
        #[inline(always)]
        fn items_in(&mut self, _from: Coord, _to: Coord, _amount: usize) {}
        #[inline(always)]
        fn items_out(&mut self, _from: Coord, _to: Coord, _amount: usize) {}
        #[inline(always)]
        fn net_bytes_in(&mut self, _from: Coord, _to: Coord, _amount: usize) {}
        #[inline(always)]
        fn net_bytes_out(&mut self, _from: Coord, _to: Coord, _amount: usize) {}
        #[inline(always)]
        fn iteration_boundary(&mut self, _leader_block_id: BlockId) {}
    }

    /// Get a fake profiler that does nothing.
    pub fn get_profiler() -> &'static mut NoOpProfiler {
        PROFILER.with(|t| unsafe { &mut *t.get() })
    }

    /// Do nothing, since there is nothing to wait for.
    pub fn wait_profiler() -> Vec<ProfilerResult> {
        Default::default()
    }
}

/// The implementation of the profiler when the `profiler` feature is enabled.
#[cfg(feature = "profiler")]
mod with_profiler {
    use once_cell::sync::Lazy;
    use std::cell::UnsafeCell;
    use std::time::Instant;

    use super::bucket_profiler::BucketProfiler;
    use flume::{Receiver, Sender};

    pub use super::bucket_profiler::ProfilerResult;

    /// The sender and receiver pair of the current profilers.
    ///
    /// These are options since they can be consumed.
    static CHANNEL: Lazy<(ProfilerSender, ProfilerReceiver)> = Lazy::new(|| flume::unbounded());

    /// The sender and receiver pair of the current profilers.
    ///
    /// These are options since they can be consumed.
    static START_TIME: Lazy<Instant> = Lazy::new(|| Instant::now());

    thread_local! {
        /// The actual profiler for the current thread, if the `profiler` feature is enabled.
        static PROFILER: UnsafeCell<BucketProfiler> = UnsafeCell::new(BucketProfiler::new(*START_TIME));
    }

    /// The type of the channel sender with the `ProfilerResult`s.
    type ProfilerSender = Sender<ProfilerResult>;
    /// The type of the channel receiver with the `ProfilerResult`s.
    type ProfilerReceiver = Receiver<ProfilerResult>;

    /// Get the sender for sending the profiler results.
    pub(crate) fn get_sender() -> ProfilerSender {
        CHANNEL.0.clone()
    }

    /// Get the current profiler.
    pub fn get_profiler() -> &'static mut BucketProfiler {
        PROFILER.with(|t| unsafe { &mut *t.get() })
    }

    /// Wait for all the threads that used the profiler to exit, collect all their data and reset
    /// the profiler.
    pub fn wait_profiler() -> Vec<ProfilerResult> {
        CHANNEL.1.drain().collect()
    }
}
