use std::time::{Duration, Instant};

pub use metrics::*;
#[cfg(feature = "profiler")]
pub use with_profiler::*;
#[cfg(not(feature = "profiler"))]
pub use without_profiler::*;

use crate::network::Coord;
use crate::stream::BlockId;

#[cfg(feature = "profiler")]
mod backend;
mod metrics;

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

/// Measure the time from the creating till the drop, and print the elapsed ns to the stderr.
pub struct Stopwatch {
    /// The name of the stopwatch.
    name: String,
    /// The instant of the start of the stopwatch.
    start: Instant,
}

impl Stopwatch {
    pub fn new<S: Into<String>>(name: S) -> Self {
        Self {
            name: name.into(),
            start: Instant::now(),
        }
    }

    /// Print a timing information in a recognizable format.
    pub fn print(name: &str, elapsed: Duration) {
        eprintln!("timens:{}:{}", name, elapsed.as_nanos())
    }
}

impl Drop for Stopwatch {
    fn drop(&mut self) {
        let dur = self.start.elapsed();
        Self::print(&self.name, dur);
    }
}

/// The implementation of the profiler when the `profiler` feature is enabled.
#[cfg(feature = "profiler")]
mod with_profiler {
    use std::cell::UnsafeCell;
    use std::sync::Mutex;
    use std::time::Instant;

    use crate::channel::{UnboundedChannelReceiver, UnboundedChannelSender};
    use crate::profiler::backend::ProfilerBackend;
    use crate::profiler::metrics::ProfilerResult;

    lazy_static! {
        /// The sender and receiver pair of the current profilers.
        ///
        /// These are options since they can be consumed.
        static ref CHANNEL: Mutex<(Option<ProfilerSender>, Option<ProfilerReceiver>)> = {
            let (sender, receiver) = ProfilerReceiver::new();
            Mutex::new((Some(sender), Some(receiver)))
        };
    }

    lazy_static! {
        /// The sender and receiver pair of the current profilers.
        ///
        /// These are options since they can be consumed.
        static ref START_TIME: Instant = Instant::now();
    }

    thread_local! {
        /// The actual profiler for the current thread, if the `profiler` feature is enabled.
        static PROFILER: UnsafeCell<ProfilerBackend> = UnsafeCell::new(ProfilerBackend::new(*START_TIME));
    }

    /// The type of the channel sender with the `ProfilerResult`s.
    type ProfilerSender = UnboundedChannelSender<ProfilerResult>;
    /// The type of the channel receiver with the `ProfilerResult`s.
    type ProfilerReceiver = UnboundedChannelReceiver<ProfilerResult>;

    /// Get the sender for sending the profiler results.
    pub(crate) fn get_sender() -> ProfilerSender {
        let channels = CHANNEL.lock().unwrap();
        channels.0.clone().expect("Profiler sender already dropped")
    }

    /// Get the current profiler.
    pub fn get_profiler() -> &'static mut ProfilerBackend {
        PROFILER.with(|t| unsafe { &mut *t.get() })
    }

    /// Wait for all the threads that used the profiler to exit, collect all their data and reset
    /// the profiler.
    pub fn wait_profiler() -> Vec<ProfilerResult> {
        let mut channels = CHANNEL.lock().unwrap();
        let profiler_receiver = channels.1.take().expect("Profiler receiver already taken");

        // allow the following loop to exit when all the senders are dropped
        channels.0.take().expect("Profiler sender already dropped");

        let mut results = vec![];
        while let Ok(profiler_res) = profiler_receiver.recv() {
            results.push(profiler_res);
        }

        let (sender, receiver) = ProfilerReceiver::new();
        channels.0 = Some(sender);
        channels.1 = Some(receiver);

        results
    }
}

/// The implementation of the profiler when the `profiler` feature is disabled.
#[cfg(not(feature = "profiler"))]
mod without_profiler {
    use crate::network::Coord;
    use crate::profiler::*;

    /// The fake profiler for when the `profiler` feature is disabled.
    static mut PROFILER: NoOpProfiler = NoOpProfiler;

    /// Fake profiler. This is used when the `profiler` feature is not enabled.
    ///
    /// This struct MUST NOT contain any field and must do absolutely nothing since it is accessed
    /// from a static reference.
    #[derive(Debug, Clone, Copy, Default)]
    pub struct NoOpProfiler;

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
        fn iteration_boundary(&mut self, _leader_block_id: usize) {}
    }

    /// Get a fake profiler that does nothing.
    pub fn get_profiler() -> &'static mut NoOpProfiler {
        // SAFETY: the profiler does nothing, so, even though this is a mutable borrow, no data is
        //         shared.
        unsafe { &mut PROFILER }
    }

    /// Do nothing, since there is nothing to wait for.
    pub fn wait_profiler() -> Vec<ProfilerResult> {
        Default::default()
    }
}
