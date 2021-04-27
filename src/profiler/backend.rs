use std::time::Instant;

use crate::channel::UnboundedChannelSender;
use crate::network::Coord;
use crate::profiler::metrics::{ProfilerBucket, ProfilerResult};
use crate::profiler::{get_sender, Profiler, TimePoint};

/// The size of a bucket, in milliseconds.
///
/// Each bucket will contain events for up to this amount of time.
const BUCKET_RESOLUTION_MS: TimePoint = 20;

/// A thread-local implementation of a profiler.
///
/// This will collect the events and store them inside buckets of size `BUCKET_RESOLUTION_MS`. All
/// the events inside a bucket are merged together.
#[derive(Clone, Debug)]
pub struct ProfilerBackend {
    /// The name of the current thread.
    thread_name: String,
    /// The start of time: all the times will be relative to this instant, ie the start of the
    /// execution.
    start: Instant,
    /// The list of all the buckets, sorted by their start time.
    buckets: Vec<ProfilerBucket>,
    /// The sender to use to send the profiler results back to the main thread.
    sender: UnboundedChannelSender<ProfilerResult>,
}

impl ProfilerBackend {
    pub fn new(start: Instant) -> Self {
        Self {
            thread_name: std::thread::current()
                .name()
                .unwrap_or("unnamed")
                .to_string(),
            start,
            buckets: vec![ProfilerBucket::new(0)],
            sender: get_sender(),
        }
    }

    /// Get the current time relative to the start of the execution.
    fn now(&self) -> TimePoint {
        self.start.elapsed().as_millis() as TimePoint
    }

    /// Get a reference to the current bucket, creating a new one if needed.
    #[inline]
    fn bucket(&mut self) -> &mut ProfilerBucket {
        let now = self.now();
        // the timestamp is outside the last bucket, create a new one
        if now >= self.buckets.last().unwrap().start_ms + BUCKET_RESOLUTION_MS {
            let start = now - now % BUCKET_RESOLUTION_MS;
            self.buckets.push(ProfilerBucket::new(start));
        }
        self.buckets.last_mut().unwrap()
    }
}

impl Drop for ProfilerBackend {
    fn drop(&mut self) {
        self.sender
            .send(ProfilerResult {
                thread_name: std::mem::take(&mut self.thread_name),
                buckets: std::mem::take(&mut self.buckets),
            })
            .unwrap();
    }
}

impl Profiler for ProfilerBackend {
    #[inline]
    fn items_in(&mut self, from: Coord, to: Coord, amount: usize) {
        let entry = self.bucket().metrics.items_in.entry((from, to));
        *entry.or_default() += amount;
    }

    #[inline]
    fn items_out(&mut self, from: Coord, to: Coord, amount: usize) {
        let entry = self.bucket().metrics.items_out.entry((from, to));
        *entry.or_default() += amount;
    }

    #[inline]
    fn net_bytes_in(&mut self, from: Coord, to: Coord, amount: usize) {
        let entry = self.bucket().metrics.net_messages_in.entry((from, to));
        let entry = entry.or_default();
        entry.0 += 1;
        entry.1 += amount;
    }

    #[inline]
    fn net_bytes_out(&mut self, from: Coord, to: Coord, amount: usize) {
        let entry = self.bucket().metrics.net_messages_out.entry((from, to));
        let entry = entry.or_default();
        entry.0 += 1;
        entry.1 += amount;
    }

    #[inline]
    fn iteration_boundary(&mut self, leader_block_id: usize) {
        let now = self.now();
        self.bucket()
            .metrics
            .iteration_boundaries
            .push((leader_block_id, now))
    }
}
