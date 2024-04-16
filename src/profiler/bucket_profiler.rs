use std::time::Instant;

use crate::network::Coord;
use crate::scheduler::BlockId;
use flume::Sender;
use std::collections::HashMap;

use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::block::CoordHasherBuilder;

use super::{get_sender, Profiler};

/// The size of a bucket, in milliseconds.
///
/// Each bucket will contain events for up to this amount of time.
const BUCKET_RESOLUTION_MS: TimePoint = 50;

/// A thread-local implementation of a profiler.
///
/// This will collect the events and store them inside buckets of size `BUCKET_RESOLUTION_MS`. All
/// the events inside a bucket are merged together.
#[derive(Clone, Debug)]
pub struct BucketProfiler {
    /// The name of the current thread.
    thread_name: String,
    /// The start of time: all the times will be relative to this instant, ie the start of the
    /// execution.
    start: Instant,
    /// The list of all the buckets, sorted by their start time.
    buckets: Vec<MetricsBucket>,
    /// The sender to use to send the profiler results back to the main thread.
    sender: Sender<ProfilerResult>,
}

impl BucketProfiler {
    pub fn new(start: Instant) -> Self {
        Self {
            thread_name: std::thread::current()
                .name()
                .unwrap_or("unnamed")
                .to_string(),
            start,
            buckets: vec![MetricsBucket::new(0)],
            sender: get_sender(),
        }
    }

    /// Get the current time relative to the start of the execution.
    fn now(&self) -> TimePoint {
        self.start.elapsed().as_millis() as TimePoint
    }

    /// Get a reference to the current bucket, creating a new one if needed.
    #[inline]
    fn bucket(&mut self) -> &mut MetricsBucket {
        let now = self.now();
        // the timestamp is outside the last bucket, create a new one
        if now >= self.buckets.last().unwrap().start_ms + BUCKET_RESOLUTION_MS {
            let start = now - now % BUCKET_RESOLUTION_MS;
            self.buckets.push(MetricsBucket::new(start));
        }
        self.buckets.last_mut().unwrap()
    }
}

impl Drop for BucketProfiler {
    fn drop(&mut self) {
        self.sender
            .send(ProfilerResult {
                thread_name: std::mem::take(&mut self.thread_name),
                buckets: std::mem::take(&mut self.buckets),
            })
            .unwrap();
    }
}

impl Profiler for BucketProfiler {
    #[inline]
    fn items_in(&mut self, from: Coord, to: Coord, amount: usize) {
        let entry = self.bucket().link_metrics.entry((from, to)).or_default();
        entry.items_in += amount;
    }

    #[inline]
    fn items_out(&mut self, from: Coord, to: Coord, amount: usize) {
        let entry = self.bucket().link_metrics.entry((from, to)).or_default();
        entry.items_out += amount;
    }

    #[inline]
    fn net_bytes_in(&mut self, from: Coord, to: Coord, amount: usize) {
        let entry = self.bucket().link_metrics.entry((from, to)).or_default();
        entry.net_messages_in += 1;
        entry.bytes_in += amount;
    }

    #[inline]
    fn net_bytes_out(&mut self, from: Coord, to: Coord, amount: usize) {
        let entry = self.bucket().link_metrics.entry((from, to)).or_default();
        entry.net_messages_out += 1;
        entry.bytes_out += amount;
    }

    #[inline]
    fn iteration_boundary(&mut self, leader_block_id: BlockId) {
        let now = self.now();
        self.bucket().iteration_metrics.push((leader_block_id, now))
    }
}

/// A time point.
///
/// This represents the number of milliseconds since the start of the execution.
pub type TimePoint = u32;

/// The results of the profiler of a thread.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfilerResult {
    /// The name of the thread that collected the data.
    pub thread_name: String,
    /// The list of collected buckets.
    pub buckets: Vec<MetricsBucket>,
}

/// The available metrics to be collected.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LinkMetrics {
    /// The number of items that arrived to a block.
    pub items_in: usize,
    /// The number of items that left from a block.
    pub items_out: usize,

    /// The number of network messages that arrived to a block and their total size.
    pub net_messages_in: usize,
    /// The number of network messages that left from a block and their total size.
    pub net_messages_out: usize,

    pub bytes_in: usize,
    pub bytes_out: usize,
}

/// A bucket with the profiler metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetricsBucket {
    /// The time point of the start of the bucket.
    ///
    /// This bucket should contain the metrics in the time interval `[start, start+RESOLUTION)`.
    pub start_ms: TimePoint,
    /// The metrics of this bucket.
    #[serde(serialize_with = "serialize_map", deserialize_with = "deserialize_map")]
    pub link_metrics: HashMap<(Coord, Coord), LinkMetrics, CoordHasherBuilder>,

    /// The time point of the end of an iteration, with the id of the leader block that manages that
    /// iteration.
    pub iteration_metrics: Vec<(BlockId, TimePoint)>,
}

impl MetricsBucket {
    #[inline]
    pub fn new(start_ms: TimePoint) -> Self {
        Self {
            start_ms,
            ..Default::default()
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Entry<T> {
    from: Coord,
    to: Coord,
    value: T,
}

/// Since JSON supports only maps with strings as key, this serialized _flattens_ those maps.
fn serialize_map<S: Serializer, T: Serialize>(
    map: &HashMap<(Coord, Coord), T, CoordHasherBuilder>,
    s: S,
) -> Result<S::Ok, S::Error> {
    let mut seq = s.serialize_seq(Some(map.len()))?;
    for (&(from, to), value) in map.iter() {
        let entry = Entry { from, to, value };
        seq.serialize_element(&entry)?;
    }
    seq.end()
}

/// The inverse of `serialize_map`.
fn deserialize_map<'de, D, T>(
    d: D,
) -> Result<HashMap<(Coord, Coord), T, CoordHasherBuilder>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    let as_vec: Vec<Entry<T>> = serde::de::Deserialize::deserialize(d)?;
    Ok(as_vec
        .into_iter()
        .map(|e| ((e.from, e.to), e.value))
        .collect())
}
