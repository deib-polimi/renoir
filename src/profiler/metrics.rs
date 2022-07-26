use std::collections::HashMap;

use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::network::Coord;
use crate::scheduler::BlockId;

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
    pub buckets: Vec<ProfilerBucket>,
}

/// The available metrics to be collected.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProfilerMetrics {
    /// The number of items that arrived to a block.
    #[serde(serialize_with = "serialize_map", deserialize_with = "deserialize_map")]
    pub items_in: HashMap<(Coord, Coord), usize>,
    /// The number of items that left from a block.
    #[serde(serialize_with = "serialize_map", deserialize_with = "deserialize_map")]
    pub items_out: HashMap<(Coord, Coord), usize>,

    /// The number of network messages that arrived to a block and their total size.
    #[serde(serialize_with = "serialize_map", deserialize_with = "deserialize_map")]
    pub net_messages_in: HashMap<(Coord, Coord), (usize, usize)>,
    /// The number of network messages that left from a block and their total size.
    #[serde(serialize_with = "serialize_map", deserialize_with = "deserialize_map")]
    pub net_messages_out: HashMap<(Coord, Coord), (usize, usize)>,

    /// The time point of the end of an iteration, with the id of the leader block that manages that
    /// iteration.
    pub iteration_boundaries: Vec<(BlockId, TimePoint)>,
}

/// A bucket with the profiler metrics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfilerBucket {
    /// The time point of the start of the bucket.
    ///
    /// This bucket should contain the metrics in the time interval `[start, start+RESOLUTION)`.
    pub start_ms: TimePoint,
    /// The metrics of this bucket.
    pub metrics: ProfilerMetrics,
}

impl ProfilerBucket {
    #[inline]
    #[allow(dead_code)]
    pub fn new(start_ms: TimePoint) -> Self {
        Self {
            start_ms,
            metrics: Default::default(),
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
fn serialize_map<S: Serializer, T: Serialize + Copy>(
    map: &HashMap<(Coord, Coord), T>,
    s: S,
) -> Result<S::Ok, S::Error> {
    let mut seq = s.serialize_seq(Some(map.len()))?;
    for (&(from, to), &value) in map.iter() {
        let entry = Entry { from, to, value };
        seq.serialize_element(&entry)?;
    }
    seq.end()
}

/// The inverse of `serialize_map`.
fn deserialize_map<'de, D, T>(d: D) -> Result<HashMap<(Coord, Coord), T>, D::Error>
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
