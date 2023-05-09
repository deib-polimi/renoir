use indexmap::IndexMap;

use crate::block::CoordHasherBuilder;
use crate::network::Coord;
use crate::operator::Timestamp;

/// Handle watermarks coming from multiple replicas.
///
/// A watermark with timestamp `ts` is safe to be passed downstream if and only if, for every
/// previous replica, a watermark with timestamp greater or equal to `ts` has already been received.
#[derive(Clone, Debug, Default)]
pub(super) struct WatermarkFrontier {
    map: IndexMap<Coord, Option<Timestamp>, CoordHasherBuilder>,
    front: Option<Timestamp>,
}

fn opt_join<T: std::cmp::Ord>(a: Option<T>, b: Option<T>, f: fn(T, T) -> T) -> Option<T> {
    match (a, b) {
        (Some(a), Some(b)) => Some(f(a, b)),
        (a, None) | (None, a) => a,
    }
}

impl WatermarkFrontier {
    pub fn new(prev_replicas: impl IntoIterator<Item = Coord>) -> Self {
        Self {
            map: prev_replicas.into_iter().map(|c| (c, None)).collect(),
            front: None,
        }
    }

    fn compute_frontier(&self) -> Option<Timestamp> {
        let (complete, min) = self.map.values().fold((true, None), |(all, min), x| {
            (all & x.is_some(), opt_join(min, *x, std::cmp::min))
        });

        if complete {
            min
        } else {
            None
        }
    }

    /// Update the frontier, return `Some(ts)` if timestamp `ts` is now safe
    pub fn update(&mut self, coord: Coord, ts: Timestamp) -> Option<Timestamp> {
        let t0 = &mut self.map[&coord];
        if matches!(t0, Some(t) if *t >= ts) {
            // Early break for old watermark
            return None;
        }
        *t0 = Some(ts);

        // get new frontier
        let prev_frontier = self.front;
        self.front = self.compute_frontier();

        match (prev_frontier, self.front) {
            (None, Some(new)) => Some(new),
            (Some(old), Some(new)) if old != new => Some(new),
            _ => None,
        }
    }

    /// Reset all the watermarks.
    pub fn reset(&mut self) {
        self.map.values_mut().for_each(|v| *v = None);
        self.front = None;
    }
}
