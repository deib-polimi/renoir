use itertools::Itertools;

use crate::network::Coord;
use crate::operator::Timestamp;

/// Handle watermarks coming from multiple replicas.
///
/// A watermark with timestamp `ts` is safe to be passed downstream if and only if, for every
/// previous replica, a watermark with timestamp greater or equal to `ts` has already been received.
#[derive(Clone, Debug, Default)]
pub(super) struct WatermarkFrontier {
    /// Watermark with largest timestamp received, for each replica
    largest_watermark: Vec<Option<Timestamp>>,
    /// List of previous replicas
    prev_replicas: Vec<Coord>,
}

impl WatermarkFrontier {
    pub fn new(prev_replicas: Vec<Coord>) -> Self {
        Self {
            largest_watermark: vec![None; prev_replicas.len()],
            prev_replicas,
        }
    }

    /// Return the current maximum safe timestamp
    pub fn get_frontier(&self) -> Option<Timestamp> {
        // if at least one replica has not sent a watermark yet, return None
        let missing_watermarks = self.largest_watermark.iter().any(|x| x.is_none());
        if missing_watermarks {
            None
        } else {
            // the current frontier is the minimum watermark received
            Some(
                self.largest_watermark
                    .iter()
                    .map(|x| x.unwrap())
                    .min()
                    .unwrap(),
            )
        }
    }

    /// Update the frontier, return `Some(ts)` if timestamp `ts` is now safe
    pub fn update(&mut self, coord: Coord, ts: Timestamp) -> Option<Timestamp> {
        let old_frontier = self.get_frontier();

        // find position of the replica in the list of replicas
        let replica_idx = self
            .prev_replicas
            .iter()
            .find_position(|&&prev| prev == coord)
            .unwrap()
            .0;

        // update watermark of the replica
        self.largest_watermark[replica_idx] =
            Some(self.largest_watermark[replica_idx].map_or(ts, |w| w.max(ts)));

        // get new frontier
        match self.get_frontier() {
            Some(new_frontier) => match old_frontier {
                // if the old frontier is equal to the current one,
                // the watermark has already been sent
                Some(old_frontier) if new_frontier == old_frontier => None,
                // the frontier has been updated, send the watermark downstream
                _ => Some(new_frontier),
            },
            None => None,
        }
    }

    /// Reset all the watermarks.
    pub fn reset(&mut self) {
        for w in self.largest_watermark.iter_mut() {
            *w = None;
        }
    }
}
