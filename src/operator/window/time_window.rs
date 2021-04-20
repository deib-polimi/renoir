use crate::operator::{Data, DataKey, StreamElement, Timestamp, Window, WindowGenerator};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::time::Duration;

/// This generator is used for event time and processing time windows.
#[derive(Clone)]
pub struct TimeWindowGenerator<Key: DataKey, Out: Data> {
    size: Duration,
    step: Duration,
    win_end: Timestamp,
    items: VecDeque<Out>,
    timestamps: VecDeque<Timestamp>,
    last_seen: Timestamp,
    _key: PhantomData<Key>,
}

impl<Key: DataKey, Out: Data> TimeWindowGenerator<Key, Out> {
    pub(crate) fn new(size: Duration, step: Duration) -> Self {
        TimeWindowGenerator {
            size,
            step,
            win_end: Default::default(),
            items: Default::default(),
            timestamps: Default::default(),
            last_seen: Default::default(),
            _key: Default::default(),
        }
    }

    /// Return the closing time of the oldest window containing `t`, making
    /// sure the first window starts at time 0.
    fn get_window_end(&self, t: Timestamp) -> Duration {
        if t < self.size {
            // the given timestamp is part of the first window
            return self.size;
        }

        let offset = self.size.as_nanos() % self.step.as_nanos();
        let nanos = (t.as_nanos() - offset) / self.step.as_nanos() * self.step.as_nanos() + offset;
        let dur = Duration::new(
            (nanos / 1_000_000_000) as u64,
            (nanos % 1_000_000_000) as u32,
        );
        dur + self.step
    }
}

impl<Key: DataKey, Out: Data> WindowGenerator<Key, Out> for TimeWindowGenerator<Key, Out> {
    fn add(&mut self, item: StreamElement<(Key, Out)>) {
        match item {
            StreamElement::Item(_) => {
                panic!("Event time window cannot handle elements without a timestamp")
            }
            StreamElement::Timestamped((_, v), ts) => {
                assert!(ts >= self.last_seen);
                self.last_seen = ts;

                self.items.push_back(v);
                self.timestamps.push_back(ts);

                if self.timestamps.len() == 1 {
                    // if the buffers were previously empty, recompute the end of the window
                    self.win_end = self
                        .get_window_end(*self.timestamps.front().unwrap())
                        .max(self.win_end);
                }
            }
            StreamElement::Watermark(ts) => {
                assert!(ts >= self.last_seen);
                self.last_seen = ts;
            }
            StreamElement::FlushBatch => unreachable!("Windows do not handle FlushBatch"),
            StreamElement::End => {
                self.last_seen = Timestamp::new(u64::MAX, 0);
            }
            StreamElement::IterEnd => {
                unimplemented!("Time windows are not yet supported inside an iteration (and probably never will)")
            }
        }
    }

    fn next_window(&mut self) -> Option<Window<Key, Out>> {
        if !self.items.is_empty() && self.win_end <= self.last_seen {
            let size = self
                .timestamps
                .iter()
                .take_while(|&ts| ts < &self.win_end)
                .count();
            let timestamp = Some(self.win_end);

            Some(Window {
                gen: self,
                size,
                timestamp,
            })
        } else {
            None
        }
    }

    fn advance(&mut self) {
        let next_win_start = self.win_end - self.size + self.step;
        while let Some(ts) = self.timestamps.front() {
            if ts < &next_win_start {
                // pop the item if it is not contained in the next window
                self.items.pop_front().unwrap();
                self.timestamps.pop_front().unwrap();
            } else {
                break;
            }
        }

        // move to the next window
        self.win_end += self.step;
        if let Some(&ts) = self.timestamps.front() {
            // make sure to skip empty windows
            self.win_end = self.get_window_end(ts).max(self.win_end);
        }
    }

    fn buffer(&self) -> &VecDeque<Out> {
        &self.items
    }
}
