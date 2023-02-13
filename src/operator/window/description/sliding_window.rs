use std::collections::VecDeque;
use std::marker::PhantomData;
use std::time::Duration;

use crate::operator::window::processing_time::ProcessingTimeWindowGenerator;
use crate::operator::window::{Window, WindowDescription, WindowGenerator};
use crate::operator::{Data, DataKey, StreamElement, Timestamp};

/// Window description for sliding event time windows
#[derive(Clone, Debug)]
pub struct SlidingEventTimeWindowDescr {
    size: Timestamp,
    step: Timestamp,
}

impl SlidingEventTimeWindowDescr {
    /// Create a new sliding window.
    ///
    /// Each window has the given size, and will slide with the given steps. The first window is
    /// aligned with the epoch time.
    pub fn new(size: Timestamp, step: Timestamp) -> Self {
        assert!(step <= size);
        assert_ne!(size, 0);
        assert_ne!(step, 0);
        Self { size, step }
    }
}

impl<Key: DataKey, Out: Data> WindowDescription<Key, Out> for SlidingEventTimeWindowDescr {
    type Generator = SlidingWindowGenerator<Key, Out>;

    fn new_generator(&self) -> Self::Generator {
        Self::Generator::new(self.size, self.step)
    }

    fn to_string(&self) -> String {
        format!(
            "SlidingEventTimeWindow[event, size={}, step={}]",
            self.size, self.step
        )
    }
}

/// Window description for sliding processing time windows
#[derive(Clone, Debug)]
pub struct SlidingProcessingTimeWindowDescr {
    size: Timestamp,
    step: Timestamp,
}

impl SlidingProcessingTimeWindowDescr {
    pub(crate) fn new(size: Duration, step: Duration) -> Self {
        let size = size
            .as_nanos()
            .try_into()
            .expect("Timestamp out of range. File an issue!");
        let step = step
            .as_nanos()
            .try_into()
            .expect("Timestamp out of range. File an issue!");
        assert!(step <= size);
        assert_ne!(size, 0);
        assert_ne!(step, 0);
        Self { size, step }
    }
}

impl<Key: DataKey, Out: Data> WindowDescription<Key, Out> for SlidingProcessingTimeWindowDescr {
    type Generator = ProcessingTimeWindowGenerator<Key, Out, SlidingWindowGenerator<Key, Out>>;

    fn new_generator(&self) -> Self::Generator {
        Self::Generator::new(SlidingWindowGenerator::new(self.size, self.step))
    }

    fn to_string(&self) -> String {
        format!(
            "SlidingProcessingTimeWindow[processing, size={}, step={}]",
            self.size, self.step
        )
    }
}

/// This generator is used for event time and processing time windows.
#[derive(Clone)]
pub struct SlidingWindowGenerator<Key: DataKey, Out: Data> {
    size: Timestamp,
    step: Timestamp,
    win_end: Timestamp,
    items: VecDeque<Out>,
    timestamps: VecDeque<Timestamp>,
    last_seen: Timestamp,
    _key: PhantomData<Key>,
}

impl<Key: DataKey, Out: Data> SlidingWindowGenerator<Key, Out> {
    pub(crate) fn new(size: Timestamp, step: Timestamp) -> Self {
        SlidingWindowGenerator {
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
    fn get_window_end(&self, t: Timestamp) -> Timestamp {
        if t < self.size {
            // the given timestamp is part of the first window
            return self.size;
        }

        let offset = self.size % self.step;
        let delta = (t - offset) / self.step * self.step + offset;
        delta + self.step
    }
}

impl<Key: DataKey, Out: Data> WindowGenerator<Key, Out> for SlidingWindowGenerator<Key, Out> {
    fn add(&mut self, item: StreamElement<Out>) {
        match item {
            StreamElement::Item(_) => {
                panic!("Event time window cannot handle elements without a timestamp")
            }
            StreamElement::Timestamped(v, ts) => {
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
            StreamElement::FlushAndRestart => {
                self.last_seen = Timestamp::MAX;
            }
            StreamElement::FlushBatch => unreachable!("Windows do not handle FlushBatch"),
            StreamElement::Terminate => unreachable!("Windows do not handle Terminate"),
        }
    }

    fn next_window(&mut self) -> Option<Window<Out>> {
        if !self.items.is_empty() && self.win_end <= self.last_seen {
            let size = self
                .timestamps
                .iter()
                .take_while(|&ts| ts < &self.win_end)
                .count();
            let timestamp = Some(self.win_end);

            Some(Window {
                idx: 0,
                buffer: &self.items,
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
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use itertools::Itertools;

    use crate::operator::window::description::sliding_window::SlidingWindowGenerator;
    use crate::operator::window::{EventTimeWindow, WindowDescription, WindowGenerator};
    use crate::operator::StreamElement;

    #[test]
    fn sliding_event_time() {
        let descr = EventTimeWindow::sliding(
            Duration::from_secs(3).as_millis() as i64,
            Duration::from_millis(2500).as_millis() as i64,
        );
        let mut generator: SlidingWindowGenerator<u32, _> = descr.new_generator();

        // current window [0, 3)
        generator.add(StreamElement::Timestamped(0, 0_000));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Timestamped(1, 1_000));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Timestamped(2, 2_000));
        assert!(generator.next_window().is_none());
        // this closes the window
        generator.add(StreamElement::Timestamped(3, 3_000));

        let window = generator.next_window().unwrap();
        assert_eq!(window.timestamp, Some(3_000));
        let items = window.copied().collect_vec();
        assert_eq!(items, vec![0, 1, 2]);
        generator.advance();

        // current window [2.5, 5.5)
        generator.add(StreamElement::Watermark(5_000));
        assert!(generator.next_window().is_none());
        // this closes the window
        generator.add(StreamElement::Watermark(6_000));

        let window = generator.next_window().unwrap();
        assert_eq!(window.timestamp, Some(5_500));
        let items = window.copied().collect_vec();
        assert_eq!(items, vec![3]);
        generator.advance();

        // current window [7.5, 10.5)
        generator.add(StreamElement::Timestamped(10, 10_000));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::FlushAndRestart);

        let window = generator.next_window().unwrap();
        assert_eq!(window.timestamp, Some(10_500));
        let items = window.copied().collect_vec();
        assert_eq!(items, vec![10]);
        generator.advance();

        // current window [10, 13)
        let window = generator.next_window().unwrap();
        assert_eq!(window.timestamp, Some(13_000));
        let items = window.copied().collect_vec();
        assert_eq!(items, vec![10]);
        generator.advance();

        assert!(generator.next_window().is_none());
    }
}
