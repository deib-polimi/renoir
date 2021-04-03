use crate::operator::{
    Data, DataKey, StreamElement, Timestamp, Window, WindowDescription, WindowGenerator,
};

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct SlidingEventTimeWindow {
    size: Duration,
    step: Duration,
}

impl SlidingEventTimeWindow {
    pub fn new(size: Duration, step: Duration) -> Self {
        assert!(step <= size);
        assert_ne!(size, Duration::new(0, 0));
        assert_ne!(step, Duration::new(0, 0));
        Self { size, step }
    }
}

impl<Key: DataKey, Out: Data> WindowDescription<Key, Out> for SlidingEventTimeWindow {
    type Generator = EventTimeWindowGenerator<Key, Out>;

    fn new_generator(&self) -> Self::Generator {
        Self::Generator::new(self.size, self.step)
    }

    fn to_string(&self) -> String {
        format!(
            "SlidingEventTimeWindow[size={}, step={}]",
            self.size.as_secs_f64(),
            self.step.as_secs_f64()
        )
    }
}
#[derive(Clone, Debug)]
pub struct TumblingEventTimeWindow {
    size: Duration,
}

impl TumblingEventTimeWindow {
    pub fn new(size: Duration) -> Self {
        assert_ne!(size, Duration::new(0, 0));
        Self { size }
    }
}

impl<Key: DataKey, Out: Data> WindowDescription<Key, Out> for TumblingEventTimeWindow {
    type Generator = EventTimeWindowGenerator<Key, Out>;

    fn new_generator(&self) -> Self::Generator {
        Self::Generator::new(self.size, self.size)
    }

    fn to_string(&self) -> String {
        format!("TumblingEventTimeWindow[size={}]", self.size.as_secs_f64(),)
    }
}

#[derive(Clone)]
pub struct EventTimeWindowGenerator<Key: DataKey, Out: Data> {
    size: Duration,
    step: Duration,
    win_end: Timestamp,
    items: VecDeque<Out>,
    timestamps: VecDeque<Timestamp>,
    last_seen: Timestamp,
    _key: PhantomData<Key>,
}

impl<Key: DataKey, Out: Data> EventTimeWindowGenerator<Key, Out> {
    fn new(size: Duration, step: Duration) -> Self {
        EventTimeWindowGenerator {
            size,
            step,
            win_end: Default::default(),
            items: Default::default(),
            timestamps: Default::default(),
            last_seen: Default::default(),
            _key: Default::default(),
        }
    }
}

impl<Key: DataKey, Out: Data> EventTimeWindowGenerator<Key, Out> {
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

impl<Key: DataKey, Out: Data> WindowGenerator<Key, Out> for EventTimeWindowGenerator<Key, Out> {
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

#[cfg(test)]
mod tests {
    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::window::SlidingEventTimeWindow;
    use crate::operator::{source, Timestamp, TumblingEventTimeWindow};
    use std::time::Duration;

    #[test]
    fn sliding_event_time() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::EventTimeIteratorSource::new(
            (0..10).map(|x| (x, Timestamp::from_secs(x))),
            |x, ts| {
                if x % 2 == 1 {
                    Some(*ts)
                } else {
                    None
                }
            },
        );

        let res = env
            .stream(source)
            .group_by(|x| x % 2)
            .window(SlidingEventTimeWindow::new(
                Duration::from_secs(3),
                Duration::from_millis(2500),
            ))
            .first()
            .unkey()
            .map(|(_, x)| x)
            .collect_vec();
        env.execute();

        let mut res = res.get().unwrap();
        // Windows and elements
        // 0.0 -> 3.0  [0, 2] and [1]
        // 2.5 -> 5.5  [4] and [3, 5]
        // 5.0 -> 8.0  [6] and [5, 7]
        // 7.5 -> 10.5 [8] and [9]
        res.sort_unstable();
        assert_eq!(res, vec![0, 1, 3, 4, 5, 6, 8, 9]);
    }

    #[test]
    fn tumbling_event_time() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::EventTimeIteratorSource::new(
            (0..10).map(|x| (x, Timestamp::from_secs(x))),
            |x, ts| {
                if x % 2 == 1 {
                    Some(*ts)
                } else {
                    None
                }
            },
        );

        let res = env
            .stream(source)
            .group_by(|x| x % 2)
            .window(TumblingEventTimeWindow::new(Duration::from_secs(3)))
            .first()
            .unkey()
            .map(|(_, x)| x)
            .collect_vec();
        env.execute();

        let mut res = res.get().unwrap();
        // Windows and elements
        // 0.0 -> 3.0  [0, 2] and [1]
        // 3.0 -> 6.0  [4] and [3, 5]
        // 6.0 -> 9.0  [6, 8] and [7]
        // 9.0 -> 12.0 [] and [9]
        res.sort_unstable();
        assert_eq!(res, vec![0, 1, 3, 4, 6, 7, 9]);
    }
}
