use std::collections::VecDeque;
use std::marker::PhantomData;
use std::time::Duration;

use crate::operator::window::{Window, WindowDescription, WindowGenerator};
use crate::operator::{Data, DataKey, StreamElement, Timestamp};

#[derive(Clone, Debug)]
pub struct SessionWindow {
    gap: Duration,
}

impl SessionWindow {
    pub fn with_gap(gap: Duration) -> Self {
        Self { gap }
    }
}

impl<Key: DataKey, Out: Data> WindowDescription<Key, Out> for SessionWindow {
    type Generator = SessionWindowGenerator<Key, Out>;

    fn new_generator(&self) -> Self::Generator {
        SessionWindowGenerator::new(self.clone())
    }

    fn to_string(&self) -> String {
        format!("SessionWindow[gap={:?}]", self.gap)
    }
}

#[derive(Clone, Debug)]
pub struct SessionWindowGenerator<Key: DataKey, Out: Data> {
    descr: SessionWindow,
    buffer: VecDeque<Out>,
    timestamp_buffer: VecDeque<Timestamp>,
    received_end: bool,
    last_seen: Timestamp,
    last_window_size: usize,
    _key: PhantomData<Key>,
}

impl<Key: DataKey, Out: Data> SessionWindowGenerator<Key, Out> {
    fn new(descr: SessionWindow) -> Self {
        Self {
            descr,
            buffer: Default::default(),
            timestamp_buffer: Default::default(),
            received_end: false,
            last_seen: Default::default(),
            last_window_size: 0,
            _key: Default::default(),
        }
    }
}

impl<Key: DataKey, Out: Data> WindowGenerator<Key, Out> for SessionWindowGenerator<Key, Out> {
    fn add(&mut self, item: StreamElement<Out>) {
        match item {
            StreamElement::Item(_) => {
                panic!("Session window cannot handle elements without a timestamp")
            }
            StreamElement::Timestamped(item, ts) => {
                assert!(ts >= self.last_seen);
                self.buffer.push_back(item);
                self.timestamp_buffer.push_back(ts);
                self.last_seen = ts;
            }
            StreamElement::Watermark(ts) => {
                assert!(ts >= self.last_seen);
                self.last_seen = ts;
            }
            StreamElement::FlushAndRestart => {
                self.received_end = true;
            }
            StreamElement::FlushBatch => unreachable!("Windows do not handle FlushBatch"),
            StreamElement::Terminate => unreachable!("Windows do not handle Terminate"),
        }
    }

    fn next_window(&mut self) -> Option<Window<Key, Out>> {
        // find all gaps between consecutive elements and find out if at least one of them is larger
        // than the gap chosen by the user
        let mut size = self
            .timestamp_buffer
            .iter()
            .zip(
                self.timestamp_buffer
                    .iter()
                    .skip(1)
                    .chain(&Some(self.last_seen)),
            )
            .map(|(ts1, ts2)| *ts2 - *ts1)
            .position(|gap| gap >= self.descr.gap)
            .map(|pos| pos + 1);

        // Even if there is no gap big enough we need to flush if a FlushAndRestart was received
        if size.is_none() && self.received_end && !self.buffer.is_empty() {
            size = Some(self.buffer.len());
        }

        match size {
            Some(size) => {
                self.last_window_size = size;
                let timestamp = self.timestamp_buffer.iter().take(size).max().cloned();
                Some(Window {
                    size,
                    gen: self,
                    timestamp,
                })
            }
            None => None,
        }
    }

    fn advance(&mut self) {
        for _ in 0..self.last_window_size {
            self.buffer.pop_front();
            self.timestamp_buffer.pop_front();
        }
    }

    fn buffer(&self) -> &VecDeque<Out> {
        &self.buffer
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::operator::{
        SessionWindow, SessionWindowGenerator, StreamElement, WindowDescription, WindowGenerator,
    };

    #[test]
    fn session_window_watermark() {
        let descr = SessionWindow::with_gap(Duration::from_secs(10));
        let mut generator: SessionWindowGenerator<u32, _> = descr.new_generator();

        generator.add(StreamElement::Timestamped(1, Duration::from_secs(1)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Timestamped(2, Duration::from_secs(2)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Watermark(Duration::from_secs(11)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Watermark(Duration::from_secs(12)));
        let window = generator.next_window().unwrap();
        assert_eq!(window.timestamp, Some(Duration::from_secs(2)));
        assert_eq!(window.size, 2);
        drop(window);
        assert!(generator.next_window().is_none());
    }

    #[test]
    fn session_window_flush() {
        let descr = SessionWindow::with_gap(Duration::from_secs(10));
        let mut generator: SessionWindowGenerator<u32, _> = descr.new_generator();

        generator.add(StreamElement::Timestamped(1, Duration::from_secs(1)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Timestamped(2, Duration::from_secs(2)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Watermark(Duration::from_secs(11)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::FlushAndRestart);
        let window = generator.next_window().unwrap();
        assert_eq!(window.timestamp, Some(Duration::from_secs(2)));
        assert_eq!(window.size, 2);
        drop(window);
        assert!(generator.next_window().is_none());
    }

    #[test]
    fn session_window_timestamp() {
        let descr = SessionWindow::with_gap(Duration::from_secs(10));
        let mut generator: SessionWindowGenerator<u32, _> = descr.new_generator();

        generator.add(StreamElement::Timestamped(1, Duration::from_secs(1)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Timestamped(2, Duration::from_secs(2)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Timestamped(2, Duration::from_secs(12)));
        let window = generator.next_window().unwrap();
        assert_eq!(window.timestamp, Some(Duration::from_secs(2)));
        assert_eq!(window.size, 2);
        drop(window);
        assert!(generator.next_window().is_none());
    }
}
