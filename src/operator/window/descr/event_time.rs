use std::collections::VecDeque;

use super::super::*;
use crate::operator::{Data, StreamElement, Timestamp};

#[derive(Clone, Debug)]
pub struct EventTimeWindowManager<A>
where
    A: WindowAccumulator,
{
    init: A,
    size: Timestamp,
    slide: Timestamp,
    last_watermark: Option<Timestamp>,
    ws: VecDeque<Slot<A>>,
}
impl<A: WindowAccumulator> EventTimeWindowManager<A> {
    fn alloc_windows(&mut self, ts: Timestamp) {
        assert!(self.last_watermark.map(|w| ts >= w).unwrap_or(true));

        while self.ws.back().map(|b| b.start < ts).unwrap_or(true) {
            let mut next_start = self.ws.back().map(|b| b.start + self.slide).unwrap_or(ts);
            // Skip empty windows
            if let Some(w) = self.last_watermark {
                next_start += (w - next_start).max(0) / self.slide * self.slide
            }

            log::trace!("New window {}..{}", next_start, next_start + self.size);
            self.ws.push_back(Slot::new(
                self.init.clone(),
                next_start,
                next_start + self.size,
            ));
        }
    }
}

#[derive(Clone, Debug)]
struct Slot<A> {
    acc: A,
    start: Timestamp,
    end: Timestamp,
    active: bool,
}

impl<A> Slot<A> {
    #[inline]
    fn new(acc: A, start: Timestamp, end: Timestamp) -> Self {
        Self {
            acc,
            start,
            end,
            active: false,
        }
    }
}

impl<A: WindowAccumulator> WindowManager for EventTimeWindowManager<A>
where
    A::In: Data,
    A::Out: Data,
{
    type In = A::In;
    type Out = A::Out;
    type Output = Vec<WindowResult<A::Out>>;

    #[inline]
    fn process(&mut self, el: StreamElement<A::In>) -> Self::Output {
        match el {
            StreamElement::Timestamped(item, ts) => {
                self.alloc_windows(ts);
                self.ws
                    .iter_mut()
                    .skip_while(|w| w.end <= ts)
                    .take_while(|w| w.start <= ts)
                    .for_each(|w| {
                        w.acc.process(&item);
                        w.active = true;
                    });

                Vec::new()
            }
            StreamElement::Watermark(ts) => {
                self.last_watermark = Some(ts);
                let split = self.ws.partition_point(|w| w.end < ts);
                self.ws
                    .drain(..split)
                    .filter(|w| w.active)
                    .map(|w| WindowResult::Timestamped(w.acc.output(), w.end))
                    .collect()
            }
            StreamElement::FlushAndRestart | StreamElement::Terminate => self
                .ws
                .drain(..)
                .filter(|w| w.active)
                .map(|w| WindowResult::Timestamped(w.acc.output(), w.end))
                .collect(),
            StreamElement::Item(_) => {
                panic!("Event time windows can only handle timestamped items!")
            }
            _ => Vec::new(),
        }
    }

    fn recycle(&self) -> bool {
        self.ws.is_empty()
    }
}

/// Window based on event timestamps
#[derive(Clone)]
pub struct EventTimeWindow {
    size: Timestamp,
    slide: Timestamp,
}

impl EventTimeWindow {
    #[inline]
    pub fn sliding(size: Timestamp, slide: Timestamp) -> Self {
        assert!(size > 0, "window size must be > 0");
        assert!(slide > 0, "window slide must be > 0");
        Self { size, slide }
    }

    #[inline]
    pub fn tumbling(size: Timestamp) -> Self {
        assert!(size > 0, "window size must be > 0");
        Self { size, slide: size }
    }
}

impl<T: Data> WindowDescription<T> for EventTimeWindow {
    type Manager<A: WindowAccumulator<In = T>> = EventTimeWindowManager<A>;

    #[inline]
    fn build<A: WindowAccumulator<In = T>>(&self, accumulator: A) -> Self::Manager<A> {
        EventTimeWindowManager {
            init: accumulator,
            size: self.size,
            slide: self.slide,
            last_watermark: Default::default(),
            ws: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::window::aggr::Fold;

    macro_rules! save_result {
        ($ret:expr, $v:expr) => {{
            let iter = $ret.into_iter().map(|r| r.unwrap_item());
            $v.extend(iter);
        }};
    }

    #[test]
    fn event_time_window() {
        let window = EventTimeWindow::sliding(5, 4);

        let fold = Fold::new(Vec::new(), |v, &el| v.push(el));
        let mut manager = window.build(fold);

        let mut received = Vec::new();
        for i in 1..100i64 {
            save_result!(
                manager.process(StreamElement::Timestamped(i, i / 10)),
                received
            );

            if i % 7 == 0 {
                save_result!(manager.process(StreamElement::Watermark(i / 10)), received);
            }
        }
        save_result!(manager.process(StreamElement::FlushAndRestart), received);

        received.sort();

        let expected: Vec<Vec<_>> =
            vec![(1..50).collect(), (40..90).collect(), (80..100).collect()];
        assert_eq!(received, expected)
    }

    #[test]
    fn event_time_window_spars() {
        let window = EventTimeWindow::sliding(5, 4);

        let fold = Fold::new(Vec::new(), |v, &el| v.push(el));
        let mut manager = window.build(fold);

        let mut received = Vec::new();
        for i in 1..40 {
            if i % 15 <= 1 {
                save_result!(manager.process(StreamElement::Timestamped(i, i)), received);
            }
            if i % 3 == 0 {
                save_result!(manager.process(StreamElement::Watermark(i)), received);
            }
        }
        save_result!(manager.process(StreamElement::FlushAndRestart), received);

        received.sort();

        let expected: Vec<Vec<_>> = vec![vec![1], vec![15, 16], vec![30, 31]];
        assert_eq!(received, expected)
    }
}
