use std::collections::VecDeque;

use super::super::*;
use crate::operator::{Data, StreamElement, Timestamp};

#[derive(Clone)]
pub struct EventTimeWindowManager<A>
where
    A: WindowAccumulator,
{
    init: A,
    size: Timestamp,
    slide: Timestamp,
    ws: VecDeque<Slot<A>>,
}

#[derive(Clone)]
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
                while self.ws.back().map(|b| b.start < ts).unwrap_or(true) {
                    let next_start = self.ws.back().map(|b| b.start + self.slide).unwrap_or(ts);
                    self.ws.push_back(Slot::new(
                        self.init.clone(),
                        next_start,
                        next_start + self.size,
                    ));
                }
                self.ws
                    .iter_mut()
                    .skip_while(|w| w.end <= ts)
                    .take_while(|w| w.start <= ts)
                    .for_each(|w| {
                        w.acc.process(item.clone());
                        w.active = true;
                    });

                Vec::new()
            }
            StreamElement::Watermark(ts) => {
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
}

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

impl<T: Data> WindowBuilder<T> for EventTimeWindow {
    type Manager<A: WindowAccumulator<In = T>> = EventTimeWindowManager<A>;

    #[inline]
    fn build<A: WindowAccumulator<In = T>>(&self, accumulator: A) -> Self::Manager<A> {
        EventTimeWindowManager {
            init: accumulator,
            size: self.size,
            slide: self.slide,
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

        let fold = Fold::new(Vec::new(), |v, el| v.push(el));
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
}
