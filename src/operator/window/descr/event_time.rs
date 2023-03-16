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
}

impl<A> Slot<A> {
    fn new(acc: A, start: Timestamp, end: Timestamp) -> Self {
        Self { acc, start, end }
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
                    .for_each(|w| w.acc.process(item.clone()));

                Vec::new()
            }
            StreamElement::Watermark(ts) => {
                let split = self.ws.partition_point(|w| w.end < ts);
                self.ws
                    .drain(..split)
                    .map(|w| WindowResult::Timestamped(w.acc.output(), w.end))
                    .collect()
            }
            StreamElement::FlushAndRestart | StreamElement::Terminate => self
                .ws
                .drain(..)
                .map(|w| WindowResult::Timestamped(w.acc.output(), w.end))
                .collect(),
            StreamElement::Item(_) => {
                panic!("Event time windows can only handle timestamped items!")
            }
            _ => Vec::new(),
        }
    }
}

pub struct EventTimeWindow {
    size: Timestamp,
    slide: Timestamp,
}

impl EventTimeWindow {
    pub fn sliding(size: Timestamp, slide: Timestamp) -> Self {
        assert!(size > 0, "window size must be > 0");
        assert!(slide > 0, "window slide must be > 0");
        Self { size, slide }
    }

    pub fn tumbling(size: Timestamp) -> Self {
        assert!(size > 0, "window size must be > 0");
        Self { size, slide: size }
    }
}

impl WindowBuilder for EventTimeWindow {
    type Manager<A: WindowAccumulator> = EventTimeWindowManager<A>;

    fn build<A: WindowAccumulator>(&self, accumulator: A) -> Self::Manager<A> {
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

    macro_rules! check_return {
        ($ret:expr, $v:expr) => {{
            let mut ia = $ret.into_iter();
            let mut ib = $v.into_iter();
            loop {
                let (a, b) = (ia.next(), ib.next());
                assert_eq!(a, b);

                if let (None, None) = (a, b) {
                    break;
                }
            }
        }};
    }

    #[test]
    fn count_window() {
        let size = 3;
        let slide = 2;
        let window = EventTimeWindow::sliding(3, 2);

        let fold: Fold<isize, Vec<isize>, _> = Fold::new(Vec::new(), |v, el| v.push(el));
        let mut manager = window.build(fold);

        for i in 1..100 {
            let expected = if i >= size && (i - size) % slide == 0 {
                let v = ((i - size + 1)..=(i)).collect::<Vec<_>>();
                Some(WindowResult::Item(v))
            } else {
                None
            };
            eprintln!("{expected:?}");
            check_return!(manager.process(StreamElement::Item(i)), expected);
        }
    }

    #[test]
    #[cfg(feature = "timestamp")]
    fn count_window_timestamped() {
        let size = 3;
        let slide = 2;
        let window = EventTimeWindow::sliding(3, 2);

        let fold: Fold<isize, Vec<isize>, _> = Fold::new(Vec::new(), |v, el| v.push(el));
        let mut manager = window.build(fold);

        for i in 1..100 {
            let expected = if i >= size && (i - size) % slide == 0 {
                let v = ((i - size + 1)..=(i)).collect::<Vec<_>>();
                Some(WindowResult::Timestamped(v, i as i64 / 2))
            } else {
                None
            };
            eprintln!("{expected:?}");
            check_return!(
                manager.process(StreamElement::Timestamped(i, i as i64 / 2)),
                expected
            );
        }
    }
}
