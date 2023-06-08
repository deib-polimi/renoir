use std::collections::VecDeque;
use std::time::{Duration, Instant};

use super::super::*;
use crate::operator::{Data, StreamElement};

#[derive(Clone)]
pub struct ProcessingTimeWindowManager<A>
where
    A: WindowAccumulator,
{
    init: A,
    size: Duration,
    slide: Duration,
    ws: VecDeque<Slot<A>>,
}

#[derive(Clone)]
struct Slot<A> {
    acc: A,
    start: Instant,
    end: Instant,
    active: bool,
}

impl<A> Slot<A> {
    #[inline]
    fn new(acc: A, start: Instant, end: Instant) -> Self {
        Self {
            acc,
            start,
            end,
            active: false,
        }
    }
}

impl<A: WindowAccumulator> WindowManager for ProcessingTimeWindowManager<A>
where
    A::In: Data,
    A::Out: Data,
{
    type In = A::In;
    type Out = A::Out;
    type Output = Vec<WindowResult<A::Out>>;

    #[inline]
    fn process(&mut self, el: StreamElement<A::In>) -> Self::Output {
        let now = Instant::now();
        match el {
            StreamElement::Item(item) | StreamElement::Timestamped(item, _) => {
                // TODO: Windows are not aligned if there are periods without windows, evaluate if it needs to be changed
                while self.ws.back().map(|b| b.start < now).unwrap_or(true) {
                    let next_start = self.ws.back().map(|b| b.start + self.slide).unwrap_or(now);
                    self.ws.push_back(Slot::new(
                        self.init.clone(),
                        next_start,
                        next_start + self.size,
                    ));
                }
                self.ws
                    .iter_mut()
                    .skip_while(|w| w.end <= now)
                    .take_while(|w| w.start <= now)
                    .for_each(|w| {
                        w.acc.process(item.clone());
                        w.active = true;
                    });
            }
            StreamElement::Terminate | StreamElement::FlushAndRestart => {
                return self
                    .ws
                    .drain(..)
                    .filter(|w| w.active)
                    .map(|w| WindowResult::Item(w.acc.output()))
                    .collect();
            }
            _ => {}
        }

        let split = self.ws.partition_point(|w| w.end < now);
        self.ws
            .drain(..split)
            .filter(|w| w.active)
            .map(|w| WindowResult::Item(w.acc.output()))
            .collect()
    }
}

/// Window based on wall clock at time of processing
#[derive(Clone)]
pub struct ProcessingTimeWindow {
    size: Duration,
    slide: Duration,
}

impl ProcessingTimeWindow {
    #[inline]
    pub fn sliding(size: Duration, slide: Duration) -> Self {
        assert!(!size.is_zero(), "window size must be > 0");
        assert!(!slide.is_zero(), "window slide must be > 0");
        Self { size, slide }
    }

    #[inline]
    pub fn tumbling(size: Duration) -> Self {
        assert!(!size.is_zero(), "window size must be > 0");
        Self { size, slide: size }
    }
}

impl<T: Data> WindowDescription<T> for ProcessingTimeWindow {
    type Manager<A: WindowAccumulator<In = T>> = ProcessingTimeWindowManager<A>;

    #[inline]
    fn build<A: WindowAccumulator<In = T>>(&self, accumulator: A) -> Self::Manager<A> {
        ProcessingTimeWindowManager {
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
        ($ret:expr, $v:expr, $n:ident) => {{
            let iter = $ret
                .into_iter()
                .inspect(|r| {
                    if !r.item().is_empty() {
                        $n += 1;
                    }
                })
                .map(|r| r.unwrap_item())
                .flatten();
            $v.extend(iter);
        }};
    }

    #[test]
    #[ignore]
    fn processing_time_window() {
        let size = Duration::from_micros(100);
        let window = ProcessingTimeWindow::tumbling(size);

        let fold: Fold<isize, Vec<isize>, _> = Fold::new(Vec::new(), |v, el| v.push(el));
        let mut manager = window.build(fold);

        let start = Instant::now();
        let mut received = Vec::new();
        let mut n_windows = 0;
        for i in 1..100 {
            save_result!(manager.process(StreamElement::Item(i)), received, n_windows);
        }
        let expected_n = start.elapsed().as_micros() / size.as_micros() + 1;

        save_result!(
            manager.process(StreamElement::FlushAndRestart),
            received,
            n_windows
        );

        eprintln!("expected {expected_n} windows");

        received.sort();
        assert_eq!(n_windows, expected_n);
        assert_eq!(received, (1..100).collect::<Vec<_>>())
    }
}
