//! The types related to the windowed streams.

use std::collections::VecDeque;

// pub use aggregator::*;
// pub use description::*;

use crate::operator::{Data, StreamElement, Timestamp};

use super::super::*;

#[derive(Clone)]
pub struct CountWindowManager<A> {
    init: A,
    size: usize,
    slide: usize,
    exact: bool,
    ws: VecDeque<Slot<A>>,
}

#[derive(Clone)]
struct Slot<A> {
    count: usize,
    acc: A,
    ts: Option<Timestamp>,
}

impl<A> Slot<A> {
    #[inline]
    fn new(acc: A) -> Self {
        Self {
            count: 0,
            acc,
            ts: None,
        }
    }
}

impl<A: WindowAccumulator> CountWindowManager<A> {
    #[inline]
    fn update_slot(&mut self, idx: usize, el: A::In, ts: Option<Timestamp>) {
        self.ws[idx].count += 1;
        self.ws[idx].ts = match (self.ws[idx].ts, ts) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(t), None) | (None, Some(t)) => Some(t),
            (None, None) => None,
        };
        self.ws[idx].acc.process(el);
    }
}

impl<A: WindowAccumulator> WindowManager for CountWindowManager<A>
where
    A::In: Data,
    A::Out: Data,
{
    type In = A::In;
    type Out = A::Out;
    type Output = Option<WindowResult<A::Out>>;

    #[inline]
    fn process(&mut self, el: StreamElement<A::In>) -> Self::Output {
        let ts = el.timestamp().cloned();
        match el {
            StreamElement::Item(item) | StreamElement::Timestamped(item, _) => {
                while self.ws.len() < (self.size + self.slide - 1) / self.slide {
                    self.ws.push_back(Slot::new(self.init.clone()))
                }
                let k = self.ws.front().unwrap().count / self.slide + 1; // TODO: Check
                for i in 0..k {
                    self.update_slot(i, item.clone(), ts);
                }
                if self.ws[0].count == self.size {
                    let r = self.ws.pop_front().unwrap();
                    Some(WindowResult::new(r.acc.output(), r.ts))
                } else {
                    None
                }
            }
            StreamElement::FlushAndRestart | StreamElement::Terminate => {
                let ret = if self.exact {
                    None
                } else {
                    self.ws
                        .pop_front()
                        .filter(|r| r.count > 0)
                        .map(|r| WindowResult::new(r.acc.output(), r.ts))
                };
                self.ws.drain(..);
                ret
            }
            _ => None,
        }
    }
}

/// Window of fixed count of elements
#[derive(Clone)]
pub struct CountWindow {
    pub size: usize,
    pub slide: usize,
    /// If exact is `true`, only results from windows of size `size` will be returned.
    /// If exact is `false`, on terminate, the first incomplete window result will be returned if present
    pub exact: bool,
}

impl CountWindow {
    /// Windows of `size` elements, generated each `slide` elements.
    /// If exact is `true`, only results from windows of size `size` will be returned.
    /// If exact is `false`, on terminate, the first incomplete window result will be returned if present
    #[inline]
    pub fn new(size: usize, slide: usize, exact: bool) -> Self {
        Self { size, slide, exact }
    }

    /// Exact windows of `size` elements, generated each `slide` elements
    #[inline]
    pub fn sliding(size: usize, slide: usize) -> Self {
        assert!(size > 0, "window size must be > 0"); // TODO: consider using NonZeroUsize
        assert!(slide > 0, "window slide must be > 0");
        Self {
            size,
            slide,
            exact: true,
        }
    }

    /// Exact windows of `size` elements
    #[inline]
    pub fn tumbling(size: usize) -> Self {
        assert!(size > 0, "window size must be > 0");
        Self {
            size,
            slide: size,
            exact: true,
        }
    }
}

impl<T: Data> WindowDescription<T> for CountWindow {
    type Manager<A: WindowAccumulator<In = T>> = CountWindowManager<A>;

    #[inline]
    fn build<A: WindowAccumulator<In = T>>(&self, accumulator: A) -> Self::Manager<A> {
        CountWindowManager {
            init: accumulator,
            size: self.size,
            slide: self.slide,
            exact: self.exact,
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
        let window = CountWindow::sliding(3, 2);

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
        let window = CountWindow::sliding(3, 2);

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
