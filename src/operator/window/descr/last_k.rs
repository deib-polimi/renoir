//! The types related to the windowed streams.

use std::collections::VecDeque;

// pub use aggregator::*;
// pub use description::*;

use crate::operator::{Data, StreamElement, Timestamp};

use super::super::*;

#[derive(Clone)]
pub struct LastKWindowManager<A> {
    init: A,
    idx: usize,
    size: usize,
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

impl<A: WindowAccumulator> LastKWindowManager<A> {
    #[inline]
    fn update_slot(&mut self, idx: usize, el: &A::In, ts: Option<Timestamp>) {
        self.ws[idx].count += 1;
        self.ws[idx].ts = match (self.ws[idx].ts, ts) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(t), None) | (None, Some(t)) => Some(t),
            (None, None) => None,
        };
        self.ws[idx].acc.process(el);
    }
}

impl<A: WindowAccumulator> WindowManager for LastKWindowManager<A>
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
                while self.ws.len() < self.size {
                    self.ws.push_back(Slot::new(self.init.clone()))
                }
                let k = self.ws.front().unwrap().count + 1; // TODO: Check
                for i in 0..k {
                    self.update_slot(i, &item, ts);
                }
                self.idx += 1;
                if self.ws[0].count == self.size {
                    let r = self.ws.pop_front().unwrap();
                    Some(WindowResult::new(r.acc.output(), r.ts))
                } else {
                    let r = self.ws.front().cloned().unwrap();
                    Some(WindowResult::new(r.acc.output(), r.ts))
                }
            }
            StreamElement::FlushAndRestart | StreamElement::Terminate => {
                self.ws.drain(..);
                None
            }
            _ => None,
        }
    }
}

/// Similar to a [CountWindow], a LastKWindow windows elements by count,
/// however a window will be emitted for each element that is received,
/// containing up to the `k` most recent elements. Differently from a
/// `CountWindow`, this will emit windows with less than k elements at the
/// start of the stream, and they will be aligned to multiples of `slide`
/// starting from 0, instead of starting from the first valid window.
#[derive(Clone)]
pub struct LastKWindow {
    pub size: usize,
}

impl LastKWindow {
    /// Last K Window with slide = 1
    #[inline]
    pub fn new(size: usize) -> Self {
        Self { size }
    }
}

impl<T: Data> WindowDescription<T> for LastKWindow {
    type Manager<A: WindowAccumulator<In = T>> = LastKWindowManager<A>;

    #[inline]
    fn build<A: WindowAccumulator<In = T>>(&self, accumulator: A) -> Self::Manager<A> {
        LastKWindowManager {
            init: accumulator,
            idx: 0,
            size: self.size,
            ws: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::window::aggr::Fold;

    #[test]
    fn last_k_window() {
        let window = LastKWindow::new(3);

        let fold: Fold<isize, Vec<isize>, _> = Fold::new(Vec::new(), |v, &el| v.push(el));
        let mut manager = window.build(fold);

        let mut res = Vec::new();
        for i in 0..=5 {
            res.extend(
                manager
                    .process(StreamElement::Item(i))
                    .map(WindowResult::unwrap_item),
            );
        }
        manager.process(StreamElement::Terminate);

        eprintln!("{res:?}");
        assert_eq!(
            vec![
                vec![0],
                vec![0, 1],
                vec![0, 1, 2],
                vec![1, 2, 3],
                vec![2, 3, 4],
                vec![3, 4, 5],
            ],
            res
        )
    }
}
