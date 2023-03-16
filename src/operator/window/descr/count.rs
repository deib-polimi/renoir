//! The types related to the windowed streams.

use std::collections::VecDeque;
use std::fmt::Display;
use std::marker::PhantomData;

// pub use aggregator::*;
// pub use description::*;
use hashbrown::HashMap;

use crate::block::OperatorStructure;
use crate::operator::{Data, DataKey, ExchangeData, Operator, StreamElement, Timestamp};
use crate::stream::{KeyValue, KeyedStream, KeyedWindowedStream, Stream, WindowedStream};

use super::super::*;

#[derive(Clone)]
pub struct CountWindowManager<A> {
    init: A,
    size: usize,
    slide: usize,
    ws: VecDeque<(usize, Option<Timestamp>, A)>,
}

impl<A: WindowAccumulator> CountWindowManager<A> {
    fn update_slot(&mut self, idx: usize, el: A::In, ts: Option<Timestamp>) {
        self.ws[idx].0 += 1;
        self.ws[idx].1 = match (self.ws[idx].1, ts) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(t), None) | (None, Some(t)) => Some(t),
            (None, None) => None,
        };
        self.ws[idx].2.process(el);
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

    fn process(&mut self, el: StreamElement<A::In>) -> Self::Output {
        let ts = el.timestamp().cloned();
        match el {
            StreamElement::Item(item) | StreamElement::Timestamped(item, _) => {
                while self.ws.len() < (self.size + self.slide - 1) / self.slide {
                    self.ws.push_back((0, None, self.init.clone()))
                }
                let k = self.ws.front().unwrap().0 / self.slide + 1; // TODO: Check
                for i in 0..k {
                    self.update_slot(i, item.clone(), ts.clone());
                }
                if self.ws[0].0 == self.size {
                    let r = self.ws.pop_front().unwrap();
                    Some(WindowResult::new(r.2.output(), r.1))
                } else {
                    None
                }
            }
            // Only return complete windows
            _ => None,
        }
    }
}

pub struct CountWindow {
    size: usize,
    slide: usize,
}

impl CountWindow {
    pub fn sliding(size: usize, slide: usize) -> Self {
        Self {
            size,
            slide,
        }
    }

    pub fn tumbling(size: usize) -> Self {
        Self {
            size,
            slide: size,
        }
    }
}

impl WindowBuilder for CountWindow
{
    type Manager<A: WindowAccumulator> = CountWindowManager<A>;

    fn build<A: WindowAccumulator>(&self, accumulator: A) -> Self::Manager<A> {
        CountWindowManager {
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
    use crate::operator::window::aggr::FoldWrap;

    macro_rules! check_return {
        ($ret:expr, $v:expr) => {
            {
                let mut ia = $ret.into_iter();
                let mut ib = $v.into_iter();
                loop {
                    let (a, b) = (ia.next(), ib.next());
                    assert_eq!(a, b);

                    if let (None, None) = (a, b) {
                        break;
                    }
                }
            }
        };
    }

    #[test]
    fn count_window() {
        let size = 3;
        let slide = 2;
        let window = CountWindow::sliding(3, 2);

        let fold: FoldWrap<isize, Vec<isize>, _> = FoldWrap::new(Vec::new(), |v, el| v.push(el));
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

        let fold: FoldWrap<isize, Vec<isize>, _> = FoldWrap::new(Vec::new(), |v, el| v.push(el));
        let mut manager = window.build(fold);

        for i in 1..100 {
            let expected = if i >= size && (i - size) % slide == 0 {
                let v = ((i - size + 1)..=(i)).collect::<Vec<_>>();
                Some(WindowResult::Timestamped(v, i as i64 / 2))
            } else {
                None
            };
            eprintln!("{expected:?}");
            check_return!(manager.process(StreamElement::Timestamped(i, i as i64 / 2)), expected);
        }
    }
}