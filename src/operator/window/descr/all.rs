//! The types related to the windowed streams.

use crate::operator::{Data, StreamElement, Timestamp};

use super::super::*;

#[derive(Clone)]
pub struct AllWindowManager<A> {
    init: A,
    accumulator: Option<A>,
    tsmin: Option<Timestamp>,
}

impl<A: WindowAccumulator> WindowManager for AllWindowManager<A>
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
        self.tsmin = match (ts, self.tsmin.take()) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) | (None, Some(a)) => Some(a),
            (None, None) => None,
        };
        match el {
            StreamElement::Item(item) | StreamElement::Timestamped(item, _) => {
                self.accumulator
                    .get_or_insert_with(|| self.init.clone())
                    .process(&item);
                None
            }
            StreamElement::FlushAndRestart => Some(WindowResult::new(
                self.accumulator.take().unwrap().output(),
                self.tsmin.take(),
            )),
            StreamElement::Terminate => self
                .accumulator
                .take()
                .map(|a| WindowResult::new(a.output(), self.tsmin.take())),
            _ => None,
        }
    }
}

/// Window of fixed count of elements
#[derive(Clone, Default)]
pub struct AllWindow;

impl AllWindow {
    /// Windows that contains all elements until the stream is flushed and restarted or terminated.
    #[inline]
    pub fn new() -> Self {
        Self
    }
}

impl<T: Data> WindowDescription<T> for AllWindow {
    type Manager<A: WindowAccumulator<In = T>> = AllWindowManager<A>;

    #[inline]
    fn build<A: WindowAccumulator<In = T>>(&self, accumulator: A) -> Self::Manager<A> {
        AllWindowManager {
            init: accumulator,
            accumulator: None,
            tsmin: None,
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
    fn all_window() {
        let window = AllWindow::new();

        let fold: Fold<isize, Vec<isize>, _> = Fold::new(Vec::new(), |v, &el| v.push(el));
        let mut manager = window.build(fold);

        for i in 1..100 {
            check_return!(manager.process(StreamElement::Item(i)), None);
        }

        check_return!(
            manager.process(StreamElement::FlushAndRestart),
            Some(WindowResult::Item((1..100).collect()))
        );
        check_return!(manager.process(StreamElement::Terminate), None);
    }
}
