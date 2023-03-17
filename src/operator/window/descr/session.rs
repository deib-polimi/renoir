use std::time::{SystemTime, UNIX_EPOCH};

use super::super::*;
use crate::operator::{Data, StreamElement, Timestamp};

#[derive(Clone)]
pub struct SessionWindowManager<A>
where
    A: WindowAccumulator,
{
    init: A,
    gap: Timestamp,
    w: Option<Slot<A>>,
}

#[derive(Clone)]
struct Slot<A> {
    acc: A,
    last: Timestamp,
}

impl<A> Slot<A> {
    fn new(acc: A, last: Timestamp) -> Self {
        Self { acc, last }
    }
}

impl<A: WindowAccumulator> WindowManager for SessionWindowManager<A>
where
    A::In: Data,
    A::Out: Data,
{
    type In = A::In;
    type Out = A::Out;
    type Output = Option<WindowResult<A::Out>>;

    fn process(&mut self, el: StreamElement<A::In>) -> Self::Output {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let ret = match &self.w {
            Some(slot) if ts - slot.last > self.gap => {
                let output = self.w.take().unwrap().acc.output();
                Some(WindowResult::Item(output))
            }
            _ => None,
        };

        match el {
            StreamElement::Item(item) | StreamElement::Timestamped(item, _) => {
                let slot = self
                    .w
                    .get_or_insert_with(|| Slot::new(self.init.clone(), ts));
                slot.acc.process(item);
                slot.last = ts;
                ret
            }
            StreamElement::Terminate | StreamElement::FlushAndRestart => {
                ret.or_else(|| self.w.take().map(|s| WindowResult::Item(s.acc.output())))
            }
            _ => ret,
        }
    }
}

#[derive(Clone)]
pub struct SessionWindow {
    gap: Timestamp,
}

impl SessionWindow {
    pub fn new(gap_millis: Timestamp) -> Self {
        assert!(gap_millis > 0, "window size must be > 0");
        Self { gap: gap_millis }
    }
}

impl WindowBuilder for SessionWindow {
    type Manager<A: WindowAccumulator> = SessionWindowManager<A>;

    fn build<A: WindowAccumulator>(&self, accumulator: A) -> Self::Manager<A> {
        SessionWindowManager {
            init: accumulator,
            gap: self.gap,
            w: Default::default(),
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
        let window = SessionWindow::new(100);

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
}
