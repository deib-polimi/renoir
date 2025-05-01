use super::super::*;
use crate::operator::{Data, StreamElement};

/// Controls the status of a transaction window after the current element has been accumulated.
#[derive(Default)]
pub enum TransactionOp {
    /// Keep the window open and continue accumulating elements in this window.
    #[default]
    Continue,
    /// Output the result of the accumulator for this window.
    Commit,
    /// Output the result of the accumulator when the watermark is greater than a timestamp.
    CommitAfter(Timestamp),
    /// Discard the result of the accumulator for this window.
    Discard,
}

#[derive(Clone)]
pub struct TransactionWindowManager<A, F>
where
    A: WindowAccumulator,
    F: Fn(&A::In) -> TransactionOp,
{
    init: A,
    f: F,
    w: Option<Slot<A>>,
}

#[derive(Clone)]
struct Slot<A> {
    acc: A,
    close: Option<Timestamp>,
}

impl<A> Slot<A> {
    #[inline]
    fn new(acc: A) -> Self {
        Self { acc, close: None }
    }
}

impl<A: WindowAccumulator, F: Fn(&A::In) -> TransactionOp + Clone + Send + 'static> WindowManager
    for TransactionWindowManager<A, F>
where
    A::In: Data,
    A::Out: Data,
{
    type In = A::In;
    type Out = A::Out;
    type Output = Option<WindowResult<A::Out>>;

    #[inline]
    fn process(&mut self, el: StreamElement<A::In>) -> Self::Output {
        macro_rules! return_current {
            () => {
                return Some(WindowResult::Item(self.w.take().unwrap().acc.output()))
            };
        }

        match el {
            StreamElement::Timestamped(item, _ts) => {
                let slot = self.w.get_or_insert_with(|| Slot::new(self.init.clone()));

                let command = (self.f)(&item);
                slot.acc.process(&item);

                match command {
                    TransactionOp::Commit => return_current!(),
                    TransactionOp::CommitAfter(t) => slot.close = Some(t),
                    TransactionOp::Discard => self.w = None,
                    TransactionOp::Continue => {}
                }
            }
            StreamElement::Watermark(ts) => {
                if let Some(close) = self.w.as_ref().and_then(|w| w.close) {
                    if close < ts {
                        return_current!()
                    }
                }
            }
            StreamElement::Terminate | StreamElement::FlushAndRestart
                if self.w.as_ref().and_then(|w| w.close).is_some() =>
            {
                return_current!()
            }
            StreamElement::Item(_) => panic!(
                "Non timestamped streams are not currently supported with transaction windows!"
            ),
            _ => {}
        }
        None
    }

    fn recycle(&self) -> bool {
        self.w.is_none()
    }
}

/// Window that closes according to user supplied logic
///
/// + Windows are implicitly created when the first element for the partition is received.
/// + Only one window per partition can be active at the same time.
/// + The `logic` function determines when (and if) the window should be committed producing an output.
/// + The `logic` function is called on each element before it is passed to the accumulator.
/// + Returning [`TransactionOp::Continue`] will keep the window open and continue processing.
/// + Returing [`TransactionOp::Commit`] will close the current window and generate an output.
///   (The element triggering the commit will be included in the window)
/// + Returning [`TransactionOp::Discard`] will close the window dropping the accumulator without producing
///   an output
/// + Returning [`TransactionOp::CommitAfter`] will register the window to be commited after a watermark
///   with event time greater than the specified time has been received. The commit time can be overwritten
///   by another message or cancelled by returning [`TransactionOp::Discard`].
#[derive(Clone)]
pub struct TransactionWindow<T, F: Fn(&T) -> TransactionOp> {
    logic: F,
    _t: PhantomData<T>,
}

impl<T, F: Fn(&T) -> TransactionOp> TransactionWindow<T, F> {
    #[inline]
    pub fn new(logic: F) -> Self {
        Self {
            logic,
            _t: PhantomData,
        }
    }
}

impl<T: Data, F: Fn(&T) -> TransactionOp + Data> WindowDescription<T> for TransactionWindow<T, F> {
    type Manager<A: WindowAccumulator<In = T>> = TransactionWindowManager<A, F>;

    #[inline]
    fn build<A: WindowAccumulator<In = T>>(&self, accumulator: A) -> Self::Manager<A> {
        TransactionWindowManager {
            init: accumulator,
            f: self.logic.clone(),
            w: None,
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use std::time::Duration;

//     use super::*;
//     use crate::operator::window::aggr::Fold;

//     macro_rules! save_result {
//         ($ret:expr, $v:expr) => {{
//             let iter = $ret.into_iter().map(|r| r.unwrap_item());
//             $v.extend(iter);
//         }};
//     }

//     #[test]
//     fn event_time_window() {
//         let window = TransactionWindow::new(Duration::from_millis(10));

//         let fold = Fold::new(Vec::new(), |v, el| v.push(el));
//         let mut manager = window.build(fold);

//         let mut received = Vec::new();
//         for i in 0..100i64 {
//             if i == 33 || i == 80 {
//                 std::thread::sleep(Duration::from_millis(11))
//             }
//             save_result!(
//                 manager.process(StreamElement::Timestamped(i, i / 10)),
//                 received
//             );
//         }
//         save_result!(manager.process(StreamElement::FlushAndRestart), received);

//         received.sort();

//         let expected: Vec<Vec<_>> =
//             vec![(0..33).collect(), (33..80).collect(), (80..100).collect()];
//         assert_eq!(received, expected)
//     }
// }
