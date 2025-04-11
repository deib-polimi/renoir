use std::cmp::Ordering;
use std::fmt::Display;
use std::vec::IntoIter;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;

enum State<T> {
    Accumulating(Vec<T>),
    Sorting(Vec<T>),
    Flushing(IntoIter<T>, usize),
    Restarting,
    Terminating,
}

impl<T> Default for State<T> {
    fn default() -> Self {
        Self::Accumulating(Vec::new())
    }
}

pub struct LimitSorted<F, Op>
where
    Op: Operator,
{
    prev: Op,
    limit: Option<usize>,
    offset: Option<usize>,
    sorted: bool,
    sort_by: F,
    state: State<Op::Out>,

    timestamp: Option<Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
}

impl<F: Clone, Op: Clone> Clone for LimitSorted<F, Op>
where
    Op: Operator,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            limit: self.limit,
            sorted: self.sorted,
            offset: self.offset,
            sort_by: self.sort_by.clone(),
            state: Default::default(),
            timestamp: Default::default(),
            max_watermark: Default::default(),
            received_end: Default::default(),
        }
    }
}

impl<F, Op: Operator> Display for LimitSorted<F, Op> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> LimitSorted<{}>",
            self.prev,
            std::any::type_name::<Op::Out>(),
        )
    }
}

impl<F, Op: Operator> LimitSorted<F, Op>
where
    F: Fn(&Op::Out, &Op::Out) -> Ordering + Clone + Send,
{
    pub(super) fn new(
        prev: Op,
        sort_by: F,
        limit: Option<usize>,
        offset: Option<usize>,
        sorted: bool,
    ) -> Self {
        LimitSorted {
            prev,
            limit,
            sorted,
            sort_by,
            offset,
            state: Default::default(),
            timestamp: None,
            max_watermark: None,
            received_end: false,
        }
    }
}

impl<F, Op: Operator> Operator for LimitSorted<F, Op>
where
    F: Fn(&Op::Out, &Op::Out) -> Ordering + Clone + Send,
{
    type Out = Op::Out;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Op::Out> {
        loop {
            match &mut self.state {
                State::Accumulating(buf) => {
                    match self.prev.next() {
                        StreamElement::Terminate => {
                            self.received_end = true;
                            self.state = State::Sorting(std::mem::take(buf));
                        }
                        StreamElement::FlushAndRestart => {
                            self.state = State::Sorting(std::mem::take(buf));
                        }
                        StreamElement::Watermark(ts) => {
                            self.max_watermark = Some(self.max_watermark.unwrap_or(ts).max(ts))
                        }
                        StreamElement::Item(item) => {
                            buf.push(item);
                        }
                        StreamElement::Timestamped(item, ts) => {
                            self.timestamp = Some(self.timestamp.unwrap_or(ts).max(ts));
                            buf.push(item);
                        }
                        // this block wont sent anything until the stream ends
                        StreamElement::FlushBatch => {}
                    }
                }
                State::Sorting(items) => {
                    if self.sorted {
                        items.sort_by(&self.sort_by);
                    }
                    let mut iter = std::mem::take(items).into_iter();
                    if let Some(offset) = self.offset {
                        for _ in 0..offset {
                            // TODO: this is not nice
                            _ = iter.next();
                        }
                    }

                    let count = self.limit.unwrap_or(iter.len()).min(iter.len());
                    self.state = State::Flushing(iter, count);
                }
                State::Flushing(iter, count) => {
                    if *count > 0 {
                        let item = iter.next().unwrap();
                        *count -= 1;
                        if let Some(ts) = self.timestamp {
                            return StreamElement::Timestamped(item, ts);
                        } else {
                            return StreamElement::Item(item);
                        }
                    }

                    if self.received_end {
                        self.state = State::Terminating;
                        return StreamElement::FlushAndRestart;
                    } else {
                        self.state = State::Restarting;
                        if let Some(w) = self.max_watermark {
                            return StreamElement::Watermark(w);
                        }
                    }
                }
                State::Restarting => {
                    self.state = State::Accumulating(Vec::new());
                    return StreamElement::FlushAndRestart;
                }
                State::Terminating => return StreamElement::Terminate,
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Op::Out, _>("LimitSorted"))
    }
}
