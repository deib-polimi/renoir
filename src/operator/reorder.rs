use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt::Display;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;

#[derive(Clone)]
struct TimestampedItem<Out> {
    item: Out,
    timestamp: Timestamp,
}

impl<Out> Ord for TimestampedItem<Out> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl<Out> PartialOrd for TimestampedItem<Out> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<Out> Eq for TimestampedItem<Out> {}

impl<Out> PartialEq for TimestampedItem<Out> {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
    }
}

#[derive(Clone)]
pub(crate) struct Reorder<Out: Data, PreviousOperators>
where
    PreviousOperators: Operator<Out = Out>,
{
    buffer: VecDeque<TimestampedItem<Out>>,
    // Scratch memory used for glidesort
    scratch: Vec<TimestampedItem<Out>>,
    last_watermark: Option<Timestamp>,
    prev: PreviousOperators,
    received_end: bool,
}

impl<Out: Data, PreviousOperators> Display for Reorder<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out = Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Reorder<{}>",
            self.prev,
            std::any::type_name::<Out>(),
        )
    }
}

impl<Out: Data, PreviousOperators> Reorder<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out = Out>,
{
    pub(crate) fn new(prev: PreviousOperators) -> Self {
        Self {
            buffer: Default::default(),
            scratch: Default::default(),
            last_watermark: None,
            prev,
            received_end: false,
        }
    }
}

impl<Out: Data, PreviousOperators> Operator for Reorder<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out = Out>,
{
    type Out = Out;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Out> {
        while !self.received_end && self.last_watermark.is_none() {
            match self.prev.next() {
                element @ StreamElement::Item(_) => return element,
                StreamElement::Timestamped(item, timestamp) => {
                    self.buffer.push_back(TimestampedItem { item, timestamp })
                }
                StreamElement::Watermark(ts) => {
                    self.last_watermark = Some(ts);
                    glidesort::sort_with_vec(self.buffer.make_contiguous(), &mut self.scratch);
                }
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::FlushAndRestart => {
                    self.received_end = true;
                    glidesort::sort_with_vec(self.buffer.make_contiguous(), &mut self.scratch);
                }
                StreamElement::Terminate => return StreamElement::Terminate,
            }
        }

        if let Some(w) = self.last_watermark {
            match self.buffer.front() {
                Some(front) if front.timestamp <= w => {
                    let element = self.buffer.pop_front().unwrap();
                    StreamElement::Timestamped(element.item, element.timestamp)
                }
                _ => StreamElement::Watermark(self.last_watermark.take().unwrap()),
            }
        } else {
            // here we know that received_end must be true, otherwise we wouldn't have exited the loop
            match self.buffer.pop_front() {
                // pop remaining elements in the heap
                Some(element) => StreamElement::Timestamped(element.item, element.timestamp),
                None => {
                    self.received_end = false;
                    StreamElement::FlushAndRestart
                }
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Out, _>("Reorder"))
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::reorder::Reorder;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[cfg(feature = "timestamp")]
    #[test]
    fn reorder() {
        let mut fake = FakeOperator::empty();
        for i in &[1, 3, 2] {
            fake.push(StreamElement::Timestamped(*i, *i));
        }
        fake.push(StreamElement::Watermark(3));
        for i in &[6, 4, 5] {
            fake.push(StreamElement::Timestamped(*i, *i));
        }
        fake.push(StreamElement::FlushAndRestart);
        fake.push(StreamElement::Terminate);

        let mut reorder = Reorder::new(fake);

        for i in 1..=6 {
            assert_eq!(reorder.next(), StreamElement::Timestamped(i, i));
            if i == 3 {
                assert_eq!(reorder.next(), StreamElement::Watermark(3));
            }
        }

        assert_eq!(reorder.next(), StreamElement::FlushAndRestart);
        assert_eq!(reorder.next(), StreamElement::Terminate);
    }
}
