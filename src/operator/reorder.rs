use crate::operator::{Data, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

#[derive(Clone)]
struct HeapElement<Out> {
    item: Out,
    timestamp: Timestamp,
}

impl<Out> Ord for HeapElement<Out> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Returns the opposite of the actual order of the timestamps
        // This is needed to have a min-heap
        other.timestamp.cmp(&self.timestamp)
    }
}

impl<Out> PartialOrd for HeapElement<Out> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<Out> Eq for HeapElement<Out> {}

impl<Out> PartialEq for HeapElement<Out> {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
    }
}

#[derive(Clone)]
pub(crate) struct Reorder<Out: Data, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    buffer: BinaryHeap<HeapElement<Out>>,
    last_watermark: Option<Timestamp>,
    prev: PreviousOperators,
    received_end: bool,
}

impl<Out: Data, PreviousOperators> Reorder<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    pub(crate) fn new(prev: PreviousOperators) -> Self {
        Self {
            buffer: Default::default(),
            last_watermark: None,
            prev,
            received_end: false,
        }
    }
}

impl<Out: Data, PreviousOperators> Operator<Out> for Reorder<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Out> {
        while !self.received_end && self.last_watermark.is_none() {
            match self.prev.next() {
                // TODO: should this do something different with elements that are not timestamped?
                element @ StreamElement::Item(_) => return element,
                StreamElement::Timestamped(item, timestamp) => {
                    self.buffer.push(HeapElement { item, timestamp })
                }
                StreamElement::Watermark(ts) => self.last_watermark = Some(ts),
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::End => self.received_end = true,
            }
        }

        if let Some(ts) = self.last_watermark {
            match self.buffer.peek() {
                Some(top) if top.timestamp <= ts => {
                    let element = self.buffer.pop().unwrap();
                    StreamElement::Timestamped(element.item, element.timestamp)
                }
                _ => StreamElement::Watermark(self.last_watermark.take().unwrap()),
            }
        } else {
            // here we know that received_end must be true, otherwise we wouldn't have exited the loop
            match self.buffer.pop() {
                // pop remaining elements in the heap
                Some(element) => StreamElement::Timestamped(element.item, element.timestamp),
                None => StreamElement::End,
            }
        }
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> Reorder<{}>",
            self.prev.to_string(),
            std::any::type_name::<Out>(),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::reorder::Reorder;
    use crate::operator::{Data, Operator, StreamElement};
    use crate::scheduler::ExecutionMetadata;
    use std::collections::VecDeque;
    use std::time::Duration;

    #[derive(Clone)]
    struct FakeOperator<Out: Data> {
        buffer: VecDeque<StreamElement<Out>>,
    }

    impl<Out: Data> FakeOperator<Out> {
        fn new() -> Self {
            Self {
                buffer: Default::default(),
            }
        }

        fn push(&mut self, el: StreamElement<Out>) {
            self.buffer.push_back(el);
        }
    }

    impl<Out: Data> Operator<Out> for FakeOperator<Out> {
        fn setup(&mut self, _metadata: ExecutionMetadata) {}

        fn next(&mut self) -> StreamElement<Out> {
            self.buffer.pop_front().unwrap()
        }

        fn to_string(&self) -> String {
            format!("FakeOperator<{}>", std::any::type_name::<Out>())
        }
    }

    #[test]
    fn reorder() {
        let mut fake = FakeOperator::new();
        for i in &[1, 3, 2] {
            fake.push(StreamElement::Timestamped(*i, Duration::new(*i, 0)));
        }
        fake.push(StreamElement::Watermark(Duration::new(3, 0)));
        for i in &[6, 4, 5] {
            fake.push(StreamElement::Timestamped(*i, Duration::new(*i, 0)));
        }
        fake.push(StreamElement::End);

        let mut reorder = Reorder::new(fake);

        for i in 1..=6 {
            assert_eq!(
                reorder.next(),
                StreamElement::Timestamped(i, Duration::new(i, 0))
            );
            if i == 3 {
                assert_eq!(
                    reorder.next(),
                    StreamElement::Watermark(Duration::new(3, 0))
                );
            }
        }

        assert_eq!(reorder.next(), StreamElement::End);
    }
}
