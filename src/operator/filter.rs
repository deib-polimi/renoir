use std::fmt::Display;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

#[derive(Clone)]
pub struct Filter<Op, Predicate>
where
    Predicate: Fn(&Op::Out) -> bool + Send + Clone + 'static,
    Op: Operator,
{
    prev: Op,
    predicate: Predicate,
}

impl<Op, Predicate> Display for Filter<Op, Predicate>
where
    Predicate: Fn(&Op::Out) -> bool + Send + Clone + 'static,
    Op: Operator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Filter<{}>",
            self.prev,
            std::any::type_name::<Op::Out>()
        )
    }
}

impl<Op, Predicate> Filter<Op, Predicate>
where
    Predicate: Fn(&Op::Out) -> bool + Clone + Send + 'static,
    Op: Operator,
{
    pub(super) fn new(prev: Op, predicate: Predicate) -> Self {
        Self { prev, predicate }
    }
}

impl<Op, Predicate> Operator for Filter<Op, Predicate>
where
    Predicate: Fn(&Op::Out) -> bool + Clone + Send + 'static,
    Op: Operator,
{
    type Out = Op::Out;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Op::Out> {
        loop {
            match self.prev.next() {
                StreamElement::Item(ref item) | StreamElement::Timestamped(ref item, _)
                    if !(self.predicate)(item) => {} // Skip
                element => return element,
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Op::Out, _>("Filter"))
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::filter::Filter;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn test_filter() {
        let fake_operator = FakeOperator::new(0..10u8);
        let mut filter = Filter::new(fake_operator, |n| n % 2 == 0);

        assert_eq!(filter.next(), StreamElement::Item(0));
        assert_eq!(filter.next(), StreamElement::Item(2));
        assert_eq!(filter.next(), StreamElement::Item(4));
        assert_eq!(filter.next(), StreamElement::Item(6));
        assert_eq!(filter.next(), StreamElement::Item(8));
        assert_eq!(filter.next(), StreamElement::Terminate);
    }
}
