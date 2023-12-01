use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

#[derive(Clone)]
pub struct Filter<Out: Data, PreviousOperator, Predicate>
where
    Predicate: Fn(&Out) -> bool + Send + Clone + 'static,
    PreviousOperator: Operator<Out = Out> + 'static,
{
    prev: PreviousOperator,
    predicate: Predicate,
    _out: PhantomData<Out>,
}

impl<Out: Data, PreviousOperator, Predicate> Display for Filter<Out, PreviousOperator, Predicate>
where
    Predicate: Fn(&Out) -> bool + Send + Clone + 'static,
    PreviousOperator: Operator<Out = Out> + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Filter<{}>",
            self.prev,
            std::any::type_name::<Out>()
        )
    }
}

impl<Out: Data, PreviousOperator, Predicate> Filter<Out, PreviousOperator, Predicate>
where
    Predicate: Fn(&Out) -> bool + Clone + Send + 'static,
    PreviousOperator: Operator<Out = Out> + 'static,
{
    pub(super) fn new(prev: PreviousOperator, predicate: Predicate) -> Self {
        Self {
            prev,
            predicate,
            _out: Default::default(),
        }
    }
}

impl<Out: Data, PreviousOperator, Predicate> Operator for Filter<Out, PreviousOperator, Predicate>
where
    Predicate: Fn(&Out) -> bool + Clone + Send + 'static,
    PreviousOperator: Operator<Out = Out> + 'static,
{
    type Out = Out;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Out> {
        loop {
            match self.prev.next() {
                StreamElement::Item(ref item) | StreamElement::Timestamped(ref item, _)
                    if !(self.predicate)(item) => {}
                element => return element,
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Out, _>("Filter"))
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
