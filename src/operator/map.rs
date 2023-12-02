use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Map<O: Send, F, Op>
where
    F: Fn(Op::Out) -> O + Send + Clone,
    Op: Operator,
{
    prev: Op,
    #[derivative(Debug = "ignore")]
    f: F,
    _out: PhantomData<O>,
}

impl<O: Send, F: Clone, Op: Clone> Clone for Map<O, F, Op>
where
    F: Fn(Op::Out) -> O + Send + Clone,
    Op: Operator,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            f: self.f.clone(),
            _out: self._out.clone(),
        }
    }
}

impl<O: Send, F, Op> Display for Map<O, F, Op>
where
    F: Fn(Op::Out) -> O + Send + Clone,
    Op: Operator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Map<{} -> {}>",
            self.prev,
            std::any::type_name::<Op::Out>(),
            std::any::type_name::<O>()
        )
    }
}

impl<O: Send, F, Op> Map<O, F, Op>
where
    F: Fn(Op::Out) -> O + Send + Clone,
    Op: Operator,
{
    pub(super) fn new(prev: Op, f: F) -> Self {
        Self {
            prev,
            f,
            _out: Default::default(),
        }
    }
}

impl<O: Send, F, Op> Operator for Map<O, F, Op>
where
    F: Fn(Op::Out) -> O + Send + Clone,
    Op: Operator,
{
    type Out = O;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<O> {
        self.prev.next().map(&self.f)
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<O, _>("Map"))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::operator::map::Map;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    #[cfg(feature = "timestamp")]
    fn map_stream() {
        let mut fake_operator = FakeOperator::new(0..10u8);
        for i in 0..10 {
            fake_operator.push(StreamElement::Timestamped(i, i as i64));
        }
        fake_operator.push(StreamElement::Watermark(100));

        let map = Map::new(fake_operator, |x| x.to_string());
        let map = Map::new(map, |x| x + "000");
        let mut map = Map::new(map, |x| u32::from_str(&x).unwrap());

        for i in 0..10 {
            let elem = map.next();
            assert_eq!(elem, StreamElement::Item(i * 1000));
        }
        for i in 0..10 {
            let elem = map.next();
            assert_eq!(elem, StreamElement::Timestamped(i * 1000, i as i64));
        }
        assert_eq!(map.next(), StreamElement::Watermark(100));
        assert_eq!(map.next(), StreamElement::Terminate);
    }
}
