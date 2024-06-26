use std::fmt::Display;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::DataKey;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct KeyBy<Key: DataKey, Keyer, Op>
where
    Keyer: Fn(&Op::Out) -> Key + Send + Clone,
    Op: Operator,
{
    prev: Op,
    #[derivative(Debug = "ignore")]
    keyer: Keyer,
}

impl<Key: DataKey, Keyer: Clone, Op: Clone> Clone for KeyBy<Key, Keyer, Op>
where
    Keyer: Fn(&Op::Out) -> Key + Send + Clone,
    Op: Operator,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            keyer: self.keyer.clone(),
        }
    }
}

impl<Key: DataKey, Keyer, Op> Display for KeyBy<Key, Keyer, Op>
where
    Keyer: Fn(&Op::Out) -> Key + Send + Clone,
    Op: Operator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> KeyBy<{}>",
            self.prev,
            std::any::type_name::<Key>(),
        )
    }
}

impl<Key: DataKey, Keyer, Op> KeyBy<Key, Keyer, Op>
where
    Keyer: Fn(&Op::Out) -> Key + Send + Clone,
    Op: Operator,
{
    pub fn new(prev: Op, keyer: Keyer) -> Self {
        Self { prev, keyer }
    }
}

impl<Key: DataKey, Keyer, Op> Operator for KeyBy<Key, Keyer, Op>
where
    Keyer: Fn(&Op::Out) -> Key + Send + Clone,
    Op: Operator,
{
    type Out = (Key, Op::Out);

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Self::Out> {
        match self.prev.next() {
            StreamElement::Item(t) => StreamElement::Item(((self.keyer)(&t), t)),
            StreamElement::Timestamped(t, ts) => {
                StreamElement::Timestamped(((self.keyer)(&t), t), ts)
            }
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::Terminate => StreamElement::Terminate,
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
            StreamElement::FlushBatch => StreamElement::FlushBatch,
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Self::Out, _>("KeyBy"))
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::key_by::KeyBy;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn test_key_by() {
        let fake_operator = FakeOperator::new(0..10u8);
        let mut key_by = KeyBy::new(fake_operator, |&n| n);

        for i in 0..10u8 {
            match key_by.next() {
                StreamElement::Item((a, b)) => {
                    assert_eq!(a, i);
                    assert_eq!(b, i);
                }
                item => panic!("Expected StreamElement::Item, got {}", item.variant_str()),
            }
        }
        assert_eq!(key_by.next(), StreamElement::Terminate);
    }
}
