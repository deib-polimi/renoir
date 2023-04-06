use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, DataKey};
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct KeyBy<Key: DataKey, Out: Data, Keyer, OperatorChain>
where
    Keyer: Fn(&Out) -> Key + Send + Clone,
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    #[derivative(Debug = "ignore")]
    keyer: Keyer,
    _key: PhantomData<Key>,
    _out: PhantomData<Out>,
}

impl<Key: DataKey, Out: Data, Keyer, OperatorChain> Display
    for KeyBy<Key, Out, Keyer, OperatorChain>
where
    Keyer: Fn(&Out) -> Key + Send + Clone,
    OperatorChain: Operator<Out>,
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

impl<Key: DataKey, Out: Data, Keyer, OperatorChain> KeyBy<Key, Out, Keyer, OperatorChain>
where
    Keyer: Fn(&Out) -> Key + Send + Clone,
    OperatorChain: Operator<Out>,
{
    pub fn new(prev: OperatorChain, keyer: Keyer) -> Self {
        Self {
            prev,
            keyer,
            _key: Default::default(),
            _out: Default::default(),
        }
    }
}

impl<Key: DataKey, Out: Data, Keyer, OperatorChain> Operator<(Key, Out)>
    for KeyBy<Key, Out, Keyer, OperatorChain>
where
    Keyer: Fn(&Out) -> Key + Send + Clone,
    OperatorChain: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<(Key, Out)> {
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
            .add_operator(OperatorStructure::new::<(Key, Out), _>("KeyBy"))
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
                item => panic!("Expected StreamElement::Item, got {}", item.variant()),
            }
        }
        assert_eq!(key_by.next(), StreamElement::Terminate);
    }
}
