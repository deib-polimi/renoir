use core::iter::{IntoIterator, Iterator};
use std::fmt::Display;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;
use crate::stream::KeyedItem;

type KeyedFlattenIter<Op> = <<<Op as Operator>::Out as KeyedItem>::Value as IntoIterator>::IntoIter;
type KeyedFlattenIterItem<Op> = <<<Op as Operator>::Out as KeyedItem>::Value as IntoIterator>::Item;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Flatten<Op>
where
    Op: Operator,
    Op::Out: IntoIterator,
    <Op::Out as IntoIterator>::Item: Send,
    <Op::Out as IntoIterator>::IntoIter: Send,
{
    prev: Op,
    // used to store elements that have not been returned by next() yet
    // buffer: VecDeque<StreamElement<NewOut>>,
    // Make an element of type `Out` iterable
    // This is used to make `Flatten` behave differently when applied to `Stream` or `KeyedStream`
    // Takes `Out` as input, returns an `Iterator` with items of type `NewOut`
    #[derivative(Debug = "ignore")]
    frontiter: Option<<Op::Out as IntoIterator>::IntoIter>,
    #[cfg(feature = "timestamp")]
    timestamp: Option<Timestamp>,
}

impl<Op: Clone> Clone for Flatten<Op>
where
    Op: Operator,
    Op::Out: IntoIterator,
    <Op::Out as IntoIterator>::Item: Send,
    <Op::Out as IntoIterator>::IntoIter: Send,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            frontiter: Default::default(),
            timestamp: Default::default(),
        }
    }
}

impl<Op> Display for Flatten<Op>
where
    Op: Operator,
    Op::Out: IntoIterator,
    <Op::Out as IntoIterator>::Item: Send,
    <Op::Out as IntoIterator>::IntoIter: Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Flatten<{} -> {}>",
            self.prev,
            std::any::type_name::<Op::Out>(),
            std::any::type_name::<<Op::Out as IntoIterator>::Item>()
        )
    }
}

impl<Op> Flatten<Op>
where
    Op: Operator,
    Op::Out: IntoIterator,
    <Op::Out as IntoIterator>::Item: Send,
    <Op::Out as IntoIterator>::IntoIter: Send,
{
    pub(super) fn new(prev: Op) -> Self {
        Self {
            prev,
            frontiter: None,
            #[cfg(feature = "timestamp")]
            timestamp: None,
        }
    }
}

impl<Op> Operator for Flatten<Op>
where
    Op: Operator,
    Op::Out: IntoIterator,
    <Op::Out as IntoIterator>::Item: Send,
    <Op::Out as IntoIterator>::IntoIter: Send,
{
    type Out = <Op::Out as IntoIterator>::Item;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Self::Out> {
        loop {
            if let Some(ref mut inner) = self.frontiter {
                match inner.next() {
                    None => self.frontiter = None,
                    #[cfg(not(feature = "timestamp"))]
                    Some(item) => return StreamElement::Item(item),
                    #[cfg(feature = "timestamp")]
                    Some(item) => match self.timestamp {
                        None => return StreamElement::Item(item),
                        Some(ts) => return StreamElement::Timestamped(item, ts),
                    },
                }
            }
            match self.prev.next() {
                #[cfg(not(feature = "timestamp"))]
                StreamElement::Item(inner) | StreamElement::Timestamped(inner, _) => {
                    self.frontiter = Some(inner.into_iter());
                }

                #[cfg(feature = "timestamp")]
                StreamElement::Item(inner) => {
                    self.frontiter = Some(inner.into_iter());
                    self.timestamp = None;
                }
                #[cfg(feature = "timestamp")]
                StreamElement::Timestamped(inner, ts) => {
                    self.frontiter = Some(inner.into_iter());
                    self.timestamp = Some(ts);
                }
                StreamElement::Watermark(ts) => return StreamElement::Watermark(ts),
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Terminate => return StreamElement::Terminate,
                StreamElement::FlushAndRestart => return StreamElement::FlushAndRestart,
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Self::Out, _>("Flatten"))
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct KeyedFlatten<Op>
where
    Op: Operator,
    Op::Out: KeyedItem,
    <Op::Out as KeyedItem>::Value: IntoIterator,
    KeyedFlattenIterItem<Op>: Send,
    KeyedFlattenIter<Op>: Send,
{
    prev: Op,
    // used to store elements that have not been returned by next() yet
    // buffer: VecDeque<StreamElement<NewOut>>,
    // Make an element of type `Out` iterable
    // This is used to make `Flatten` behave differently when applied to `Stream` or `KeyedStream`
    // Takes `Out` as input, returns an `Iterator` with items of type `NewOut`
    #[derivative(Debug = "ignore")]
    frontiter: Option<(<Op::Out as KeyedItem>::Key, KeyedFlattenIter<Op>)>,
    timestamp: Option<Timestamp>,
}

impl<Op: Clone> Clone for KeyedFlatten<Op>
where
    Op: Operator,
    Op::Out: KeyedItem,
    <Op::Out as KeyedItem>::Value: IntoIterator,
    KeyedFlattenIterItem<Op>: Send,
    KeyedFlattenIter<Op>: Send,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            frontiter: Default::default(),
            timestamp: Default::default(),
        }
    }
}

impl<Op> Display for KeyedFlatten<Op>
where
    Op: Operator,
    Op::Out: KeyedItem,
    <Op::Out as KeyedItem>::Value: IntoIterator,
    KeyedFlattenIterItem<Op>: Send,
    KeyedFlattenIter<Op>: Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> KeyedFlatten<{} -> {}>",
            self.prev,
            std::any::type_name::<Op::Out>(),
            std::any::type_name::<(<Op::Out as KeyedItem>::Key, KeyedFlattenIterItem<Op>)>()
        )
    }
}

impl<Op> KeyedFlatten<Op>
where
    Op: Operator,
    Op::Out: KeyedItem,
    <Op::Out as KeyedItem>::Value: IntoIterator,
    KeyedFlattenIterItem<Op>: Send,
    KeyedFlattenIter<Op>: Send,
{
    pub(super) fn new(prev: Op) -> Self {
        Self {
            prev,
            frontiter: None,
            timestamp: None,
        }
    }
}

impl<Op> Operator for KeyedFlatten<Op>
where
    Op: Operator,
    Op::Out: KeyedItem,
    <Op::Out as KeyedItem>::Value: IntoIterator,
    KeyedFlattenIterItem<Op>: Send,
    KeyedFlattenIter<Op>: Send,
{
    type Out = (<Op::Out as KeyedItem>::Key, KeyedFlattenIterItem<Op>);

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Self::Out> {
        loop {
            if let Some((ref key, ref mut inner)) = self.frontiter {
                match inner.next() {
                    None => self.frontiter = None,
                    #[cfg(not(feature = "timestamp"))]
                    Some(item) => return StreamElement::Item((key.clone(), item)),
                    #[cfg(feature = "timestamp")]
                    Some(item) => match self.timestamp {
                        None => return StreamElement::Item((key.clone(), item)),
                        Some(ts) => return StreamElement::Timestamped((key.clone(), item), ts),
                    },
                }
            }
            match self.prev.next() {
                #[cfg(not(feature = "timestamp"))]
                StreamElement::Item((key, inner)) | StreamElement::Timestamped((key, inner), _) => {
                    self.frontiter = Some((key, inner.into_iter()));
                }
                #[cfg(feature = "timestamp")]
                StreamElement::Item(kv) => {
                    let (key, value) = kv.into_kv();
                    self.frontiter = Some((key, value.into_iter()));
                    self.timestamp = None;
                }
                #[cfg(feature = "timestamp")]
                StreamElement::Timestamped(kv, ts) => {
                    let (key, value) = kv.into_kv();
                    self.frontiter = Some((key, value.into_iter()));
                    self.timestamp = Some(ts);
                }
                StreamElement::Watermark(ts) => return StreamElement::Watermark(ts),
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Terminate => return StreamElement::Terminate,
                StreamElement::FlushAndRestart => return StreamElement::FlushAndRestart,
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Self::Out, _>("KeyedFlatten"))
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::flatten::Flatten;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn test_flatten_no_timestamps() {
        let fake_operator = FakeOperator::new(
            vec![
                vec![],
                vec![0, 1, 2, 3],
                vec![],
                vec![4],
                vec![5, 6, 7],
                vec![],
            ]
            .into_iter(),
        );
        let mut flatten = Flatten::new(fake_operator);
        for i in 0..=7 {
            assert_eq!(flatten.next(), StreamElement::Item(i));
        }
        assert_eq!(flatten.next(), StreamElement::Terminate);
    }

    #[test]
    #[cfg(feature = "timestamp")]
    fn test_flatten_timestamped() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Timestamped(vec![], 0));
        fake_operator.push(StreamElement::Timestamped(vec![1, 2, 3], 1));
        fake_operator.push(StreamElement::Timestamped(vec![4], 2));
        fake_operator.push(StreamElement::Timestamped(vec![], 3));
        fake_operator.push(StreamElement::Watermark(4));

        let mut flatten = Flatten::new(fake_operator);

        assert_eq!(flatten.next(), StreamElement::Timestamped(1, 1));
        assert_eq!(flatten.next(), StreamElement::Timestamped(2, 1));
        assert_eq!(flatten.next(), StreamElement::Timestamped(3, 1));
        assert_eq!(flatten.next(), StreamElement::Timestamped(4, 2));
        assert_eq!(flatten.next(), StreamElement::Watermark(4));
        assert_eq!(flatten.next(), StreamElement::Terminate);
    }
}
