use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;
use crate::stream::KeyedItem;
use core::iter::{IntoIterator, Iterator};
use std::fmt::Display;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct FlatMap<It, F, Op>
where
    Op: Operator,
    It: IntoIterator,
    It::IntoIter: Send + 'static,
    It::Item: Send,
    F: Fn(Op::Out) -> It + Clone + Send + 'static,
{
    prev: Op,
    f: F,
    // used to store elements that have not been returned by next() yet
    // buffer: VecDeque<StreamElement<NewOut>>,
    // Make an element of type `Out` iterable
    // This is used to make `FlatMap` behave differently when applied to `Stream` or `KeyedStream`
    // Takes `Out` as input, returns an `Iterator` with items of type `NewOut`
    #[derivative(Debug = "ignore")]
    frontiter: Option<<It as IntoIterator>::IntoIter>,
    #[cfg(feature = "timestamp")]
    timestamp: Option<Timestamp>,
}

impl<It, F, Op> Clone for FlatMap<It, F, Op>
where
    Op: Operator,
    It: IntoIterator,
    It::IntoIter: Send + 'static,
    It::Item: Send,
    F: Fn(Op::Out) -> It + Clone + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            f: self.f.clone(),
            frontiter: None,
            #[cfg(feature = "timestamp")]
            timestamp: self.timestamp,
        }
    }
}

impl<It, F, Op> Display for FlatMap<It, F, Op>
where
    Op: Operator,
    It: IntoIterator,
    It::IntoIter: Send + 'static,
    It::Item: Send,
    F: Fn(Op::Out) -> It + Clone + Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> FlatMap<{} -> {}>",
            self.prev,
            std::any::type_name::<Op::Out>(),
            std::any::type_name::<It::Item>()
        )
    }
}

impl<It, F, Op> FlatMap<It, F, Op>
where
    Op: Operator,
    It: IntoIterator,
    It::IntoIter: Send + 'static,
    It::Item: Send,
    F: Fn(Op::Out) -> It + Clone + Send + 'static,
{
    pub(super) fn new(prev: Op, f: F) -> Self {
        Self {
            prev,
            f,
            frontiter: None,
            #[cfg(feature = "timestamp")]
            timestamp: None,
        }
    }
}

impl<It, F, Op> Operator for FlatMap<It, F, Op>
where
    Op: Operator,
    It: IntoIterator,
    It::IntoIter: Send + 'static,
    It::Item: Send,
    F: Fn(Op::Out) -> It + Clone + Send + 'static,
{
    type Out = It::Item;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

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
                    self.frontiter = Some((self.f)(inner).into_iter());
                }

                #[cfg(feature = "timestamp")]
                StreamElement::Item(inner) => {
                    self.frontiter = Some((self.f)(inner).into_iter());
                    self.timestamp = None;
                }
                #[cfg(feature = "timestamp")]
                StreamElement::Timestamped(inner, ts) => {
                    self.frontiter = Some((self.f)(inner).into_iter());
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
            .add_operator(OperatorStructure::new::<It::Item, _>("FlatMap"))
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct KeyedFlatMap<It, F, Op>
where
    Op: Operator,
    Op::Out: KeyedItem,
    It: IntoIterator,
    It::IntoIter: Send + 'static,
    It::Item: Send,
    F: Fn(Op::Out) -> It + Clone + Send + 'static,
{
    prev: Op,
    f: F,
    // used to store elements that have not been returned by next() yet
    // buffer: VecDeque<StreamElement<NewOut>>,
    // Make an element of type `Out` iterable
    // This is used to make `FlatMap` behave differently when applied to `Stream` or `KeyedStream`
    // Takes `Out` as input, returns an `Iterator` with items of type `NewOut`
    #[derivative(Debug = "ignore")]
    frontiter: Option<(<Op::Out as KeyedItem>::Key, It::IntoIter)>,
    timestamp: Option<Timestamp>,
}

impl<It, F, Op> Clone for KeyedFlatMap<It, F, Op>
where
    Op: Operator,
    Op::Out: KeyedItem,
    It: IntoIterator,
    It::IntoIter: Send + 'static,
    It::Item: Send,
    F: Fn(Op::Out) -> It + Clone + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            f: self.f.clone(),
            frontiter: None,
            timestamp: self.timestamp,
        }
    }
}

impl<It, F, Op> Display for KeyedFlatMap<It, F, Op>
where
    Op: Operator,
    Op::Out: KeyedItem,
    It: IntoIterator,
    It::IntoIter: Send + 'static,
    It::Item: Send,
    F: Fn(Op::Out) -> It + Clone + Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> KeyedFlatMap<{} -> {}>",
            self.prev,
            std::any::type_name::<Op::Out>(),
            std::any::type_name::<It::Item>()
        )
    }
}

impl<It, F, Op> KeyedFlatMap<It, F, Op>
where
    Op: Operator,
    Op::Out: KeyedItem,
    It: IntoIterator,
    It::IntoIter: Send + 'static,
    It::Item: Send,
    F: Fn(Op::Out) -> It + Clone + Send + 'static,
{
    pub(super) fn new(prev: Op, f: F) -> Self {
        Self {
            prev,
            f,
            frontiter: None,
            timestamp: None,
        }
    }
}

impl<It, F, Op> Operator for KeyedFlatMap<It, F, Op>
where
    Op: Operator,
    Op::Out: KeyedItem,
    It: IntoIterator,
    It::IntoIter: Send + 'static,
    It::Item: Send,
    F: Fn(Op::Out) -> It + Clone + Send + 'static,
{
    type Out = (<Op::Out as KeyedItem>::Key, It::Item);

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
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
                    let iter = (self.f)((&key, inner)).into_iter();
                    self.frontiter = Some((key, iter));
                }
                #[cfg(feature = "timestamp")]
                StreamElement::Item(kv) => {
                    let key = kv.key().clone();
                    let iter = (self.f)(kv).into_iter();
                    self.frontiter = Some((key, iter));
                    self.timestamp = None;
                }
                #[cfg(feature = "timestamp")]
                StreamElement::Timestamped(kv, ts) => {
                    let key = kv.key().clone();
                    let iter = (self.f)(kv).into_iter();
                    self.frontiter = Some((key, iter));
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
            .add_operator(OperatorStructure::new::<Self::Out, _>("KeyedFlatMap"))
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::flat_map::FlatMap;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn test_flat_map_no_timestamps() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Item(3));
        fake_operator.push(StreamElement::Item(1));
        fake_operator.push(StreamElement::Item(2));

        let mut flat_map = FlatMap::new(fake_operator, |i| 0..i);

        assert_eq!(flat_map.next(), StreamElement::Item(0));
        assert_eq!(flat_map.next(), StreamElement::Item(1));
        assert_eq!(flat_map.next(), StreamElement::Item(2));
        assert_eq!(flat_map.next(), StreamElement::Item(0));
        assert_eq!(flat_map.next(), StreamElement::Item(0));
        assert_eq!(flat_map.next(), StreamElement::Item(1));
        assert_eq!(flat_map.next(), StreamElement::Terminate);
    }

    #[test]
    #[cfg(feature = "timestamp")]
    fn test_flat_map_timestamped() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Timestamped(3, 0));
        fake_operator.push(StreamElement::Timestamped(1, 1));
        fake_operator.push(StreamElement::Timestamped(2, 3));
        fake_operator.push(StreamElement::Watermark(4));

        let mut flat_map = FlatMap::new(fake_operator, |i| 0..i);

        assert_eq!(flat_map.next(), StreamElement::Timestamped(0, 0));
        assert_eq!(flat_map.next(), StreamElement::Timestamped(1, 0));
        assert_eq!(flat_map.next(), StreamElement::Timestamped(2, 0));
        assert_eq!(flat_map.next(), StreamElement::Timestamped(0, 1));
        assert_eq!(flat_map.next(), StreamElement::Timestamped(0, 3));
        assert_eq!(flat_map.next(), StreamElement::Timestamped(1, 3));
        assert_eq!(flat_map.next(), StreamElement::Watermark(4));
        assert_eq!(flat_map.next(), StreamElement::Terminate);
    }
}
