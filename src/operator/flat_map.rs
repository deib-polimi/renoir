use core::iter::{IntoIterator, Iterator};
use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, DataKey, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct FlatMap<In, Out, Iter, F, PreviousOperators>
where
    In: Data,
    Out: Data,
    PreviousOperators: Operator<In>,
    Iter: IntoIterator<Item = Out>,
    <Iter as IntoIterator>::IntoIter: Send + 'static,
    F: Fn(In) -> Iter + Clone + Send + 'static,
{
    prev: PreviousOperators,
    f: F,
    // used to store elements that have not been returned by next() yet
    // buffer: VecDeque<StreamElement<NewOut>>,
    // Make an element of type `Out` iterable
    // This is used to make `FlatMap` behave differently when applied to `Stream` or `KeyedStream`
    // Takes `Out` as input, returns an `Iterator` with items of type `NewOut`
    #[derivative(Debug = "ignore")]
    frontiter: Option<<Iter as IntoIterator>::IntoIter>,
    #[cfg(feature = "timestamp")]
    timestamp: Option<Timestamp>,
    _out: PhantomData<In>,
    _iter_out: PhantomData<Out>,
}

impl<In: Clone, Out: Clone, Iter, F: Clone, PreviousOperators: Clone> Clone
    for FlatMap<In, Out, Iter, F, PreviousOperators>
where
    In: Data,
    Out: Data,
    PreviousOperators: Operator<In>,
    Iter: IntoIterator<Item = Out>,
    <Iter as IntoIterator>::IntoIter: Send + 'static,
    F: Fn(In) -> Iter + Clone + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            f: self.f.clone(),
            frontiter: None,
            timestamp: self.timestamp,
            _out: self._out,
            _iter_out: self._iter_out,
        }
    }
}

impl<In, Out, Iter, F, PreviousOperators> Display for FlatMap<In, Out, Iter, F, PreviousOperators>
where
    In: Data,
    Out: Data,
    PreviousOperators: Operator<In>,
    Iter: IntoIterator<Item = Out>,
    <Iter as IntoIterator>::IntoIter: Send + 'static,
    F: Fn(In) -> Iter + Clone + Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> FlatMap<{} -> {}>",
            self.prev,
            std::any::type_name::<In>(),
            std::any::type_name::<Out>()
        )
    }
}

impl<In, Out, Iter, F, PreviousOperators> FlatMap<In, Out, Iter, F, PreviousOperators>
where
    In: Data,
    Out: Data,
    PreviousOperators: Operator<In>,
    Iter: IntoIterator<Item = Out>,
    <Iter as IntoIterator>::IntoIter: Send + 'static,
    F: Fn(In) -> Iter + Clone + Send + 'static,
{
    pub(super) fn new(prev: PreviousOperators, f: F) -> Self {
        Self {
            prev,
            f,
            frontiter: None,
            #[cfg(feature = "timestamp")]
            timestamp: None,
            _out: Default::default(),
            _iter_out: Default::default(),
        }
    }
}

impl<In, Out, Iter, F, PreviousOperators> Operator<Out>
    for FlatMap<In, Out, Iter, F, PreviousOperators>
where
    In: Data,
    Out: Data,
    PreviousOperators: Operator<In>,
    Iter: IntoIterator<Item = Out>,
    <Iter as IntoIterator>::IntoIter: Send + 'static,
    F: Fn(In) -> Iter + Clone + Send + 'static,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Out> {
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
            .add_operator(OperatorStructure::new::<Out, _>("FlatMap"))
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct KeyedFlatMap<Key, In, Out, Iter, F, PreviousOperators>
where
    In: Data,
    Key: DataKey,
    Out: Data,
    PreviousOperators: Operator<(Key, In)>,
    Iter: IntoIterator<Item = Out>,
    <Iter as IntoIterator>::IntoIter: Send + 'static,
    F: Fn((&Key, In)) -> Iter + Clone + Send + 'static,
{
    prev: PreviousOperators,
    f: F,
    // used to store elements that have not been returned by next() yet
    // buffer: VecDeque<StreamElement<NewOut>>,
    // Make an element of type `Out` iterable
    // This is used to make `FlatMap` behave differently when applied to `Stream` or `KeyedStream`
    // Takes `Out` as input, returns an `Iterator` with items of type `NewOut`
    #[derivative(Debug = "ignore")]
    frontiter: Option<(Key, Iter::IntoIter)>,
    timestamp: Option<Timestamp>,
    _key: PhantomData<Key>,
    _in: PhantomData<In>,
    _iter_out: PhantomData<Out>,
}

impl<Key: Clone, In: Clone, Out: Clone, Iter, F: Clone, PreviousOperators: Clone> Clone
    for KeyedFlatMap<Key, In, Out, Iter, F, PreviousOperators>
where
    In: Data,
    Key: DataKey,
    Out: Data,
    PreviousOperators: Operator<(Key, In)>,
    Iter: IntoIterator<Item = Out>,
    <Iter as IntoIterator>::IntoIter: Send + 'static,
    F: Fn((&Key, In)) -> Iter + Clone + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            f: self.f.clone(),
            frontiter: None,
            timestamp: self.timestamp,
            _key: self._key,
            _in: self._in,
            _iter_out: self._iter_out,
        }
    }
}

impl<Key, In, Out, Iter, F, PreviousOperators> Display
    for KeyedFlatMap<Key, In, Out, Iter, F, PreviousOperators>
where
    In: Data,
    Key: DataKey,
    Out: Data,
    PreviousOperators: Operator<(Key, In)>,
    Iter: IntoIterator<Item = Out>,
    <Iter as IntoIterator>::IntoIter: Send + 'static,
    F: Fn((&Key, In)) -> Iter + Clone + Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> KeyedFlatMap<{} -> {}>",
            self.prev,
            std::any::type_name::<In>(),
            std::any::type_name::<Out>()
        )
    }
}

impl<Key, In, Out, Iter, F, PreviousOperators>
    KeyedFlatMap<Key, In, Out, Iter, F, PreviousOperators>
where
    In: Data,
    Key: DataKey,
    Out: Data,
    PreviousOperators: Operator<(Key, In)>,
    Iter: IntoIterator<Item = Out>,
    <Iter as IntoIterator>::IntoIter: Send + 'static,
    F: Fn((&Key, In)) -> Iter + Clone + Send + 'static,
{
    pub(super) fn new(prev: PreviousOperators, f: F) -> Self {
        Self {
            prev,
            f,
            frontiter: None,
            timestamp: None,
            _key: Default::default(),
            _in: Default::default(),
            _iter_out: Default::default(),
        }
    }
}

impl<Key, In, Out, Iter, F, PreviousOperators> Operator<(Key, Out)>
    for KeyedFlatMap<Key, In, Out, Iter, F, PreviousOperators>
where
    In: Data,
    Key: DataKey,
    Out: Data,
    PreviousOperators: Operator<(Key, In)>,
    Iter: IntoIterator<Item = Out>,
    <Iter as IntoIterator>::IntoIter: Send + 'static,
    F: Fn((&Key, In)) -> Iter + Clone + Send + 'static,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<(Key, Out)> {
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
                StreamElement::Item((key, inner)) => {
                    let iter = (self.f)((&key, inner)).into_iter();
                    self.frontiter = Some((key, iter));
                    self.timestamp = None;
                }
                #[cfg(feature = "timestamp")]
                StreamElement::Timestamped((key, inner), ts) => {
                    let iter = (self.f)((&key, inner)).into_iter();
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
            .add_operator(OperatorStructure::new::<Out, _>("KeyedFlatMap"))
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
