use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, DataKey, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyedStream, Stream};

#[derive(Clone)]
pub struct AddTimestamp<Out: Data, TimestampGen, WatermarkGen, OperatorChain>
where
    OperatorChain: Operator<Out>,
    TimestampGen: FnMut(&Out) -> Timestamp + Clone + Send + 'static,
    WatermarkGen: FnMut(&Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
{
    prev: OperatorChain,
    timestamp_gen: TimestampGen,
    watermark_gen: WatermarkGen,
    pending_watermark: Option<Timestamp>,
    _out: PhantomData<Out>,
}

impl<Out: Data, TimestampGen, WatermarkGen, OperatorChain> Display
    for AddTimestamp<Out, TimestampGen, WatermarkGen, OperatorChain>
where
    OperatorChain: Operator<Out>,
    TimestampGen: FnMut(&Out) -> Timestamp + Clone + Send + 'static,
    WatermarkGen: FnMut(&Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> AddTimestamp", self.prev)
    }
}

impl<Out: Data, TimestampGen, WatermarkGen, OperatorChain>
    AddTimestamp<Out, TimestampGen, WatermarkGen, OperatorChain>
where
    OperatorChain: Operator<Out>,
    TimestampGen: FnMut(&Out) -> Timestamp + Clone + Send + 'static,
    WatermarkGen: FnMut(&Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
{
    fn new(prev: OperatorChain, timestamp_gen: TimestampGen, watermark_gen: WatermarkGen) -> Self {
        Self {
            prev,
            timestamp_gen,
            watermark_gen,
            pending_watermark: None,
            _out: Default::default(),
        }
    }
}

impl<Out: Data, TimestampGen, WatermarkGen, OperatorChain> Operator<Out>
    for AddTimestamp<Out, TimestampGen, WatermarkGen, OperatorChain>
where
    OperatorChain: Operator<Out>,
    TimestampGen: FnMut(&Out) -> Timestamp + Clone + Send + 'static,
    WatermarkGen: FnMut(&Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Out> {
        if let Some(ts) = self.pending_watermark.take() {
            return StreamElement::Watermark(ts);
        }

        let elem = self.prev.next();
        match elem {
            StreamElement::Item(item) => {
                let ts = (self.timestamp_gen)(&item);
                let watermark = (self.watermark_gen)(&item, &ts);
                self.pending_watermark = watermark;
                StreamElement::Timestamped(item, ts)
            }
            StreamElement::FlushAndRestart
            | StreamElement::FlushBatch
            | StreamElement::Terminate => elem,
            _ => panic!("AddTimestamp received invalid variant: {}", elem.variant()),
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Out, _>("AddTimestamp"))
    }
}

#[derive(Clone)]
pub struct DropTimestamp<Out: Data, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    _out: PhantomData<Out>,
}

impl<Out: Data, OperatorChain> Display for DropTimestamp<Out, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> DropTimestamp", self.prev)
    }
}

impl<Out: Data, OperatorChain> DropTimestamp<Out, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    fn new(prev: OperatorChain) -> Self {
        Self {
            prev,
            _out: Default::default(),
        }
    }
}

impl<Out: Data, OperatorChain> Operator<Out> for DropTimestamp<Out, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Out> {
        loop {
            match self.prev.next() {
                StreamElement::Watermark(_) => continue,
                StreamElement::Timestamped(item, _) => return StreamElement::Item(item),
                el => return el,
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Out, _>("DropTimestamp"))
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Given a stream without timestamps nor watermarks, tag each item with a timestamp and insert
    /// watermarks.
    ///
    /// The two functions given to this operator are the following:
    /// - `timestamp_gen` returns the timestamp assigned to the provided element of the stream
    /// - `watermark_gen` returns an optional watermark to add after the provided element
    ///
    /// Note that the two functions **must** follow the watermark semantics.
    /// TODO: link to watermark semantics
    ///
    /// ## Example
    ///
    /// In this example the stream contains the integers from 0 to 9, each will be tagged with a
    /// timestamp with the value of the item as milliseconds, and after each even number a watermark
    /// will be inserted.
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// use noir::operator::Timestamp;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    ///
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// s.add_timestamps(
    ///     |&n| n,
    ///     |&n, &ts| if n % 2 == 0 { Some(ts) } else { None }
    /// );
    /// ```
    pub fn add_timestamps<TimestampGen, WatermarkGen>(
        self,
        timestamp_gen: TimestampGen,
        watermark_gen: WatermarkGen,
    ) -> Stream<Out, impl Operator<Out>>
    where
        TimestampGen: FnMut(&Out) -> Timestamp + Clone + Send + 'static,
        WatermarkGen: FnMut(&Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
    {
        self.add_operator(|prev| AddTimestamp::new(prev, timestamp_gen, watermark_gen))
    }

    pub fn drop_timestamps(self) -> Stream<Out, impl Operator<Out>> {
        self.add_operator(|prev| DropTimestamp::new(prev))
    }
}

impl<Key, Out, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    Key: DataKey,
    Out: Data,
    OperatorChain: Operator<(Key, Out)> + 'static,
{
    /// Given a keyed stream without timestamps nor watermarks, tag each item with a timestamp and insert
    /// watermarks.
    ///
    /// The two functions given to this operator are the following:
    /// - `timestamp_gen` returns the timestamp assigned to the provided element of the stream
    /// - `watermark_gen` returns an optional watermark to add after the provided element
    ///
    /// Note that the two functions **must** follow the watermark semantics.
    /// TODO: link to watermark semantics
    ///
    /// ## Example
    ///
    /// In this example the stream contains the integers from 0 to 9 and group them by parity, each will be tagged with a
    /// timestamp with the value of the item as milliseconds, and after each even number a watermark
    /// will be inserted.
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// use noir::operator::Timestamp;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    ///
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// s
    ///     .group_by(|i| i % 2)
    ///     .add_timestamps(
    ///     |&(_k, n)| n,
    ///     |&(_k, n), &ts| if n % 2 == 0 { Some(ts) } else { None }
    /// );
    /// ```
    pub fn add_timestamps<TimestampGen, WatermarkGen>(
        self,
        timestamp_gen: TimestampGen,
        watermark_gen: WatermarkGen,
    ) -> KeyedStream<Key, Out, impl Operator<(Key, Out)>>
    where
        TimestampGen: FnMut(&(Key, Out)) -> Timestamp + Clone + Send + 'static,
        WatermarkGen: FnMut(&(Key, Out), &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
    {
        self.add_operator(|prev| AddTimestamp::new(prev, timestamp_gen, watermark_gen))
    }

    pub fn drop_timestamps(self) -> KeyedStream<Key, Out, impl Operator<(Key, Out)>> {
        self.add_operator(|prev| DropTimestamp::new(prev))
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::add_timestamps::AddTimestamp;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn add_timestamps() {
        let fake_operator = FakeOperator::new(0..10u64);

        let mut oper = AddTimestamp::new(
            fake_operator,
            |n| *n as i64,
            |n, ts| {
                if n % 2 == 0 {
                    Some(*ts)
                } else {
                    None
                }
            },
        );

        for i in 0..5u64 {
            let t = i * 2;
            assert_eq!(oper.next(), StreamElement::Timestamped(t, t as i64));
            assert_eq!(oper.next(), StreamElement::Watermark(t as i64));
            assert_eq!(oper.next(), StreamElement::Timestamped(t + 1, t as i64 + 1));
        }
        assert_eq!(oper.next(), StreamElement::Terminate);
    }
}
