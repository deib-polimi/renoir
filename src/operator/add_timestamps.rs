use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;
use crate::stream::Stream;

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
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

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

    fn to_string(&self) -> String {
        format!("{} -> AddTimestamp", self.prev.to_string())
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Out, _>("AddTimestamp"))
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
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::IteratorSource;
    /// use rstream::operator::Timestamp;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    ///
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// s.add_timestamps(
    ///     |&n| Timestamp::from_millis(n),
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
}

#[cfg(test)]
mod tests {
    use crate::operator::add_timestamps::AddTimestamp;
    use crate::operator::{Operator, StreamElement, Timestamp};
    use crate::test::FakeOperator;

    #[test]
    fn add_timestamps() {
        let fake_operator = FakeOperator::new(0..10u64);

        let mut oper = AddTimestamp::new(
            fake_operator,
            |n| Timestamp::from_secs(*n),
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
            assert_eq!(
                oper.next(),
                StreamElement::Timestamped(t, Timestamp::from_secs(t))
            );
            assert_eq!(
                oper.next(),
                StreamElement::Watermark(Timestamp::from_secs(t))
            );
            assert_eq!(
                oper.next(),
                StreamElement::Timestamped(t + 1, Timestamp::from_secs(t + 1))
            );
        }
        assert_eq!(oper.next(), StreamElement::Terminate);
    }
}
