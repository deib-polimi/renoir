use std::fmt::Display;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;

#[derive(Clone)]
pub struct AddTimestamp<TimestampGen, WatermarkGen, OperatorChain>
where
    OperatorChain: Operator,
    TimestampGen: FnMut(&OperatorChain::Out) -> Timestamp + Clone + Send + 'static,
    WatermarkGen:
        FnMut(&OperatorChain::Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
{
    prev: OperatorChain,
    timestamp_gen: TimestampGen,
    watermark_gen: WatermarkGen,
    pending_watermark: Option<Timestamp>,
}

impl<TimestampGen, WatermarkGen, OperatorChain> Display
    for AddTimestamp<TimestampGen, WatermarkGen, OperatorChain>
where
    OperatorChain: Operator,
    TimestampGen: FnMut(&OperatorChain::Out) -> Timestamp + Clone + Send + 'static,
    WatermarkGen:
        FnMut(&OperatorChain::Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> AddTimestamp", self.prev)
    }
}

impl<TimestampGen, WatermarkGen, OperatorChain>
    AddTimestamp<TimestampGen, WatermarkGen, OperatorChain>
where
    OperatorChain: Operator,
    TimestampGen: FnMut(&OperatorChain::Out) -> Timestamp + Clone + Send + 'static,
    WatermarkGen:
        FnMut(&OperatorChain::Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
{
    pub(super) fn new(
        prev: OperatorChain,
        timestamp_gen: TimestampGen,
        watermark_gen: WatermarkGen,
    ) -> Self {
        Self {
            prev,
            timestamp_gen,
            watermark_gen,
            pending_watermark: None,
        }
    }
}

impl<TimestampGen, WatermarkGen, OperatorChain> Operator
    for AddTimestamp<TimestampGen, WatermarkGen, OperatorChain>
where
    OperatorChain: Operator,
    TimestampGen: FnMut(&OperatorChain::Out) -> Timestamp + Clone + Send + 'static,
    WatermarkGen:
        FnMut(&OperatorChain::Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
{
    type Out = OperatorChain::Out;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Self::Out> {
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
            .add_operator(OperatorStructure::new::<Self::Out, _>("AddTimestamp"))
    }
}

#[derive(Clone)]
pub struct DropTimestamp<OperatorChain>
where
    OperatorChain: Operator,
{
    prev: OperatorChain,
}

impl<OperatorChain> Display for DropTimestamp<OperatorChain>
where
    OperatorChain: Operator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> DropTimestamp", self.prev)
    }
}

impl<OperatorChain> DropTimestamp<OperatorChain>
where
    OperatorChain: Operator,
{
    pub(super) fn new(prev: OperatorChain) -> Self {
        Self { prev }
    }
}

impl<OperatorChain> Operator for DropTimestamp<OperatorChain>
where
    OperatorChain: Operator,
{
    type Out = OperatorChain::Out;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Self::Out> {
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
            .add_operator(OperatorStructure::new::<Self::Out, _>("DropTimestamp"))
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
