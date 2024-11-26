use std::fmt::Display;

use rdkafka::message::ToBytes;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};

use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::Stream;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct KafkaSink<Op>
where
    Op: Operator,
{
    prev: Op,
    #[derivative(Debug = "ignore")]
    producer: FutureProducer,
    topic: String,
}

impl<Op> KafkaSink<Op>
where
    Op: Operator,
{
    pub(crate) fn new(prev: Op, producer: FutureProducer, topic: String) -> Self {
        Self {
            prev,
            producer,
            topic,
        }
    }
}

impl<Op> Display for KafkaSink<Op>
where
    Op: Operator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> KafkaSink", self.prev)
    }
}

impl<Op> Operator for KafkaSink<Op>
where
    Op: Operator,
    Op::Out: ToBytes,
{
    type Out = ();

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<()> {
        loop {
            match self.prev.next() {
                StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                    let record = FutureRecord::to(&self.topic)
                    .key(&[])
                    .payload(&t)
                    // .key(&t)
                    ;

                    futures::executor::block_on(
                        self.producer
                            .send(record, Timeout::After(std::time::Duration::from_secs(30))),
                    )
                    .expect("kafka fail");
                }
                StreamElement::Watermark(w) => return StreamElement::Watermark(w),
                StreamElement::Terminate => return StreamElement::Terminate,
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::FlushAndRestart => return StreamElement::FlushAndRestart,
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Op::Out, _>("KafkaSinkSink");
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }
}

impl<Op> Stream<Op>
where
    Op: Operator<Out: ToBytes> + 'static,
{
    pub fn write_kafka(self, producer: FutureProducer, topic: &str) {
        self.add_operator(|prev| KafkaSink::new(prev, producer, topic.to_owned()))
            .finalize_block();
    }
}
