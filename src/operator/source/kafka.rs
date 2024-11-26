use std::fmt::Display;

use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure, Replication};
use crate::operator::source::Source;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::Stream;

/// Source that consumes an iterator and emits all its elements into the stream.
///
/// The iterator will be consumed **only from one replica**, therefore this source is not parallel.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct KafkaSource {
    #[derivative(Debug = "ignore")]
    consumer: StreamConsumer,
    terminated: bool,
}

impl Display for KafkaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KafkaSource")
    }
}

impl From<StreamConsumer> for KafkaSource {
    fn from(value: StreamConsumer) -> Self {
        KafkaSource {
            consumer: value,
            terminated: false,
        }
    }
}

impl Source for KafkaSource {
    fn replication(&self) -> Replication {
        Replication::One
    }
}

impl Operator for KafkaSource {
    type Out = (Option<Vec<u8>>, Option<Vec<u8>>);

    fn setup(&mut self, _metadata: &mut ExecutionMetadata) {}

    fn next(&mut self) -> StreamElement<Self::Out> {
        if self.terminated {
            return StreamElement::Terminate;
        }

        let r = futures::executor::block_on(self.consumer.recv()).expect("kafka failure");

        let key = r.key().map(ToOwned::to_owned);
        let payload = r.payload().map(ToOwned::to_owned);

        // TODO: with adaptive batching this does not work since it never emits FlushBatch messages
        let output = StreamElement::Item((key, payload));

        self.consumer
            .commit_message(&r, CommitMode::Async)
            .expect("kafka failure");
        output
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Self::Out, _>("KafkaSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl Clone for KafkaSource {
    fn clone(&self) -> Self {
        // Since this is a non-parallel source, we don't want the other replicas to emit any value
        panic!("KafkaSource cannot be cloned, replication should be 1");
    }
}

impl crate::StreamContext {
    /// Convenience method, creates a `KafkaSource` and makes a stream using `StreamContext::stream`
    /// 
    /// See Examples
    pub fn stream_kafka(&self, consumer: StreamConsumer) -> Stream<KafkaSource> {
        let source = KafkaSource::from(consumer);
        self.stream(source)
    }
}
