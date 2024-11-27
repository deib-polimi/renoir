use std::fmt::Display;

use flume::Receiver;
use futures::{StreamExt, TryStreamExt};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::OwnedMessage;
use rdkafka::ClientConfig;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure, Replication};
use crate::operator::source::Source;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::Stream;

enum KafkaSourceInner {
    Init {
        config: ClientConfig,
        topics: Vec<String>,
    },
    Running(Receiver<OwnedMessage>),
    // Terminated,
}

impl Clone for KafkaSourceInner {
    fn clone(&self) -> Self {
        match self {
            Self::Init { config, topics } => Self::Init {
                config: config.clone(),
                topics: topics.clone(),
            },
            _ => panic!("can only clone KafkaSource in itialization state"),
        }
    }
}

/// Source that consumes an iterator and emits all its elements into the stream.
///
/// The iterator will be consumed **only from one replica**, therefore this source is not parallel.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct KafkaSource {
    #[derivative(Debug = "ignore")]
    inner: KafkaSourceInner,
    replication: Replication,
    terminated: bool,
}

impl Display for KafkaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KafkaSource")
    }
}

impl Source for KafkaSource {
    fn replication(&self) -> Replication {
        self.replication
    }
}

impl Operator for KafkaSource {
    type Out = rdkafka::message::OwnedMessage;

    fn setup(&mut self, _metadata: &mut ExecutionMetadata) {
        let KafkaSourceInner::Init { config, topics } = &self.inner else {
            panic!("KafkaSource in invalid state")
        };

        let consumer = config
            .create::<StreamConsumer>()
            .expect("failed to create kafka consumer");
        let t = topics.iter().map(|s| s.as_str()).collect::<Vec<&str>>();
        consumer
            .subscribe(t.as_slice())
            .expect("failed to subscribe to kafka topics");
        tracing::debug!("kafka source subscribed to {topics:?}");

        let (tx, rx) = flume::bounded(8);
        tokio::spawn(async move {
            let mut stream = consumer.stream();
            while let Some(msg) = stream.next().await {
                let msg = msg.expect("failed receiving from kafka").detach();
                tx.send(msg).expect("channel fail from kafka source");
            }
        });
        self.inner = KafkaSourceInner::Running(rx);
    }

    fn next(&mut self) -> StreamElement<Self::Out> {
        match &self.inner {
            KafkaSourceInner::Init { .. } => {
                unreachable!("KafkaSource executing before setup!")
            }
            // KafkaSourceInner::Terminated => return StreamElement::Terminate,
            KafkaSourceInner::Running(rx) => {
                match rx.recv() {
                    Ok(msg) => StreamElement::Item(msg),
                    Err(_e) => todo!(),
                    // StreamElement::Terminate,
                }

            }
        }
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
        if matches!(self.replication, Replication::Host | Replication::One) {
            panic!(
                "KafkaSource with replication {:?} cannot be cloned",
                self.replication
            );
        }

        Self {
            inner: self.inner.clone(),
            replication: self.replication,
            terminated: false,
        }
    }
}

impl crate::StreamContext {
    /// Convenience method, creates a `KafkaSource` and makes a stream using `StreamContext::stream`
    ///
    /// See Examples
    pub fn stream_kafka(
        &self,
        client_config: ClientConfig,
        topics: &[&str],
        replication: Replication,
    ) -> Stream<KafkaSource> {
        let source = KafkaSource {
            inner: KafkaSourceInner::Init {
                config: client_config,
                topics: topics.iter().map(|s| s.to_string()).collect(),
            },
            replication,
            terminated: false,
        };
        self.stream(source)
    }
}
