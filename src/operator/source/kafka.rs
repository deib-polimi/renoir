use std::fmt::Display;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use flume::Receiver;
use futures::StreamExt;
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
    Running {
        rx: Receiver<OwnedMessage>,
        cancel_token: Arc<AtomicBool>,
        cooldown: bool,
    }, // Terminated,
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

/// # WARNING: KAFKA API IS EXPERIMENTAL
///
/// If replication is greater than `Replication::One` and timestamping logic
/// is being used, ensure that the number of kafka partitions receiving events
/// is greater than the number of replicas. Otherwise, watermarks may not be generated
/// stalling the computation. To solve this, reduce the replication.
///
/// TODO: address this
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
        let cancel_token = Arc::new(AtomicBool::new(false));
        let cancel = cancel_token.clone();
        tracing::debug!("started kafka source with topics {:?}", topics);
        tokio::spawn(async move {
            let mut stream = consumer.stream();
            while let Some(msg) = stream.next().await {
                let msg = msg.expect("failed receiving from kafka");
                if cancel.load(Ordering::SeqCst) {
                    break;
                }
                let owned = msg.detach();
                if let Err(e) = tx.send(owned) {
                    if cancel.load(Ordering::SeqCst) {
                        break;
                    } else {
                        panic!("channel send failed for kafka source {e}");
                    }
                }
                consumer
                    .commit_message(&msg, CommitMode::Async)
                    .expect("kafka fail to commit");
            }
        });
        self.inner = KafkaSourceInner::Running {
            rx,
            cancel_token,
            cooldown: false,
        };
    }

    fn next(&mut self) -> StreamElement<Self::Out> {
        match &mut self.inner {
            KafkaSourceInner::Init { .. } => {
                unreachable!("KafkaSource executing before setup!")
            }
            // KafkaSourceInner::Terminated => return StreamElement::Terminate,
            KafkaSourceInner::Running { rx, cooldown, .. } => {
                if *cooldown {
                    match rx.recv() {
                        Ok(msg) => {
                            *cooldown = false;
                            return StreamElement::Item(msg);
                        }
                        Err(flume::RecvError::Disconnected) => {
                            tracing::warn!("kafka background task disconnected.");
                            return StreamElement::Terminate;
                        }
                    }
                }

                match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                    Ok(msg) => StreamElement::Item(msg),
                    Err(flume::RecvTimeoutError::Timeout) => {
                        *cooldown = true;
                        StreamElement::FlushBatch
                    }
                    Err(flume::RecvTimeoutError::Disconnected) => {
                        tracing::warn!("kafka background task disconnected.");
                        StreamElement::Terminate
                    } // StreamElement::Terminate,
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

impl Drop for KafkaSource {
    fn drop(&mut self) {
        match &self.inner {
            KafkaSourceInner::Init { .. } => {}
            KafkaSourceInner::Running { cancel_token, .. } => {
                cancel_token.store(true, Ordering::SeqCst);
            }
        }
    }
}

impl crate::StreamContext {
    /// Convenience method, creates a `KafkaSource` and makes a stream using `StreamContext::stream`
    ///
    /// See Examples
    ///
    /// # WARNING: KAFKA API IS EXPERIMENTAL
    ///
    /// If replication is greater than `Replication::One` and timestamping logic
    /// is being used, ensure that the number of kafka partitions receiving events
    /// is greater than the number of replicas. Otherwise, watermarks may not be generated
    /// stalling the computation. To solve this, reduce the replication.
    ///
    /// TODO: address this
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
