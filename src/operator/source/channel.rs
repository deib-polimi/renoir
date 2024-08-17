use std::fmt::Display;

use flume::{bounded, Receiver, RecvError, Sender, TryRecvError};

use crate::block::{BlockStructure, OperatorKind, OperatorStructure, Replication};
use crate::operator::source::Source;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

const MAX_RETRY: u8 = 16;

/// Source that consumes an iterator and emits all its elements into the stream.
///
/// The iterator will be consumed **only from one replica**, therefore this source is not parallel.

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ChannelSource<Out: Send + 'static> {
    #[derivative(Debug = "ignore")]
    rx: Receiver<Out>,
    terminated: bool,
    retry_count: u8,
    replication: Replication,
}

impl<Out: Send> Display for ChannelSource<Out> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ChannelSource<{}>", std::any::type_name::<Out>())
    }
}

impl<Out: Send + 'static> ChannelSource<Out> {
    /// Create a new source that reads the items from the iterator provided as input.
    ///
    /// **Note**: the replication of this source is determined by the `replication` parameter
    /// The channel is an MPMC channel so items will be captured by one and only one of the replicas
    /// with no specified order. Developers must take into account this replication when sending items
    /// to the channel in order for messages to be delivered to renoir (eg. if replication is One,
    /// only the host with id 0 should send messages to the channel)
    ///
    /// ## Example
    ///
    /// ```
    /// # use renoir::{StreamContext, RuntimeConfig};
    /// # use renoir::operator::source::ChannelSource;
    /// # let mut env = StreamContext::new_local();
    /// let (tx_channel, source) = ChannelSource::new(4);
    /// let R = env.stream(source);
    /// tx_channel.send(1);
    /// tx_channel.send(2);
    /// ```
    pub fn new(channel_size: usize, replication: Replication) -> (Sender<Out>, Self) {
        let (tx, rx) = bounded(channel_size);
        let s = Self {
            rx,
            terminated: false,
            retry_count: 0,
            replication,
        };

        (tx, s)
    }
}
// TODO: remove Debug requirement
impl<Out: Send + core::fmt::Debug> Source for ChannelSource<Out> {
    fn replication(&self) -> Replication {
        self.replication
    }
}

impl<Out: Send + core::fmt::Debug> Operator for ChannelSource<Out> {
    type Out = Out;

    fn setup(&mut self, _metadata: &mut ExecutionMetadata) {}

    fn next(&mut self) -> StreamElement<Out> {
        loop {
            if self.terminated {
                return StreamElement::Terminate;
            }
            let result = self.rx.try_recv();

            log::debug!("Channel received stuff");
            match result {
                Ok(t) => {
                    self.retry_count = 0;
                    return StreamElement::Item(t);
                }
                Err(TryRecvError::Empty) if self.retry_count < MAX_RETRY => {
                    // Spin before blocking
                    self.retry_count += 1;
                    continue;
                }
                Err(TryRecvError::Empty) if self.retry_count == MAX_RETRY => {
                    log::debug!("no values ready after {MAX_RETRY} tries, sending flush");
                    self.retry_count += 1;
                    return StreamElement::FlushBatch;
                }
                Err(TryRecvError::Empty) => {
                    log::debug!("flushed and no values ready, blocking");
                    self.retry_count = 0;
                    match self.rx.recv() {
                        Ok(t) => return StreamElement::Item(t),
                        Err(RecvError::Disconnected) => {
                            self.terminated = true;
                            log::info!("Stream disconnected");
                            return StreamElement::FlushAndRestart;
                        }
                    }
                }
                Err(TryRecvError::Disconnected) => {
                    self.terminated = true;
                    log::info!("Stream disconnected");
                    return StreamElement::FlushAndRestart;
                }
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("ChannelSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl<Out: Send> Clone for ChannelSource<Out> {
    fn clone(&self) -> Self {
        // Since this is a non-parallel source, we don't want the other replicas to emit any value
        panic!("ChannelSource cannot be cloned, replication should be 1");
    }
}
