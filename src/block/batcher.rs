use std::num::NonZeroUsize;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::thread::JoinHandle;

use crate::network::{NetworkMessage, NetworkSender};
use crate::operator::StreamElement;

/// When `BatchMode::Fixed` is used the batch should not be flushed due to a timeout, for the sake
/// of simplicity a timeout is used anyway with a very large value.
///
/// This value cannot be too big otherwise an integer overflow will happen.
const FIXED_BATCH_MODE_MAX_DELAY: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 10);

/// Capacity of the channel to the `Batcher`.
const BATCHER_CHANNEL_CAPACITY: usize = 1024;

/// Which policy to use for batching the messages before sending them.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum BatchMode {
    /// A batch is flushed only when the specified number of messages is present.
    Fixed(NonZeroUsize),
    /// A batch is flushed only when the specified number of messages is present or a timeout
    /// expires.
    Adaptive(NonZeroUsize, Duration),
}

/// Message sent to the batcher task.
pub(crate) enum BatcherMessage<Out>
where
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    /// An actual message to put in the queue has arrived.
    Message(StreamElement<Out>),
    /// The stream is ended, flush and exit.
    End,
}

/// A `Batcher` wraps a sender and sends the messages in batches to reduce the network overhead.
///
/// Internally it spawns a new task to handle the timeouts and join it at the end.
pub(crate) struct Batcher<Out>
where
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    /// Sender to the internal task that does the batching.
    sender: SyncSender<BatcherMessage<Out>>,
    /// Handle to join the internal task.
    join_handle: JoinHandle<()>,
}

impl<Out> Batcher<Out>
where
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    pub(crate) fn new(remote_sender: NetworkSender<NetworkMessage<Out>>, mode: BatchMode) -> Self {
        let (sender, receiver) = sync_channel(BATCHER_CHANNEL_CAPACITY);
        let join_handle = std::thread::Builder::new()
            .name(format!("Batcher{}", remote_sender.coord))
            .spawn(move || Batcher::batcher_body(remote_sender, mode, receiver))
            .unwrap();
        Self {
            sender,
            join_handle,
        }
    }

    /// Put a message in the batch queue, it won't be sent immediately.
    pub(crate) fn enqueue(&self, message: StreamElement<Out>) {
        self.sender
            .send(BatcherMessage::Message(message))
            .expect("Cannot enqueue if the Batcher already exited")
    }

    /// Tell the batcher that the stream is ended, flush all the messages and join the internal
    /// task.
    pub(crate) fn end(self) {
        self.sender
            .send(BatcherMessage::End)
            .expect("Cannot enqueue if the Batcher already exited");
        self.join_handle;
    }

    /// The body of the internal task that does the batching.
    fn batcher_body(
        sender: NetworkSender<NetworkMessage<Out>>,
        mode: BatchMode,
        receiver: Receiver<BatcherMessage<Out>>,
    ) {
        debug!(
            "Batcher to {} is ready for working in mode {:?}",
            sender.coord, mode
        );
        let mut batch = Vec::with_capacity(mode.max_capacity());
        loop {
            match receiver.recv_timeout(mode.max_delay()) {
                Ok(message) => {
                    match message {
                        BatcherMessage::Message(mex) => {
                            batch.push(mex);
                            // max capacity has been reached, send and flush the buffer
                            if batch.len() >= mode.max_capacity() {
                                sender.send(batch).unwrap();
                                batch = Vec::with_capacity(mode.max_capacity());
                            }
                        }
                        BatcherMessage::End => {
                            // send the last elements in the batch
                            if !batch.is_empty() {
                                sender.send(batch).unwrap();
                            }
                            break;
                        }
                    }
                }
                Err(_) => {
                    // timeout occurred
                    if !batch.is_empty() {
                        sender.send(batch).unwrap();
                        batch = Vec::with_capacity(mode.max_capacity());
                    }
                }
            }
        }
    }
}

impl BatchMode {
    /// Construct a new `BatchMode::Fixed` with the given positive batch size.
    pub fn fixed(size: usize) -> BatchMode {
        BatchMode::Fixed(NonZeroUsize::new(size).expect("The batch size must be positive"))
    }

    /// Construct a new `BatchMode::Adaptive` with the given positive batch size and maximum delay.
    pub fn adaptive(size: usize, max_delay: Duration) -> BatchMode {
        BatchMode::Adaptive(
            NonZeroUsize::new(size).expect("The batch size must be positive"),
            max_delay,
        )
    }

    /// Size of the batch in this mode.
    fn max_capacity(&self) -> usize {
        match self {
            BatchMode::Fixed(cap) | BatchMode::Adaptive(cap, _) => cap.get(),
        }
    }

    /// Maximum delay this mode allows.
    fn max_delay(&self) -> Duration {
        match self {
            BatchMode::Fixed(_) => FIXED_BATCH_MODE_MAX_DELAY,
            BatchMode::Adaptive(_, max_delay) => *max_delay,
        }
    }
}

impl Default for BatchMode {
    fn default() -> Self {
        BatchMode::adaptive(1000, Duration::from_millis(50))
    }
}
