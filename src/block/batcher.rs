use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use crate::network::{Coord, NetworkMessage, NetworkSender};
use crate::operator::{ExchangeData, StreamElement};

/// Which policy to use for batching the messages before sending them.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum BatchMode {
    /// A batch is flushed only when the specified number of messages is present.
    Fixed(NonZeroUsize),
    /// A batch is flushed only when the specified number of messages is present or a timeout
    /// expires.
    Adaptive(NonZeroUsize, Duration),
}

/// A `Batcher` wraps a sender and sends the messages in batches to reduce the network overhead.
///
/// Internally it spawns a new task to handle the timeouts and join it at the end.
pub(crate) struct Batcher<Out: ExchangeData> {
    /// Sender used to communicate with the other replicas
    remote_sender: NetworkSender<Out>,
    /// Batching mode used by the batcher
    mode: BatchMode,
    /// Buffer used to keep messages ready to be sent
    buffer: Vec<StreamElement<Out>>,
    /// Time of the last flush of the buffer.    
    last_send: Instant,
    /// The coordinate of this block, used for marking the sender of the batch.
    coord: Coord,
}

impl<Out: ExchangeData> Batcher<Out> {
    pub(crate) fn new(remote_sender: NetworkSender<Out>, mode: BatchMode, coord: Coord) -> Self {
        Self {
            remote_sender,
            mode,
            buffer: Default::default(),
            last_send: Instant::now(),
            coord,
        }
    }

    /// Put a message in the batch queue, it won't be sent immediately.
    pub(crate) fn enqueue(&mut self, message: StreamElement<Out>) {
        self.buffer.push(message);
        // max capacity has been reached, send and flush the buffer
        // if too much time elapsed since the last flush, flush the buffer
        let max_delay = self.mode.max_delay();
        let timeout_elapsed = max_delay
            .map(|max_delay| self.last_send.elapsed() > max_delay)
            .unwrap_or(false);
        if self.buffer.len() >= self.mode.max_capacity() || timeout_elapsed {
            self.flush();
        }
    }

    /// Flush the internal buffer if it's not empty.
    pub(crate) fn flush(&mut self) {
        if !self.buffer.is_empty() {
            let mut batch = Vec::with_capacity(self.mode.max_capacity());
            std::mem::swap(&mut self.buffer, &mut batch);
            let message = NetworkMessage::new(batch, self.coord);
            self.remote_sender.send(message).unwrap();
            self.last_send = Instant::now();
        }
    }

    /// Tell the batcher that the stream is ended, flush all the remaining messages.
    pub(crate) fn end(self) {
        // Send the remaining messages
        if !self.buffer.is_empty() {
            let message = NetworkMessage::new(self.buffer, self.coord);
            self.remote_sender.send(message).unwrap();
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
    pub fn max_capacity(&self) -> usize {
        match self {
            BatchMode::Fixed(cap) | BatchMode::Adaptive(cap, _) => cap.get(),
        }
    }

    /// Maximum delay this mode allows, if any.
    pub fn max_delay(&self) -> Option<Duration> {
        match self {
            BatchMode::Fixed(_) => None,
            BatchMode::Adaptive(_, max_delay) => Some(*max_delay),
        }
    }
}

impl Default for BatchMode {
    fn default() -> Self {
        BatchMode::adaptive(1000, Duration::from_millis(50))
    }
}
