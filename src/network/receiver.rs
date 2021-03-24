use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::time::Duration;

use anyhow::{anyhow, Result};

use crate::network::{NetworkSender, ReceiverEndpoint};
use crate::operator::Data;

/// The capacity of the in-buffer.
const CHANNEL_CAPACITY: usize = 10;

/// The receiving end of a connection between two replicas.
///
/// This works for both a local in-memory connection and for a remote socket connection. This will
/// always be able to listen to a socket. No socket will be bound until a message is sent to the
/// starter returned by the constructor.
///
/// Internally it contains a in-memory sender-receiver pair, to get the local sender call
/// `.sender()`. When the socket will be bound an task will be spawned, it will bind the
/// socket and send to the same in-memory channel the received messages.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct NetworkReceiver<In: Data> {
    /// The ReceiverEndpoint of the current receiver.
    pub receiver_endpoint: ReceiverEndpoint,
    /// The actual receiver where the users of this struct will wait upon.
    #[derivative(Debug = "ignore")]
    receiver: Receiver<In>,
    /// The sender associated with `self.receiver`.
    #[derivative(Debug = "ignore")]
    pub local_sender: SyncSender<In>,
}

impl<In: Data> NetworkReceiver<In> {
    /// Construct a new `NetworkReceiver`.
    ///
    /// To get its sender use `.sender()` for a `NetworkSender` or directly `.local_sender` for the
    /// raw channel.
    pub fn new(receiver_endpoint: ReceiverEndpoint) -> Self {
        let (sender, receiver) = sync_channel(CHANNEL_CAPACITY);
        Self {
            receiver_endpoint,
            receiver,
            local_sender: sender,
        }
    }

    /// Obtain a `NetworkSender` that will send messages that will arrive to this receiver.
    pub fn sender(&self) -> NetworkSender<In> {
        NetworkSender::local(self.receiver_endpoint, self.local_sender.clone())
    }

    /// Receive a message from any sender.
    pub fn recv(&self) -> Result<In> {
        self.receiver.recv().map_err(|e| {
            anyhow!(
                "Failed to receive from channel at {:?}: {:?}",
                self.receiver_endpoint,
                e
            )
        })
    }

    /// Receive a message from any sender with a timeout.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<In, RecvTimeoutError> {
        self.receiver.recv_timeout(timeout)
    }
}
