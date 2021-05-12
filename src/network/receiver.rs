use std::time::Duration;

use anyhow::{anyhow, Result};

use crate::channel::{
    BoundedChannelReceiver, BoundedChannelSender, RecvTimeoutError, SelectAnyResult, TryRecvError,
};
use crate::network::{NetworkMessage, NetworkSender, ReceiverEndpoint};
use crate::operator::ExchangeData;
use crate::profiler::{get_profiler, Profiler};

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
pub struct NetworkReceiver<In: ExchangeData> {
    /// The ReceiverEndpoint of the current receiver.
    pub receiver_endpoint: ReceiverEndpoint,
    /// The actual receiver where the users of this struct will wait upon.
    #[derivative(Debug = "ignore")]
    receiver: BoundedChannelReceiver<NetworkMessage<In>>,
    /// The sender associated with `self.receiver`.
    #[derivative(Debug = "ignore")]
    local_sender: Option<BoundedChannelSender<NetworkMessage<In>>>,
}

impl<In: ExchangeData> NetworkReceiver<In> {
    /// Construct a new `NetworkReceiver`.
    ///
    /// To get its sender use `.sender()` for a `NetworkSender` or directly `.local_sender` for the
    /// raw channel.
    pub fn new(receiver_endpoint: ReceiverEndpoint) -> Self {
        let (sender, receiver) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);
        Self {
            receiver_endpoint,
            receiver,
            local_sender: Some(sender),
        }
    }

    /// Obtain a `NetworkSender` that will send messages that will arrive to this receiver.
    pub fn sender(&mut self) -> Option<NetworkSender<In>> {
        self.local_sender
            .take()
            .map(|sender| NetworkSender::local(self.receiver_endpoint, sender))
    }

    #[inline]
    fn profile_message<E>(
        &self,
        message: Result<NetworkMessage<In>, E>,
    ) -> Result<NetworkMessage<In>, E> {
        message.map(|message| {
            get_profiler().items_in(
                message.sender,
                self.receiver_endpoint.coord,
                message.num_items(),
            );
            message
        })
    }

    /// Receive a message from any sender.
    #[allow(dead_code)]
    pub fn recv(&self) -> Result<NetworkMessage<In>> {
        self.profile_message(self.receiver.recv().map_err(|e| {
            anyhow!(
                "Failed to receive from channel at {:?}: {:?}",
                self.receiver_endpoint,
                e
            )
        }))
    }

    /// Receive a message from any sender without blocking.
    #[allow(dead_code)]
    pub fn try_recv(&self) -> Result<NetworkMessage<In>, TryRecvError> {
        self.profile_message(self.receiver.try_recv())
    }

    /// Receive a message from any sender with a timeout.
    #[allow(dead_code)]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<NetworkMessage<In>, RecvTimeoutError> {
        self.profile_message(self.receiver.recv_timeout(timeout))
    }

    /// Same as `select`, but takes multiple receivers to select from.
    pub fn select_any(receivers: &[NetworkReceiver<In>]) -> SelectAnyResult<NetworkMessage<In>> {
        let mut res = BoundedChannelReceiver::select_any(receivers.iter().map(|r| &r.receiver));
        res.result = receivers[res.index].profile_message(res.result);
        res
    }

    /// Same as `select_timeout`, but takes multiple receivers to select from.
    pub fn select_any_timeout(
        receivers: &[NetworkReceiver<In>],
        timeout: Duration,
    ) -> Result<SelectAnyResult<NetworkMessage<In>>, RecvTimeoutError> {
        BoundedChannelReceiver::select_any_timeout(receivers.iter().map(|r| &r.receiver), timeout)
            .map(|mut res| {
                res.result = receivers[res.index].profile_message(res.result);
                res
            })
    }
}
