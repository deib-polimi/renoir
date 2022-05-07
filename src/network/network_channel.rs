use std::time::Duration;

use thiserror::Error;

use crate::channel::{
    self, Receiver, RecvError, RecvTimeoutError, SelectResult, Sender, TryRecvError,
};

use crate::network::{NetworkMessage, ReceiverEndpoint};
use crate::operator::ExchangeData;
use crate::profiler::{get_profiler, Profiler};

/// The capacity of the in-buffer.
const CHANNEL_CAPACITY: usize = 64;

pub(crate) fn local_channel<T: ExchangeData>(
    receiver_endpoint: ReceiverEndpoint,
) -> (NetworkSender<T>, NetworkReceiver<T>) {
    let (sender, receiver) = channel::bounded(CHANNEL_CAPACITY);
    (
        NetworkSender{
            receiver_endpoint,
            sender},
        NetworkReceiver {
            receiver_endpoint,
            receiver,
        },
    )
}

// pub(crate) fn remote_channel<In: ExchangeData>(
//     receiver_endpoint: ReceiverEndpoint,
//     sender: MultiplexingSender<In>,
// ) -> NetworkSender<In> {
//     let sender = NetworkSender{receiver_endpoint, sender};
//     sender
// }

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
pub(crate) struct NetworkReceiver<In: ExchangeData> {
    /// The ReceiverEndpoint of the current receiver.
    pub receiver_endpoint: ReceiverEndpoint,
    /// The actual receiver where the users of this struct will wait upon.
    #[derivative(Debug = "ignore")]
    receiver: Receiver<NetworkMessage<In>>,
}

impl<In: ExchangeData> NetworkReceiver<In> {
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
    pub fn recv(&self) -> Result<NetworkMessage<In>, RecvError> {
        self.profile_message(self.receiver.recv())
    }

    /// Receive a message from any sender without blocking.
    pub fn try_recv(&self) -> Result<NetworkMessage<In>, TryRecvError> {
        self.profile_message(self.receiver.try_recv())
    }

    /// Receive a message from any sender with a timeout.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<NetworkMessage<In>, RecvTimeoutError> {
        self.profile_message(self.receiver.recv_timeout(timeout))
    }

    /// Receive a message from any sender of this receiver of the other provided receiver.
    ///
    /// The first message of the two is returned. If both receivers are ready one of them is chosen
    /// randomly (with an unspecified probability). It's guaranteed this function has the eventual
    /// fairness property.
    pub fn select<In2: ExchangeData>(
        &self,
        other: &NetworkReceiver<In2>,
    ) -> SelectResult<NetworkMessage<In>, NetworkMessage<In2>> {
        self.receiver.select(&other.receiver)
    }

    /// Same as `select`, with a timeout.
    pub fn select_timeout<In2: ExchangeData>(
        &self,
        other: &NetworkReceiver<In2>,
        timeout: Duration,
    ) -> Result<SelectResult<NetworkMessage<In>, NetworkMessage<In2>>, RecvTimeoutError> {
        self.receiver.select_timeout(&other.receiver, timeout)
    }
}

/// The sender part of a connection between two replicas.
///
/// This works for both a local in-memory connection and for a remote socket connection. When this
/// is bound to a local channel the receiver will be a `Receiver`. When it's bound to a remote
/// connection internally this points to the multiplexer that handles the remote channel.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub(crate) struct NetworkSender<Out: ExchangeData> {
    /// The ReceiverEndpoint of the recipient.
    pub receiver_endpoint: ReceiverEndpoint,
    /// The generic sender that will send the message either locally or remotely.
    #[derivative(Debug = "ignore")]
    pub(super) sender: Sender<NetworkMessage<Out>>,
}

impl<Out: ExchangeData> NetworkSender<Out> {
    /// Send a message to a replica.
    pub fn send(&self, message: NetworkMessage<Out>) -> Result<(), NetworkSendError> {
        get_profiler().items_out(
            message.sender,
            self.receiver_endpoint.coord,
            message.num_items(),
        );
        self.sender
            .send(message)
            .map_err(|_| NetworkSendError::Disconnected(self.receiver_endpoint))
    }

    /// Get the inner sender if the channel is local.
    pub fn inner(&self) -> Option<&Sender<NetworkMessage<Out>>> {
        Some(&self.sender)
    }

    pub fn clone_inner(&self) -> Sender<NetworkMessage<Out>> {
        self.sender.clone()
    }
}

#[derive(Debug, Error)]
pub enum NetworkSendError {
    #[error("channel disconnected")]
    Disconnected(ReceiverEndpoint),
}
