use std::time::Duration;

use thiserror::Error;

use crate::channel::{
    self, Receiver, RecvError, RecvTimeoutError, SelectResult, Sender, TryRecvError, TrySendError,
};

use crate::network::{NetworkMessage, ReceiverEndpoint};
use crate::operator::ExchangeData;
use crate::profiler::{get_profiler, Profiler};

/// The capacity of the in-buffer.
const CHANNEL_CAPACITY: usize = 16;

pub(crate) fn local_channel<T: ExchangeData>(
    receiver_endpoint: ReceiverEndpoint,
) -> (NetworkSender<T>, NetworkReceiver<T>) {
    let (sender, receiver) = channel::bounded(CHANNEL_CAPACITY);
    (
        NetworkSender {
            receiver_endpoint,
            sender: SenderInner::Local(sender),
        },
        NetworkReceiver {
            receiver_endpoint,
            receiver,
        },
    )
}

pub(crate) fn mux_sender<T: ExchangeData>(
    receiver_endpoint: ReceiverEndpoint,
    tx: Sender<(ReceiverEndpoint, NetworkMessage<T>)>,
) -> NetworkSender<T> {
    NetworkSender {
        receiver_endpoint,
        sender: SenderInner::Mux(tx),
    }
}

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
pub(crate) struct NetworkReceiver<In: Send + 'static> {
    /// The ReceiverEndpoint of the current receiver.
    pub receiver_endpoint: ReceiverEndpoint,
    /// The actual receiver where the users of this struct will wait upon.
    #[derivative(Debug = "ignore")]
    receiver: Receiver<NetworkMessage<In>>,
}

impl<In: Send + 'static> NetworkReceiver<In> {
    #[inline]
    fn profile_message<E>(
        &self,
        message: Result<NetworkMessage<In>, E>,
    ) -> Result<NetworkMessage<In>, E> {
        message.inspect(|message| {
            get_profiler().items_in(
                message.sender,
                self.receiver_endpoint.coord,
                message.num_items(),
            );
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
pub(crate) struct NetworkSender<Out: Send + 'static> {
    /// The ReceiverEndpoint of the recipient.
    pub receiver_endpoint: ReceiverEndpoint,
    /// The generic sender that will send the message either locally or remotely.
    #[derivative(Debug = "ignore")]
    sender: SenderInner<Out>,
}

enum SenderInner<Out: Send + 'static> {
    Mux(Sender<(ReceiverEndpoint, NetworkMessage<Out>)>),
    Local(Sender<NetworkMessage<Out>>),
}

impl<Out: Send + 'static> Clone for SenderInner<Out> {
    fn clone(&self) -> Self {
        match self {
            Self::Mux(arg0) => Self::Mux(arg0.clone()),
            Self::Local(arg0) => Self::Local(arg0.clone()),
        }
    }
}

impl<Out: Send + 'static> NetworkSender<Out> {
    pub fn send(&self, message: NetworkMessage<Out>) -> Result<(), NetworkSendError> {
        get_profiler().items_out(
            message.sender,
            self.receiver_endpoint.coord,
            message.num_items(),
        );

        match &self.sender {
            SenderInner::Mux(tx) => tx
                .send((self.receiver_endpoint, message))
                .map_err(|_| NetworkSendError::Disconnected(self.receiver_endpoint)),
            SenderInner::Local(tx) => tx
                .send(message)
                .map_err(|_| NetworkSendError::Disconnected(self.receiver_endpoint)),
        }
    }

    pub fn try_send(&self, message: NetworkMessage<Out>) -> Result<(), NetworkTrySendError<Out>> {
        let sender = message.sender;
        let size = message.num_items();
        let res = match &self.sender {
            SenderInner::Mux(tx) => {
                tx.try_send((self.receiver_endpoint, message))
                    .map_err(|e| match e {
                        TrySendError::Full(item) => NetworkTrySendError::Full(item.1),
                        TrySendError::Disconnected(_) => {
                            NetworkTrySendError::Disconnected(self.receiver_endpoint)
                        }
                    })
            }
            SenderInner::Local(tx) => tx.try_send(message).map_err(|e| match e {
                TrySendError::Full(item) => NetworkTrySendError::Full(item),
                TrySendError::Disconnected(_) => {
                    NetworkTrySendError::Disconnected(self.receiver_endpoint)
                }
            }),
        };
        if res.is_ok() {
            get_profiler().items_out(sender, self.receiver_endpoint.coord, size);
        }
        res
    }

    pub fn clone_inner(&self) -> Sender<NetworkMessage<Out>> {
        match &self.sender {
            SenderInner::Mux(_) => panic!("Trying to clone mux channel. Not supported"),
            SenderInner::Local(tx) => tx.clone(),
        }
    }
}

#[derive(Debug, Error)]
pub enum NetworkSendError {
    #[error("channel disconnected")]
    Disconnected(ReceiverEndpoint),
}

#[derive(Debug, Error)]
pub enum NetworkTrySendError<T> {
    #[error("channel full")]
    Full(NetworkMessage<T>),
    #[error("channel disconnected")]
    Disconnected(ReceiverEndpoint),
}
