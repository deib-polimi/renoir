use std::sync::mpsc::SyncSender;

use anyhow::{anyhow, Result};

use crate::network::remote::MultiplexingSender;
use crate::network::ReceiverEndpoint;
use crate::operator::Data;

/// The sender part of a connection between two replicas.
///
/// This works for both a local in-memory connection and for a remote socket connection. When this
/// is bound to a local channel the receiver will be a `Receiver`. When it's bound to a remote
/// connection internally this points to the multiplexer that handles the remote channel.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub(crate) struct NetworkSender<Out: Data> {
    /// The ReceiverEndpoint of the recipient.
    pub receiver_endpoint: ReceiverEndpoint,
    /// The generic sender that will send the message either locally or remotely.
    #[derivative(Debug = "ignore")]
    sender: NetworkSenderImpl<Out>,
}

/// The internal sender that sends either to a local in-memory channel, or to a remote channel using
/// a multiplexer.
#[derive(Clone)]
pub(crate) enum NetworkSenderImpl<Out: Data> {
    /// The channel is local, use an in-memory channel.
    Local(SyncSender<Out>),
    /// The channel is remote, use the multiplexer.
    Remote(MultiplexingSender<Out>),
}

impl<Out: Data> NetworkSender<Out> {
    /// Create a new local sender that sends the data directly to the recipient.
    pub fn local(receiver_endpoint: ReceiverEndpoint, sender: SyncSender<Out>) -> Self {
        Self {
            receiver_endpoint,
            sender: NetworkSenderImpl::Local(sender),
        }
    }

    /// Create a new remote sender that sends the data via a multiplexer.
    pub fn remote(receiver_endpoint: ReceiverEndpoint, sender: MultiplexingSender<Out>) -> Self {
        Self {
            receiver_endpoint,
            sender: NetworkSenderImpl::Remote(sender),
        }
    }

    /// Send a message to a replica.
    pub fn send(&self, item: Out) -> Result<()> {
        match &self.sender {
            NetworkSenderImpl::Local(sender) => sender.send(item).map_err(|e| {
                anyhow!(
                    "Failed to send to channel to {:?}: {:?}",
                    self.receiver_endpoint,
                    e
                )
            }),
            NetworkSenderImpl::Remote(sender) => sender.send(self.receiver_endpoint, item),
        }
    }
}
