use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

use async_std::channel::{Receiver, Sender};

pub use topology::*;

use crate::operator::StreamElement;
use crate::scheduler::{HostId, ReplicaId};
use crate::stream::BlockId;

mod topology;

pub type Batch<T> = Vec<StreamElement<T>>;
pub type NetworkMessage<T> = Batch<T>;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct Coord {
    pub block_id: BlockId,
    pub host_id: HostId,
    pub replica_id: ReplicaId,
}

impl Coord {
    pub fn new(block_id: BlockId, host_id: HostId, replica_id: ReplicaId) -> Self {
        Self {
            block_id,
            host_id,
            replica_id,
        }
    }
}

impl Display for Coord {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Coord[b{}, h{}, r{}]",
            self.block_id, self.host_id, self.replica_id
        )
    }
}

pub struct NetworkReceiver<In> {
    pub coord: Coord,
    inner: NetworkReceiverInner<In>,
}

pub enum NetworkReceiverInner<In> {
    Local(Receiver<In>),
}

pub struct NetworkSender<Out> {
    pub coord: Coord,
    inner: NetworkSenderInner<Out>,
}

pub enum NetworkSenderInner<Out> {
    Local(Sender<Out>),
}

impl<Out> Clone for NetworkSender<Out> {
    fn clone(&self) -> Self {
        match &self.inner {
            NetworkSenderInner::Local(local) => NetworkSender {
                coord: self.coord,
                inner: NetworkSenderInner::Local(local.clone()),
            },
        }
    }
}

impl<In> NetworkReceiver<In> {
    pub fn local(coord: Coord, receiver: Receiver<In>) -> Self {
        Self {
            coord,
            inner: NetworkReceiverInner::Local(receiver),
        }
    }

    pub async fn recv(&self) -> Result<In, impl Error> {
        match &self.inner {
            NetworkReceiverInner::Local(local) => local.recv().await,
        }
    }
}

impl<In> Debug for NetworkReceiver<In> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let typ = match &self.inner {
            NetworkReceiverInner::Local(_) => "local",
        };
        f.debug_struct("NetworkReceiver")
            .field("coord", &self.coord)
            .field("type", &typ)
            .finish()
    }
}

impl<Out> NetworkSender<Out> {
    pub fn local(coord: Coord, sender: Sender<Out>) -> Self {
        Self {
            coord,
            inner: NetworkSenderInner::Local(sender),
        }
    }

    pub async fn send(&self, item: Out) -> Result<(), impl Error> {
        match &self.inner {
            NetworkSenderInner::Local(local) => local.send(item).await,
        }
    }
}

impl<Out> Debug for NetworkSender<Out> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let typ = match &self.inner {
            NetworkSenderInner::Local(_) => "local",
        };
        f.debug_struct("NetworkSender")
            .field("coord", &self.coord)
            .field("type", &typ)
            .finish()
    }
}
