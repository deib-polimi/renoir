use async_std::channel::{Receiver, Sender};

pub use topology::*;

use crate::scheduler::ReplicaId;
use crate::stream::BlockId;
use std::fmt::{Display, Formatter};

mod topology;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct Coord {
    pub block_id: BlockId,
    pub replica_id: ReplicaId,
}

impl Coord {
    pub fn new(block_id: BlockId, replica_id: ReplicaId) -> Self {
        Self {
            block_id,
            replica_id,
        }
    }
}

impl Display for Coord {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Coord[b{}, r{}]", self.block_id, self.replica_id)
    }
}

pub enum NetworkReceiver<In> {
    Local(Receiver<In>),
}

pub enum NetworkSender<Out> {
    Local(Sender<Out>),
}

impl<Out> Clone for NetworkSender<Out> {
    fn clone(&self) -> Self {
        match &self {
            NetworkSender::Local(local) => NetworkSender::Local(local.clone()),
        }
    }
}
