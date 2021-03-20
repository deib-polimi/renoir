use std::fmt::{Debug, Display, Formatter};

use std::sync::mpsc::{Receiver, SyncSender};

pub(crate) use receiver::*;
pub(crate) use sender::*;
pub(crate) use topology::*;

use crate::operator::StreamElement;
use crate::scheduler::{HostId, ReplicaId};
use crate::stream::BlockId;

mod receiver;
mod remote;
mod sender;
mod topology;

/// Sender that communicate to a network component to start working. Sending `true` will make the
/// network component start working, sending `false` or dropping all the senders will make the
/// component exit.
pub(crate) type NetworkStarter = SyncSender<bool>;
/// Receiver part of `NetworkStarter`.
pub(crate) type NetworkStarterRecv = Receiver<bool>;

/// A batch of elements to send.
pub type Batch<T> = Vec<StreamElement<T>>;
/// What is sent from a replica to the next.
pub type NetworkMessage<T> = Batch<T>;

/// Coordinates that identify a replica inside the network.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Coord {
    /// The identifier of the block the replicas works on.
    pub block_id: BlockId,
    /// The identifier of where the replica is located.
    pub host_id: HostId,
    /// The identifier of the replica inside the host.
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

/// Wait for the start signal, return whether the component should start working or not.
pub(crate) fn wait_start(receiver: NetworkStarterRecv) -> bool {
    receiver.recv().unwrap_or(false)
}
