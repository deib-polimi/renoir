use std::fmt::{Debug, Display, Formatter};

pub(crate) use receiver::*;
pub(crate) use sender::*;
pub(crate) use topology::*;

use crate::config::RemoteRuntimeConfig;
use crate::operator::StreamElement;
use crate::scheduler::{HostId, ReplicaId};
use crate::stream::BlockId;

mod demultiplexer;
mod multiplexer;
mod receiver;
mod remote;
mod sender;
mod topology;

/// A batch of elements to send.
pub(crate) type Batch<T> = Vec<StreamElement<T>>;
/// What is sent from a replica to the next.
pub(crate) type NetworkMessage<T> = Batch<T>;

/// Coordinates that identify a block inside the network.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub(crate) struct BlockCoord {
    /// The identifier of the block the replicas works on.
    pub block_id: BlockId,
    /// The identifier of where the replica is located.
    pub host_id: HostId,
}

/// Coordinates that identify a replica inside the network.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub(crate) struct Coord {
    /// The identifier of the block the replicas works on.
    pub block_id: BlockId,
    /// The identifier of where the replica is located.
    pub host_id: HostId,
    /// The identifier of the replica inside the host.
    pub replica_id: ReplicaId,
}

/// The identifier of a single receiver endpoint of a replicated block.
///
/// Note that a replicated block may have many predecessors, each with a different message type, so
/// it has to have more than one receiver. In particular it should have a receiver for each
/// previous block in the job graph.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub(crate) struct ReceiverEndpoint {
    /// Coordinate of the receiver replica.
    pub coord: Coord,
    /// Id of the sender block.
    pub prev_block_id: BlockId,
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

impl BlockCoord {
    /// Get the address/port to bind for the current block.
    pub fn address(&self, remote_runtime_config: &RemoteRuntimeConfig) -> (String, u16) {
        let host = &remote_runtime_config.hosts[self.host_id];
        (host.address.clone(), host.base_port + self.block_id as u16)
    }
}

impl ReceiverEndpoint {
    pub fn new(coord: Coord, prev_block_id: BlockId) -> Self {
        Self {
            coord,
            prev_block_id,
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

impl Display for BlockCoord {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlockCoord[b{}, h{}]", self.block_id, self.host_id)
    }
}

impl Display for ReceiverEndpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReceiverEndpoint[{}, prev {}]",
            self.coord, self.prev_block_id
        )
    }
}

impl From<Coord> for BlockCoord {
    fn from(coord: Coord) -> Self {
        Self {
            block_id: coord.block_id,
            host_id: coord.host_id,
        }
    }
}
