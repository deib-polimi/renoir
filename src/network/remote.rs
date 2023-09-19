use once_cell::sync::Lazy;
use std::any::TypeId;
#[cfg(not(feature = "async-tokio"))]
use std::io::Read;
#[cfg(not(feature = "async-tokio"))]
use std::io::Write;
#[cfg(feature = "async-tokio")]
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use std::net::SocketAddr;

use bincode::config::{FixintEncoding, RejectTrailing, WithOtherIntEncoding, WithOtherTrailing};
use bincode::{DefaultOptions, Options};
use serde::{Deserialize, Serialize};

use crate::network::{Coord, DemuxCoord, NetworkMessage, ReceiverEndpoint};
use crate::operator::ExchangeData;
use crate::profiler::{get_profiler, Profiler};
use crate::scheduler::BlockId;
use crate::scheduler::ReplicaId;

/// Configuration of the header serializer: the integers must have a fixed length encoding.
static BINCODE_HEADER_CONFIG: Lazy<
    WithOtherTrailing<WithOtherIntEncoding<DefaultOptions, FixintEncoding>, RejectTrailing>,
> = Lazy::new(|| {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
});

static BINCODE_MSG_CONFIG: Lazy<DefaultOptions> = Lazy::new(bincode::DefaultOptions::new);

pub(crate) const HEADER_SIZE: usize = 20; // std::mem::size_of::<MessageHeader>();
pub(crate) const LINK_ID_SIZE: usize = 16; // std::mem::size_of::<MessageHeader>();

/// Header of a message sent before the actual message.
#[derive(Serialize, Deserialize, Default)]
struct MessageHeader {
    /// The size of the actual message
    size: u32,
    /// The id of the replica this message is for.
    replica_id: ReplicaId,
    /// The id of the block that is sending the message.
    sender_block_id: BlockId,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub(super) struct LinkId {
    block_id: BlockId,
    prev_id: BlockId,
}

impl From<DemuxCoord> for LinkId {
    fn from(value: DemuxCoord) -> Self {
        Self {
            block_id: value.coord.block_id,
            prev_id: value.prev_block_id,
        }
    }
}

pub(super) async fn read_stream_header<R: AsyncRead + Unpin>(r: &mut R) -> LinkId {
    let mut buf = [0u8; LINK_ID_SIZE];
    r.read_exact(&mut buf).await.unwrap_or_else(|e| {
        panic!(
            "Failed to receive {} bytes of header: {:?}",
            HEADER_SIZE, e
        )
    });
    let header: LinkId = BINCODE_HEADER_CONFIG
        .deserialize(&buf)
        .expect("Malformed stream header");
    header
}

pub(super) async fn write_stream_header<W: AsyncWrite + Unpin>(w: &mut W, link_id: LinkId) {
    let mut buf = Vec::with_capacity(LINK_ID_SIZE);
    // assert_eq!(BINCODE_HEADER_CONFIG.serialized_size(&link_id).unwrap(), LINK_ID_SIZE as u64);
    BINCODE_HEADER_CONFIG
        .serialize_into(&mut buf, &link_id)
        .expect("Malformed stream header");
    debug!("serialized link_id {:?} as {:?}", link_id, buf);
    w.write_all(&buf).await.unwrap();
}

/// Serialize and send a message to a remote socket.
///
/// The network protocol works as follow:
/// - send a `MessageHeader` serialized with bincode with `FixintEncoding`
/// - send the message
#[cfg(not(feature = "async-tokio"))]
pub(crate) fn remote_send<T: ExchangeData, W: Write>(
    msg: NetworkMessage<T>,
    dest: ReceiverEndpoint,
    writer: &mut W,
    address: &str,
) {
    let serialized_len = BINCODE_MSG_CONFIG
        .serialized_size(&msg)
        .unwrap_or_else(|e| {
            panic!("Failed to compute serialized length of outgoing message to {dest}: {e:?}",)
        });

    let header = MessageHeader {
        size: serialized_len.try_into().unwrap(),
        replica_id: dest.coord.replica_id,
        sender_block_id: dest.prev_block_id,
    };

    let mut buf = Vec::with_capacity(HEADER_SIZE + serialized_len as usize);

    BINCODE_HEADER_CONFIG
        .serialize_into(&mut buf, &header)
        .unwrap_or_else(|e| {
            panic!(
                "Failed to serialize header of message (was {serialized_len} bytes) to {dest} at {address}: {e:?}",
            )
        });

    BINCODE_MSG_CONFIG
        .serialize_into(&mut buf, &msg)
        .unwrap_or_else(|e| {
            panic!(
                "Failed to serialize message, {serialized_len} bytes to {dest} at {address}: {e:?}",
            )
        });

    assert_eq!(buf.len(), HEADER_SIZE + serialized_len as usize);

    writer.write_all(buf.as_ref()).unwrap_or_else(|e| {
        panic!("Failed to send message {serialized_len} bytes to {dest} at {address}: {e:?}",)
    });

    get_profiler().net_bytes_out(
        msg.sender,
        dest.coord,
        HEADER_SIZE + serialized_len as usize,
    );
}

/// Serialize and send a message to a remote socket.
///
/// The network protocol works as follow:
/// - send a `MessageHeader` serialized with bincode with `FixintEncoding`
/// - send the message
#[cfg(feature = "async-tokio")]
pub(crate) async fn remote_send<T: ExchangeData, W: AsyncWrite + Unpin>(
    msg: NetworkMessage<T>,
    dest: ReceiverEndpoint,
    writer: &mut W,
    address: &SocketAddr,
) {
    let serialized_len = BINCODE_MSG_CONFIG
        .serialized_size(&msg)
        .unwrap_or_else(|e| {
            panic!(
                "Failed to compute serialized length of outgoing message to {}: {:?}",
                dest, e
            )
        });

    let header = MessageHeader {
        size: serialized_len.try_into().unwrap(),
        replica_id: dest.coord.replica_id,
        sender_block_id: dest.prev_block_id,
    };

    let mut buf = Vec::with_capacity(HEADER_SIZE + serialized_len as usize);

    BINCODE_HEADER_CONFIG
        .serialize_into(&mut buf, &header)
        .unwrap_or_else(|e| {
            panic!(
                "Failed to serialize header of message (was {} bytes) to {} at {}: {:?}",
                serialized_len, dest, address, e
            )
        });

    BINCODE_MSG_CONFIG
        .serialize_into(&mut buf, &msg)
        .unwrap_or_else(|e| {
            panic!(
                "Failed to serialize message, {} bytes to {} at {}: {:?}",
                serialized_len, dest, address, e
            )
        });
    assert_eq!(buf.len(), HEADER_SIZE + serialized_len as usize);

    writer.write_all(buf.as_ref()).await.unwrap_or_else(|e| {
        panic!(
            "Failed to send message {} bytes to {} at {}: {:?}",
            serialized_len, dest, address, e
        )
    });

    get_profiler().net_bytes_out(
        msg.sender,
        dest.coord,
        HEADER_SIZE + serialized_len as usize,
    );
}

/// Receive a message from the remote channel. Returns `None` if there was a failure receiving the
/// last message.
///
/// The message won't be deserialized, use `deserialize()`.
#[cfg(not(feature = "async-tokio"))]
pub(crate) fn remote_recv<T: ExchangeData, R: Read>(
    coord: DemuxCoord,
    reader: &mut R,
    address: &str,
) -> Option<(ReceiverEndpoint, NetworkMessage<T>)> {
    let mut header = [0u8; HEADER_SIZE];
    match reader.read_exact(&mut header) {
        Ok(_) => {}
        Err(e) => {
            log::trace!(
                "Failed to receive {} bytes of header to {} from {}: {:?}",
                HEADER_SIZE,
                coord,
                address,
                e
            );
            return None;
        }
    }
    let header: MessageHeader = BINCODE_HEADER_CONFIG
        .deserialize(&header)
        .expect("Malformed header");
    let mut buf = vec![0u8; header.size as usize];
    reader.read_exact(&mut buf).unwrap_or_else(|e| {
        panic!(
            "Failed to receive {} bytes to {} from {}: {:?}",
            header.size, coord, address, e
        )
    });
    let msg: NetworkMessage<T> = BINCODE_MSG_CONFIG
        .deserialize(buf.as_ref())
        .expect("Malformed message");

    let dest = ReceiverEndpoint::new(
        Coord::new(coord.coord.block_id, coord.coord.host_id, header.replica_id),
        header.sender_block_id,
    );
    get_profiler().net_bytes_in(msg.sender, dest.coord, HEADER_SIZE + header.size as usize);
    Some((dest, msg))
}

#[cfg(feature = "async-tokio")]
pub(crate) async fn remote_recv<T: ExchangeData, R: AsyncRead + Unpin>(
    coord: DemuxCoord,
    reader: &mut R,
    address: &SocketAddr,
) -> Option<(ReceiverEndpoint, NetworkMessage<T>)> {
    let mut header = [0u8; HEADER_SIZE];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) => {
            log::trace!(
                "Failed to receive {} bytes of header to {} from {}: {:?}",
                HEADER_SIZE,
                coord,
                address,
                e
            );
            return None;
        }
    }
    let header: MessageHeader = BINCODE_HEADER_CONFIG
        .deserialize(&header)
        .expect("Malformed header");
    let mut buf = vec![0u8; header.size as usize];
    reader.read_exact(&mut buf).await.unwrap_or_else(|e| {
        panic!(
            "Failed to receive {} bytes to {} from {}: {:?}",
            header.size, coord, address, e
        )
    });
    let msg: NetworkMessage<T> = BINCODE_MSG_CONFIG
        .deserialize(buf.as_ref())
        .expect("Malformed message");

    let dest = ReceiverEndpoint::new(
        Coord::new(coord.coord.block_id, coord.coord.host_id, header.replica_id),
        header.sender_block_id,
    );
    get_profiler().net_bytes_in(msg.sender, dest.coord, HEADER_SIZE + header.size as usize);
    Some((dest, msg))
}

#[cfg(test)]
mod tests {
    use bincode::Options;

    use crate::network::remote::{HEADER_SIZE, LinkId, LINK_ID_SIZE};

    use super::{MessageHeader, BINCODE_HEADER_CONFIG};

    #[test]
    fn header_size() {
        let computed_size = BINCODE_HEADER_CONFIG
            .serialized_size(&MessageHeader::default())
            .unwrap();

        assert_eq!(HEADER_SIZE as u64, computed_size);

        let computed_size = BINCODE_HEADER_CONFIG
            .serialized_size(&LinkId{block_id: 0, prev_id: 0})
            .unwrap();

        assert_eq!(LINK_ID_SIZE as u64, computed_size);
    }
}
