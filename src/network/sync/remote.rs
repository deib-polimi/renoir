use bincode::config::Configuration;
use bincode::config::Fixint;
use bincode::config::Limit;
use bincode::config::LittleEndian;
#[cfg(not(feature = "tokio"))]
use std::io::Read;
#[cfg(not(feature = "tokio"))]
use std::io::Write;

use serde::{Deserialize, Serialize};

use crate::network::{Coord, DemuxCoord, NetworkMessage, ReceiverEndpoint};
use crate::operator::ExchangeData;
use crate::profiler::{get_profiler, Profiler};
use crate::scheduler::BlockId;
use crate::scheduler::ReplicaId;

pub(crate) const HEADER_SIZE: usize = 20; // std::mem::size_of::<MessageHeader>();
/// Configuration of the header serializer: the integers must have a fixed length encoding.
pub(crate) static BINCODE_HEADER: Configuration<LittleEndian, Fixint, Limit<HEADER_SIZE>> =
    bincode::config::standard()
        .with_fixed_int_encoding()
        .with_limit::<HEADER_SIZE>();

pub(crate) static BINCODE_MESSAGE: Configuration = bincode::config::standard();

/// Header of a message sent before the actual message.
#[derive(Serialize, Deserialize, Default)]
pub(crate) struct MessageHeader {
    /// The size of the actual message
    pub(crate) size: u32,
    /// The id of the replica this message is for.
    pub(crate) replica_id: ReplicaId,
    /// The id of the block that is sending the message.
    pub(crate) sender_block_id: BlockId,
}

/// Serialize and send a message to a remote socket.
///
/// The network protocol works as follow:
/// - send a `MessageHeader` serialized with bincode with `FixintEncoding`
/// - send the message
pub(crate) fn remote_send<T: ExchangeData, W: Write>(
    msg: NetworkMessage<T>,
    dest: ReceiverEndpoint,
    writer: &mut W,
    scratch: &mut Vec<u8>,
    address: &str,
) {
    scratch.resize(HEADER_SIZE, 0);

    let serialized_len = bincode::serde::encode_into_std_write(&msg, scratch, BINCODE_MESSAGE)
        .unwrap_or_else(|e| panic!("Failed to serialize message to {dest} at {address}: {e:?}",));

    let header = MessageHeader {
        size: serialized_len.try_into().unwrap(),
        replica_id: dest.coord.replica_id,
        sender_block_id: dest.prev_block_id,
    };

    bincode::serde::encode_into_slice(header, &mut scratch[0..HEADER_SIZE], BINCODE_HEADER)
    .unwrap_or_else(|e| {
        panic!(
            "Failed to serialize header of message (was {serialized_len} bytes) to {dest} at {address}: {e:?}",
        )
    });

    assert_eq!(scratch.len(), HEADER_SIZE + serialized_len as usize);

    writer.write_all(scratch).unwrap_or_else(|e| {
        panic!("Failed to send message {serialized_len} bytes to {dest} at {address}: {e:?}",)
    });

    if scratch.len() < scratch.capacity() / 3 {
        scratch.shrink_to(scratch.capacity() / 2);
    }

    get_profiler().net_bytes_out(msg.sender, dest.coord, scratch.len());
}

/// Receive a message from the remote channel. Returns `None` if there was a failure receiving the
/// last message.
///
/// The message won't be deserialized, use `deserialize()`.
pub(crate) fn remote_recv<T: ExchangeData, R: Read>(
    coord: DemuxCoord,
    reader: &mut R,
    scratch: &mut Vec<u8>,
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

    let (header, header_len): (MessageHeader, _) =
        bincode::serde::decode_from_slice(&header, BINCODE_HEADER).expect("malformed header");
    debug_assert_eq!(HEADER_SIZE, header_len);

    scratch.resize(header.size as usize, 0);
    reader.read_exact(&mut scratch[..]).unwrap_or_else(|e| {
        panic!(
            "Failed to receive {} bytes to {} from {}: {:?}",
            header.size, coord, address, e
        )
    });

    let (msg, msg_len): (NetworkMessage<T>, _) =
        bincode::serde::decode_from_slice(scratch, BINCODE_MESSAGE).expect("Malformed message");
    debug_assert_eq!(header.size as usize, msg_len);

    let dest = ReceiverEndpoint::new(
        Coord::new(coord.coord.block_id, coord.coord.host_id, header.replica_id),
        header.sender_block_id,
    );
    get_profiler().net_bytes_in(msg.sender, dest.coord, HEADER_SIZE + header.size as usize);
    Some((dest, msg))
}

#[cfg(test)]
mod tests {
    use super::{MessageHeader, BINCODE_HEADER};
    use crate::network::remote::HEADER_SIZE;
    use bincode::enc::write::SizeWriter;

    #[test]
    fn header_size() {
        let test_headers = vec![
            MessageHeader {
                size: 0,
                replica_id: 0,
                sender_block_id: 0,
            },
            MessageHeader {
                size: 1,
                replica_id: 1,
                sender_block_id: 1,
            },
            MessageHeader {
                size: 1 << 20,
                replica_id: 1 << 10,
                sender_block_id: 200,
            },
            MessageHeader {
                size: u32::MAX - 1,
                replica_id: u64::MAX,
                sender_block_id: u64::MAX,
            },
        ];

        for h in test_headers {
            let mut size_w = SizeWriter::default();
            bincode::serde::encode_into_writer(h, &mut size_w, BINCODE_HEADER).unwrap();

            assert_eq!(HEADER_SIZE, size_w.bytes_written);
        }
    }
}
