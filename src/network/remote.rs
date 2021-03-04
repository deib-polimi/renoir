use async_std::net::TcpStream;
use async_std::prelude::*;
use bincode::config::{FixintEncoding, RejectTrailing, WithOtherIntEncoding, WithOtherTrailing};
use bincode::{DefaultOptions, Options};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::network::Coord;

lazy_static! {
/// Configuration of the header serializer: the integers must have a fixed length encoding.
    static ref HEADER_CONFIG: WithOtherTrailing<WithOtherIntEncoding<DefaultOptions, FixintEncoding>, RejectTrailing> =
        bincode::DefaultOptions::new().with_fixint_encoding().reject_trailing_bytes();
}

lazy_static! {
/// Configuration of the message serializer
    static ref MESSAGE_CONFIG: DefaultOptions = bincode::DefaultOptions::new();
}

/// Header of a message sent before the actual message.
#[derive(Serialize, Deserialize, Default)]
struct MessageHeader {
    /// The size of the actual message
    size: u32,
}

/// Serialize and send a message to a remote socket.
///
/// The network protocol works as follow:
/// - send a `MessageHeader` serialized with bincode with `FixintEncoding`
/// - send the message
pub(crate) async fn remote_send<T>(what: T, coord: Coord, stream: &mut TcpStream)
where
    T: Send + Serialize,
{
    let address = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let serialized = MESSAGE_CONFIG
        .serialize(&what)
        .unwrap_or_else(|e| panic!("Failed to serialize outgoing message to {}: {:?}", coord, e));
    let header = MessageHeader {
        size: serialized.len() as _,
    };
    let serialized_header = HEADER_CONFIG.serialize(&header).unwrap();
    stream
        .write_all(&serialized_header)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "Failed to send size of message (was {} bytes) to {} at {}: {:?}",
                serialized.len(),
                coord,
                address,
                e
            )
        });

    stream.write_all(&serialized).await.unwrap_or_else(|e| {
        panic!(
            "Failed to send {} bytes to {} at {}: {:?}",
            serialized.len(),
            coord,
            address,
            e
        )
    });
}

/// Receive a message from the remote channel. Returns `None` if there was a failure receiving the
/// last message.
pub(crate) async fn remote_recv<T>(coord: Coord, stream: &mut TcpStream) -> Option<T>
where
    T: DeserializeOwned,
{
    let address = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let header_size = HEADER_CONFIG
        .serialized_size(&MessageHeader::default())
        .unwrap() as usize;
    let mut header = vec![0u8; header_size];
    match stream.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) => {
            debug!(
                "Failed to receive {} bytes of header to {} from {}: {:?}",
                header_size, coord, address, e
            );
            return None;
        }
    }
    let header: MessageHeader = HEADER_CONFIG
        .deserialize(&header)
        .expect("Malformed header");
    let mut buf = vec![0u8; header.size as usize];
    stream.read_exact(&mut buf).await.unwrap_or_else(|e| {
        panic!(
            "Failed to receive {} bytes to {} from {}: {:?}",
            header.size, coord, address, e
        )
    });
    Some(MESSAGE_CONFIG.deserialize(&buf).unwrap_or_else(|e| {
        panic!(
            "Failed to deserialize {} bytes to {} from {}: {:?}",
            header.size, coord, address, e
        )
    }))
}
