#[cfg(not(feature = "async-tokio"))]
use std::net::{Shutdown, TcpListener, TcpStream};
#[cfg(not(feature = "async-tokio"))]
use std::thread::JoinHandle;

#[cfg(feature = "async-tokio")]
use tokio::io::AsyncWriteExt;
#[cfg(feature = "async-tokio")]
use tokio::net::{TcpListener, TcpStream};
#[cfg(feature = "async-tokio")]
use tokio::task::JoinHandle;

use anyhow::anyhow;
use std::collections::HashMap;
use std::net::ToSocketAddrs;

use crate::channel::{self, Sender, UnboundedReceiver, UnboundedSender};
use crate::network::remote::remote_recv;
use crate::network::{DemuxCoord, NetworkMessage, ReceiverEndpoint};
use crate::operator::ExchangeData;

/// Like `NetworkReceiver`, but this should be used in a multiplexed channel (i.e. a remote one).
///
/// This receiver is handled in a separate thread that keeps track of the local registered receivers
/// and the open connections. The incoming messages are tagged with the receiver endpoint. Upon
/// arrival they are routed to the correct receiver according to the `ReceiverEndpoint` the message
/// is tagged with.
#[derive(Debug)]
pub(crate) struct DemuxHandle<In: ExchangeData> {
    coord: DemuxCoord,
    /// Tell the dem&ultiplexer that a new receiver is present,
    tx_senders: UnboundedSender<(ReceiverEndpoint, Sender<NetworkMessage<In>>)>,
}

#[cfg(not(feature = "async-tokio"))]
impl<In: ExchangeData> DemuxHandle<In> {
    /// Construct a new `DemultiplexingReceiver` for a block.
    ///
    /// All the local replicas of this block should be registered to this demultiplexer.
    /// `num_client` is the number of multiplexers that will connect to this demultiplexer. Since
    /// the remote senders are all multiplexed this corresponds to the number of remote replicas in
    /// the previous block (relative to the block this demultiplexer refers to).
    pub fn new(
        coord: DemuxCoord,
        address: (String, u16),
        num_clients: usize,
    ) -> (Self, JoinHandle<()>) {
        let (tx_senders, rx_senders) = channel::unbounded();
        let join_handle = std::thread::Builder::new()
            .name(format!(
                "reg-{}:{}-{}",
                coord.coord.host_id, coord.prev_block_id, coord.coord.block_id
            ))
            .spawn(move || bind_remotes(coord, address, num_clients, rx_senders))
            .unwrap();
        (Self { coord, tx_senders }, join_handle)
    }

    /// Register a local receiver to this demultiplexer.
    pub fn register(
        &mut self,
        receiver_endpoint: ReceiverEndpoint,
        sender: Sender<NetworkMessage<In>>,
    ) {
        log::debug!("demux register {} to {}", receiver_endpoint, self.coord);
        self.tx_senders
            .send((receiver_endpoint, sender))
            .unwrap_or_else(|_| panic!("register for {:?} failed", self.coord))
    }
}

/// Bind the socket of this demultiplexer.
#[cfg(not(feature = "async-tokio"))]
fn bind_remotes<In: ExchangeData>(
    coord: DemuxCoord,
    address: (String, u16),
    num_clients: usize,
    rx_senders: UnboundedReceiver<(ReceiverEndpoint, Sender<NetworkMessage<In>>)>,
) {
    let address = (address.0.as_ref(), address.1);
    let address: Vec<_> = address
        .to_socket_addrs()
        .map_err(|e| format!("Failed to get the address for {coord}: {e:?}"))
        .unwrap()
        .collect();

    log::debug!("{coord} binding {}", address[0]);
    let listener = TcpListener::bind(&*address)
        .map_err(|e| {
            anyhow!(
                "Failed to bind socket for {} at {:?}: {:?}",
                coord,
                address,
                e
            )
        })
        .unwrap();
    let address = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    debug!(
        "{} ready at {}, waiting for {} clients",
        coord, address, num_clients
    );

    // the list of JoinHandle of all the spawned threads, including the demultiplexer one
    let mut join_handles = vec![];
    let mut tx_broadcast = vec![];

    let mut incoming = listener.incoming();
    let mut connected_clients = 0;
    while connected_clients < num_clients {
        let stream = incoming.next().unwrap();
        let stream = match stream {
            Ok(stream) => stream,
            Err(e) => {
                warn!("{} to accept incoming connection: {:?}", coord, e);
                continue;
            }
        };
        connected_clients += 1;
        let peer_addr = stream.peer_addr().unwrap();
        debug!(
            "{} new connection from {} ({} / {})",
            coord, peer_addr, connected_clients, num_clients
        );

        let (demux_tx, demux_rx) = channel::unbounded();
        let join_handle = std::thread::Builder::new()
            .name(format!(
                "demux-{}:{}-{}",
                coord.coord.host_id, coord.prev_block_id, coord.coord.block_id
            ))
            .spawn(move || {
                let mut senders = HashMap::new();
                while let Ok((endpoint, sender)) = demux_rx.recv() {
                    senders.insert(endpoint, sender);
                }
                log::debug!("{coord} got senders");
                demux_thread::<In>(coord, senders, stream);
            })
            .unwrap();
        join_handles.push(join_handle);
        tx_broadcast.push(demux_tx);
    }
    log::debug!("{} all clients connected", coord);
    drop(listener);

    // Broadcast senders
    while let Ok(t) = rx_senders.recv() {
        for tx in tx_broadcast.iter() {
            tx.send(t.clone()).unwrap();
        }
    }
    drop(tx_broadcast); // Start all demuxes
    for handle in join_handles {
        handle.join().unwrap();
    }
    log::debug!("{} finished", coord);
}

/// Handle the connection with a remote sender.
///
/// Will deserialize the message upon arrival and send to the corresponding recipient the
/// deserialized data. If the recipient is not yet known, it is waited until it registers.
///
/// # Upgrade path
///
/// Replace send with queue.
///
/// The queue uses a hierarchical queue:
/// + First try to reserve and put the value in the fast queue
/// + If the fast queue is full, put in the slow (unbounded?) queue
/// + Return an enum, either Queued or Overflowed
///
/// if overflowed send a yield request through a second channel
#[cfg(not(feature = "async-tokio"))]
fn demux_thread<In: ExchangeData>(
    coord: DemuxCoord,
    senders: HashMap<ReceiverEndpoint, Sender<NetworkMessage<In>>>,
    mut stream: TcpStream,
) {
    let address = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    log::debug!("{} started", coord);

    // let mut r = std::io::BufReader::new(&mut stream);
    let mut r = &mut stream;

    while let Some((dest, message)) = remote_recv(coord, &mut r, &address) {
        if let Err(e) = senders[&dest].send(message) {
            warn!("demux failed to send message to {}: {:?}", dest, e);
        }
    }

    let _ = stream.shutdown(Shutdown::Both);
    log::debug!("{} finished", coord);
}

#[cfg(feature = "async-tokio")]
impl<In: ExchangeData> DemuxHandle<In> {
    /// Construct a new `DemultiplexingReceiver` for a block.
    ///
    /// All the local replicas of this block should be registered to this demultiplexer.
    /// `num_client` is the number of multiplexers that will connect to this demultiplexer. Since
    /// the remote senders are all multiplexed this corresponds to the number of remote replicas in
    /// the previous block (relative to the block this demultiplexer refers to).
    pub fn new(
        coord: DemuxCoord,
        address: (String, u16),
        num_clients: usize,
    ) -> (Self, JoinHandle<()>) {
        let (tx_senders, rx_senders) = channel::unbounded();

        let join_handle = tokio::spawn(bind_remotes(coord, address, num_clients, rx_senders));
        (Self { coord, tx_senders }, join_handle)
    }

    /// Register a local receiver to this demultiplexer.
    pub fn register(
        &mut self,
        receiver_endpoint: ReceiverEndpoint,
        sender: Sender<NetworkMessage<In>>,
    ) {
        log::debug!(
            "registering {} to the demultiplexer of {}",
            receiver_endpoint,
            self.coord
        );
        self.tx_senders
            .send((receiver_endpoint, sender))
            .unwrap_or_else(|_| panic!("register for {:?} failed", self.coord))
    }
}

/// Bind the socket of this demultiplexer.
#[cfg(feature = "async-tokio")]
async fn bind_remotes<In: ExchangeData>(
    coord: DemuxCoord,
    address: (String, u16),
    num_clients: usize,
    rx_senders: UnboundedReceiver<(ReceiverEndpoint, Sender<NetworkMessage<In>>)>,
) {
    let address = (address.0.as_ref(), address.1);
    let address: Vec<_> = address
        .to_socket_addrs()
        .map_err(|e| format!("Failed to get the address for {}: {:?}", coord, e))
        .unwrap()
        .collect();

    log::debug!("demux binding {}", address[0]);
    let listener = TcpListener::bind(&*address)
        .await
        .map_err(|e| {
            anyhow!(
                "Failed to bind socket for {} at {:?}: {:?}",
                coord,
                address,
                e
            )
        })
        .unwrap();
    let address = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    info!(
        "Remote receiver at {} is ready to accept {} connections to {}",
        coord, num_clients, address
    );

    // the list of JoinHandle of all the spawned threads, including the demultiplexer one
    let mut join_handles = vec![];
    let mut tx_broadcast = vec![];

    let mut connected_clients = 0;
    while connected_clients < num_clients {
        let stream = listener.accept().await;
        let (stream, peer_addr) = match stream {
            Ok(stream) => stream,
            Err(e) => {
                warn!("Failed to accept incoming connection at {}: {:?}", coord, e);
                continue;
            }
        };
        connected_clients += 1;
        info!(
            "Remote receiver at {} accepted a new connection from {} ({} / {})",
            coord, peer_addr, connected_clients, num_clients
        );

        let (demux_tx, demux_rx) = flume::unbounded();
        let join_handle = tokio::spawn(async move {
            let mut senders = HashMap::new();
            while let Ok((endpoint, sender)) = demux_rx.recv_async().await {
                senders.insert(endpoint, sender);
            }
            log::debug!("demux got senders");
            demux_thread::<In>(coord, senders, stream).await;
        });
        join_handles.push(join_handle);
        tx_broadcast.push(demux_tx);
    }
    log::debug!("All connection to {} started, waiting for senders", coord);

    // Broadcast senders
    while let Ok(t) = rx_senders.recv() {
        for tx in tx_broadcast.iter() {
            tx.send(t.clone()).unwrap();
        }
    }
    drop(tx_broadcast); // Start all demuxes
    for handle in join_handles {
        handle.await.unwrap();
    }
    log::debug!("all demuxes for {} finished", coord);
}

/// Handle the connection with a remote sender.
///
/// Will deserialize the message upon arrival and send to the corresponding recipient the
/// deserialized data. If the recipient is not yet known, it is waited until it registers.
///
/// # Upgrade path
///
/// Replace send with queue.
///
/// The queue uses a hierarchical queue:
/// + First try to reserve and put the value in the fast queue
/// + If the fast queue is full, put in the slow (unbounded?) queue
/// + Return an enum, either Queued or Overflowed
///
/// if overflowed send a yield request through a second channel
#[cfg(feature = "async-tokio")]
async fn demux_thread<In: ExchangeData>(
    coord: DemuxCoord,
    senders: HashMap<ReceiverEndpoint, Sender<NetworkMessage<In>>>,
    mut stream: TcpStream,
) {
    let address = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    log::debug!("{} started", coord);

    while let Some((dest, message)) = remote_recv(coord, &mut stream, &address).await {
        if let Err(e) = senders[&dest].send(message) {
            warn!("demux failed to send message to {}: {:?}", dest, e);
        }
    }

    stream.shutdown().await.unwrap();
    log::debug!("{} finished", coord);
}
