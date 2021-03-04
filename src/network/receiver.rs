use anyhow::{anyhow, Result};
use async_std::channel::{bounded, Receiver, Sender};
use async_std::net::{TcpListener, TcpStream, ToSocketAddrs};
use async_std::stream::StreamExt;
use async_std::task::spawn;
use async_std::task::JoinHandle;

use crate::network::{wait_start, Coord, NetworkSender, NetworkStarter, NetworkStarterRecv};

#[derive(Derivative)]
#[derivative(Debug)]
pub struct NetworkReceiver<In> {
    pub coord: Coord,
    #[derivative(Debug = "ignore")]
    receiver: Receiver<In>,
    #[derivative(Debug = "ignore")]
    local_sender: Sender<In>,
}

impl<In> NetworkReceiver<In>
where
    In: Send + 'static,
{
    pub fn new<A>(coord: Coord, address: A) -> (Self, NetworkStarter, JoinHandle<()>)
    where
        A: ToSocketAddrs + Send + Sync + 'static,
        <A as ToSocketAddrs>::Iter: Send,
    {
        let (sender, receiver) = bounded(1);
        let (bind_socket, bind_socket_rx) = bounded(1);
        let local_sender = sender.clone();
        let join_handle = spawn(async move {
            NetworkReceiver::bind_remote(coord, local_sender, address, bind_socket_rx).await;
        });
        (
            Self {
                coord,
                receiver,
                local_sender: sender,
            },
            bind_socket,
            join_handle,
        )
    }

    pub fn sender(&self) -> NetworkSender<In> {
        NetworkSender::local(self.coord, self.local_sender.clone())
    }

    pub async fn recv(&self) -> Result<In> {
        self.receiver.recv().await.map_err(|e| {
            anyhow!(
                "Failed to receive from local channel at {}: {:?}",
                self.coord,
                e
            )
        })
    }

    async fn bind_remote(
        coord: Coord,
        local_sender: Sender<In>,
        address: impl ToSocketAddrs,
        bind_socket: NetworkStarterRecv,
    ) {
        // wait the signal before binding the socket
        if !wait_start(bind_socket).await {
            debug!(
                "Remote receiver at {} is asked not to bind, exiting...",
                coord
            );
            return;
        }
        // from now we can start binding the socket
        let address: Vec<_> = address
            .to_socket_addrs()
            .await
            .map_err(|e| format!("Failed to get the address for {}: {:?}", coord, e))
            .unwrap()
            .collect();
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
        info!(
            "Remote receiver at {} is ready to accept connection from {:?}",
            coord,
            listener.local_addr()
        );
        let local_sender = local_sender.clone();
        let mut incoming = listener.incoming();
        while let Some(stream) = incoming.next().await {
            let stream = match stream {
                Ok(stream) => stream,
                Err(e) => {
                    warn!("Failed to accept incoming connection at {}: {:?}", coord, e);
                    continue;
                }
            };
            info!(
                "Remote receiver at {} accepted a new connection from {:?}",
                coord,
                stream.peer_addr()
            );
            let local_sender = local_sender.clone();
            spawn(async move { NetworkReceiver::handle_remote_client(local_sender, stream).await });
        }
    }

    async fn handle_remote_client(_local_sender: Sender<In>, _receiver: TcpStream) {
        todo!("receive data, deserialize, send back to local_sender")
    }
}
