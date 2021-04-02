use std::time::Duration;

use anyhow::{anyhow, Result};
use crossbeam_channel::{bounded, select, Receiver, RecvError, RecvTimeoutError, Select, Sender};

use crate::network::{NetworkSender, ReceiverEndpoint};
use crate::operator::Data;

/// The capacity of the in-buffer.
const CHANNEL_CAPACITY: usize = 10;

/// An _either_ type with the result of a select on 2 channels.
#[allow(dead_code)] // TODO: remove once joins are implemented
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SelectResult<In1, In2> {
    /// The result refers to the first selected channel.
    A(Result<In1, RecvError>),
    /// The result refers to the second selected channel.
    B(Result<In2, RecvError>),
}

/// The result of a `NetworkReceiver::select_any`.
pub struct SelectAnyResult<In> {
    /// The actual value returned by the select.
    pub result: Result<In, RecvError>,
    /// The index of the receiver this result refers to.
    pub index: usize,
}

/// The receiving end of a connection between two replicas.
///
/// This works for both a local in-memory connection and for a remote socket connection. This will
/// always be able to listen to a socket. No socket will be bound until a message is sent to the
/// starter returned by the constructor.
///
/// Internally it contains a in-memory sender-receiver pair, to get the local sender call
/// `.sender()`. When the socket will be bound an task will be spawned, it will bind the
/// socket and send to the same in-memory channel the received messages.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct NetworkReceiver<In: Data> {
    /// The ReceiverEndpoint of the current receiver.
    pub receiver_endpoint: ReceiverEndpoint,
    /// The actual receiver where the users of this struct will wait upon.
    #[derivative(Debug = "ignore")]
    receiver: Receiver<In>,
    /// The sender associated with `self.receiver`.
    #[derivative(Debug = "ignore")]
    local_sender: Option<Sender<In>>,
}

impl<In: Data> NetworkReceiver<In> {
    /// Construct a new `NetworkReceiver`.
    ///
    /// To get its sender use `.sender()` for a `NetworkSender` or directly `.local_sender` for the
    /// raw channel.
    pub fn new(receiver_endpoint: ReceiverEndpoint) -> Self {
        let (sender, receiver) = bounded(CHANNEL_CAPACITY);
        Self {
            receiver_endpoint,
            receiver,
            local_sender: Some(sender),
        }
    }

    /// Obtain a `NetworkSender` that will send messages that will arrive to this receiver.
    pub fn sender(&mut self) -> Option<NetworkSender<In>> {
        self.local_sender
            .take()
            .map(|sender| NetworkSender::local(self.receiver_endpoint, sender))
    }

    /// Receive a message from any sender.
    #[allow(dead_code)]
    pub fn recv(&self) -> Result<In> {
        self.receiver.recv().map_err(|e| {
            anyhow!(
                "Failed to receive from channel at {:?}: {:?}",
                self.receiver_endpoint,
                e
            )
        })
    }

    /// Receive a message from any sender with a timeout.
    #[allow(dead_code)]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<In, RecvTimeoutError> {
        self.receiver.recv_timeout(timeout)
    }

    /// Receive a message from any sender of this receiver of the other provided receiver.
    ///
    /// The first message of the two is returned. If both receivers are ready one of them is chosen
    /// randomly (with an unspecified probability). It's guaranteed this function has the eventual
    /// fairness property.
    #[allow(dead_code)] // TODO: remove once joins are implemented
    pub fn select<In2: Data>(&self, other: &NetworkReceiver<In2>) -> SelectResult<In, In2> {
        select! {
            recv(self.receiver) -> elem => SelectResult::A(elem),
            recv(other.receiver) -> elem => SelectResult::B(elem),
        }
    }

    /// Same as `select`, with a timeout.
    #[allow(dead_code)] // TODO: remove once joins are implemented
    pub fn select_timeout<In2: Data>(
        &self,
        other: &NetworkReceiver<In2>,
        timeout: Duration,
    ) -> Result<SelectResult<In, In2>, RecvTimeoutError> {
        select! {
            recv(self.receiver) -> elem => Ok(SelectResult::A(elem)),
            recv(other.receiver) -> elem => Ok(SelectResult::B(elem)),
            default(timeout) => Err(RecvTimeoutError::Timeout)
        }
    }

    /// Same as `select`, but takes multiple receivers to select from.
    pub fn select_any<'a, I: Iterator<Item = &'a NetworkReceiver<In>>>(
        receivers: I,
    ) -> SelectAnyResult<In> {
        let receivers: Vec<_> = receivers.collect();
        // select is pretty expensive, when there is only one receiver select == recv
        if receivers.len() == 1 {
            SelectAnyResult {
                result: receivers[0].receiver.recv(),
                index: 0,
            }
        } else {
            let mut select = Select::new();
            for &receiver in &receivers {
                select.recv(&receiver.receiver);
            }
            let index = select.ready();
            SelectAnyResult {
                index,
                result: receivers[index].receiver.recv(),
            }
        }
    }

    /// Same as `select_timeout`, but takes multiple receivers to select from.
    pub fn select_any_timeout<'a, I: Iterator<Item = &'a NetworkReceiver<In>>>(
        receivers: I,
        timeout: Duration,
    ) -> Result<SelectAnyResult<In>, RecvTimeoutError> {
        let receivers: Vec<_> = receivers.collect();
        // select is pretty expensive, when there is only one receiver select == recv
        if receivers.len() == 1 {
            match receivers[0].receiver.recv_timeout(timeout) {
                Ok(res) => Ok(SelectAnyResult {
                    result: Ok(res),
                    index: 0,
                }),
                Err(RecvTimeoutError::Disconnected) => Ok(SelectAnyResult {
                    result: Err(RecvError),
                    index: 0,
                }),
                Err(RecvTimeoutError::Timeout) => Err(RecvTimeoutError::Timeout),
            }
        } else {
            let mut select = Select::new();
            for &receiver in &receivers {
                select.recv(&receiver.receiver);
            }
            let index = select
                .ready_timeout(timeout)
                .map_err(|_| RecvTimeoutError::Timeout)?;
            Ok(SelectAnyResult {
                index,
                result: receivers[index].receiver.recv(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::network::{Coord, NetworkReceiver, ReceiverEndpoint, SelectResult};

    use super::CHANNEL_CAPACITY;
    use itertools::Itertools;

    #[test]
    fn test_recv_local() {
        let mut receiver = NetworkReceiver::new(ReceiverEndpoint::new(Coord::new(0, 0, 0), 0));
        let sender = receiver.sender().unwrap();

        sender.send(123).unwrap();
        sender.send(456).unwrap();

        drop(sender);

        assert_eq!(receiver.recv().unwrap(), 123);
        assert_eq!(receiver.recv().unwrap(), 456);
        // sender has dropped
        assert!(receiver.recv().is_err());
    }

    #[test]
    fn test_recv_timeout_local() {
        let mut receiver = NetworkReceiver::new(ReceiverEndpoint::new(Coord::new(0, 0, 0), 0));
        let sender = receiver.sender().unwrap();

        sender.send(123).unwrap();

        assert_eq!(
            receiver.recv_timeout(Duration::from_millis(1)).unwrap(),
            123
        );

        assert!(receiver.recv_timeout(Duration::from_millis(50)).is_err());

        sender.send(456).unwrap();
        assert_eq!(
            receiver.recv_timeout(Duration::from_millis(1)).unwrap(),
            456
        );
    }

    #[test]
    fn test_select_local() {
        let mut receiver1 = NetworkReceiver::new(ReceiverEndpoint::new(Coord::new(0, 0, 0), 0));
        let sender1 = receiver1.sender().unwrap();
        let mut receiver2 = NetworkReceiver::new(ReceiverEndpoint::new(Coord::new(0, 0, 0), 0));
        let sender2 = receiver2.sender().unwrap();

        sender1.send(123).unwrap();

        let elem1 = receiver1.select(&receiver2);
        assert_eq!(elem1, SelectResult::A(Ok(123)));

        sender2.send("test".to_string()).unwrap();

        let elem2 = receiver1.select(&receiver2);
        assert_eq!(elem2, SelectResult::B(Ok("test".to_string())));
    }

    #[test]
    fn test_select_timeout_local() {
        let mut receiver1 = NetworkReceiver::new(ReceiverEndpoint::new(Coord::new(0, 0, 0), 0));
        let sender1 = receiver1.sender().unwrap();
        let mut receiver2 = NetworkReceiver::new(ReceiverEndpoint::new(Coord::new(0, 0, 0), 0));
        let sender2 = receiver2.sender().unwrap();

        sender1.send(123).unwrap();

        let elem1 = receiver1
            .select_timeout(&receiver2, Duration::from_millis(1))
            .unwrap();
        assert_eq!(elem1, SelectResult::A(Ok(123)));

        let timeout = receiver1.select_timeout(&receiver2, Duration::from_millis(50));
        assert!(timeout.is_err());

        sender2.send("test".to_string()).unwrap();

        let elem2 = receiver1
            .select_timeout(&receiver2, Duration::from_millis(1))
            .unwrap();
        assert_eq!(elem2, SelectResult::B(Ok("test".to_string())));
    }

    /// This test checks if the `select` function selects randomly between the two channels if they
    /// are both ready. The actual distribution of probability does not really matters in practice,
    /// as long as eventually both channels are selected.
    #[test]
    fn test_select_fairness() {
        // this test has a probability of randomly failing of c!c! / (2c)! where c is
        // CHANNEL_CAPACITY. Repeating this test enough times makes sure to avoid any fluke.
        // If CHANNEL_CAPACITY == 10, with 100 tries the failure probability is ~3x10^-23 (the
        // single try has a failure probability of ~5x10^-6)
        let tries = 100;
        let mut failures = 0;
        for _ in 0..100 {
            let mut receiver1 = NetworkReceiver::new(ReceiverEndpoint::new(Coord::new(0, 0, 0), 0));
            let sender1 = receiver1.sender().unwrap();
            let mut receiver2 = NetworkReceiver::new(ReceiverEndpoint::new(Coord::new(0, 0, 0), 0));
            let sender2 = receiver2.sender().unwrap();

            for _ in 0..CHANNEL_CAPACITY {
                sender1.send(1).unwrap();
                sender2.send(2).unwrap();
            }

            let mut order = Vec::new();

            for _ in 0..2 * CHANNEL_CAPACITY {
                let elem = receiver1.select(&receiver2);
                match elem {
                    SelectResult::A(Ok(_)) => order.push(1),
                    SelectResult::B(Ok(_)) => order.push(2),
                    _ => {}
                }
            }

            let in_order1 = (0..CHANNEL_CAPACITY)
                .map(|_| 1)
                .chain((0..CHANNEL_CAPACITY).map(|_| 2))
                .collect_vec();
            let in_order2 = (0..CHANNEL_CAPACITY)
                .map(|_| 2)
                .chain((0..CHANNEL_CAPACITY).map(|_| 1))
                .collect_vec();

            if order == in_order1 || order == in_order2 {
                failures += 1;
            }
        }
        // if more than 5% of the tries failed, this test fails
        assert!(100 * failures < 5 * tries);
    }

    #[test]
    fn test_select_any_local() {
        let mut receiver1 = NetworkReceiver::new(ReceiverEndpoint::new(Coord::new(0, 0, 0), 0));
        let sender1 = receiver1.sender().unwrap();
        let mut receiver2 = NetworkReceiver::new(ReceiverEndpoint::new(Coord::new(0, 0, 0), 0));
        let sender2 = receiver2.sender().unwrap();

        let receivers = vec![receiver1, receiver2];

        sender1.send(123).unwrap();

        let elem1 = NetworkReceiver::select_any(receivers.iter());
        assert_eq!(elem1.index, 0);
        assert_eq!(elem1.result, Ok(123));

        sender2.send(456).unwrap();

        let elem1 = NetworkReceiver::select_any(receivers.iter());
        assert_eq!(elem1.index, 1);
        assert_eq!(elem1.result, Ok(456));

        drop(sender1);
        drop(sender2);

        let err = NetworkReceiver::select_any(receivers.iter());
        assert!(err.result.is_err());
    }

    #[test]
    fn test_select_any_timeout_local() {
        let mut receiver1 = NetworkReceiver::new(ReceiverEndpoint::new(Coord::new(0, 0, 0), 0));
        let sender1 = receiver1.sender().unwrap();
        let mut receiver2 = NetworkReceiver::new(ReceiverEndpoint::new(Coord::new(0, 0, 0), 0));
        let sender2 = receiver2.sender().unwrap();

        let receivers = vec![receiver1, receiver2];

        sender1.send(123).unwrap();

        let elem1 = NetworkReceiver::select_any_timeout(receivers.iter(), Duration::from_millis(1))
            .unwrap();
        assert_eq!(elem1.index, 0);
        assert_eq!(elem1.result, Ok(123));

        let timeout =
            NetworkReceiver::select_any_timeout(receivers.iter(), Duration::from_millis(50));
        assert!(timeout.is_err());

        sender2.send(456).unwrap();

        let elem1 = NetworkReceiver::select_any_timeout(receivers.iter(), Duration::from_millis(1))
            .unwrap();
        assert_eq!(elem1.index, 1);
        assert_eq!(elem1.result, Ok(456));

        drop(sender1);
        drop(sender2);

        let err = NetworkReceiver::select_any(receivers.iter());
        assert!(err.result.is_err());
    }
}
