//! Wrapper to in-memory channels.
//!
//! This module exists to ease the transition between channel libraries.

use std::time::Duration;

use anyhow::Result;

#[cfg(all(feature = "crossbeam", not(feature = "flume")))]
use crossbeam_channel::{
    bounded, select, unbounded, Receiver, RecvError as ExtRecvError,
    RecvTimeoutError as ExtRecvTimeoutError, Select, Sender,
};
#[cfg(all(not(feature = "crossbeam"), feature = "flume"))]
use flume::{
    bounded, unbounded, Receiver, RecvError as ExtRecvError,
    RecvTimeoutError as ExtRecvTimeoutError, Sender,
};

pub trait ChannelItem: Send + Sync + 'static {}
impl<T: Send + Sync + 'static> ChannelItem for T {}

pub type RecvError = ExtRecvError;
pub type RecvTimeoutError = ExtRecvTimeoutError;

/// An _either_ type with the result of a select on 2 channels.
#[allow(dead_code)] // TODO: remove once joins are implemented
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SelectResult<In1, In2> {
    /// The result refers to the first selected channel.
    A(Result<In1, RecvError>),
    /// The result refers to the second selected channel.
    B(Result<In2, RecvError>),
}

/// The result of a `select_any`.
pub struct SelectAnyResult<In> {
    /// The actual value returned by the select.
    pub result: Result<In, RecvError>,
    /// The index of the receiver this result refers to.
    pub index: usize,
}

#[cfg(all(feature = "crossbeam", not(feature = "flume")))]
#[macro_use]
mod select_impl {
    macro_rules! select_impl {
        ($self:expr, $other:expr) => {
            select! {
                recv($self.0) -> elem => SelectResult::A(elem),
                recv($other.0) -> elem => SelectResult::B(elem),
            }
        };
    }

    macro_rules! select_timeout_impl {
        ($self:expr, $other:expr, $timeout:expr) => {
            select! {
                recv($self.0) -> elem => Ok(SelectResult::A(elem)),
                recv($other.0) -> elem => Ok(SelectResult::B(elem)),
                default($timeout) => Err(RecvTimeoutError::Timeout),
            }
        };
    }

    macro_rules! select_any_impl {
        ($receivers:expr) => {
            // select is pretty expensive, when there is only one receiver select == recv
            if $receivers.len() == 1 {
                SelectAnyResult {
                    result: $receivers.nth(0).unwrap().0.recv(),
                    index: 0,
                }
            } else {
                let mut select = Select::new();
                // need to clone the iterator to avoid consuming it
                let iter = $receivers.clone();
                for receiver in iter {
                    select.recv(&receiver.0);
                }
                let index = select.ready();
                SelectAnyResult {
                    index,
                    result: $receivers.nth(index).unwrap().0.recv(),
                }
            }
        };
    }

    macro_rules! select_any_timeout_impl {
        ($receivers:expr, $timeout:expr) => {
            // select is pretty expensive, when there is only one receiver select == recv
            if $receivers.len() == 1 {
                match $receivers.nth(0).unwrap().0.recv_timeout($timeout) {
                    Ok(res) => Ok(SelectAnyResult {
                        result: Ok(res),
                        index: 0,
                    }),
                    Err(RecvTimeoutError::Disconnected) => Ok(SelectAnyResult {
                        result: Err(RecvError {}),
                        index: 0,
                    }),
                    Err(RecvTimeoutError::Timeout) => Err(RecvTimeoutError::Timeout),
                }
            } else {
                let mut select = Select::new();
                // need to clone the iterator to avoid consuming it
                let iter = $receivers.clone();
                for receiver in iter {
                    select.recv(&receiver.0);
                }
                let index = select
                    .ready_timeout($timeout)
                    .map_err(|_| RecvTimeoutError::Timeout)?;
                Ok(SelectAnyResult {
                    index,
                    result: $receivers.nth(index).unwrap().0.recv(),
                })
            }
        };
    }
}

#[cfg(all(not(feature = "crossbeam"), feature = "flume"))]
#[macro_use]
mod select_impl {
    macro_rules! select_impl {
        ($self:expr, $other:expr) => {{
            flume::Selector::new()
                .recv(&$self.0, |elem| SelectResult::A(elem))
                .recv(&$other.0, |elem| SelectResult::B(elem))
                .wait()
        }};
    }

    macro_rules! select_timeout_impl {
        ($self:expr, $other:expr, $timeout:expr) => {
            flume::Selector::new()
                .recv(&$self.0, |elem| SelectResult::A(elem))
                .recv(&$other.0, |elem| SelectResult::B(elem))
                .wait_timeout($timeout)
                .map_err(|_| RecvTimeoutError::Timeout)
        };
    }

    macro_rules! select_any_impl {
        ($receivers:expr) => {
            // select is pretty expensive, when there is only one receiver select == recv
            if $receivers.len() == 1 {
                SelectAnyResult {
                    result: $receivers.nth(0).unwrap().0.recv(),
                    index: 0,
                }
            } else {
                let mut select = flume::Selector::new();
                for (index, receiver) in $receivers.enumerate() {
                    select = select.recv(&receiver.0, move |elem| SelectAnyResult {
                        result: elem,
                        index,
                    });
                }
                select.wait()
            }
        };
    }

    macro_rules! select_any_timeout_impl {
        ($receivers:expr, $timeout:expr) => {
            // select is pretty expensive, when there is only one receiver select == recv
            if $receivers.len() == 1 {
                match $receivers.nth(0).unwrap().0.recv_timeout($timeout) {
                    Ok(res) => Ok(SelectAnyResult {
                        result: Ok(res),
                        index: 0,
                    }),
                    Err(RecvTimeoutError::Disconnected) => Ok(SelectAnyResult {
                        result: Err(RecvError::Disconnected),
                        index: 0,
                    }),
                    Err(RecvTimeoutError::Timeout) => Err(RecvTimeoutError::Timeout),
                }
            } else {
                let mut select = flume::Selector::new();
                for (index, receiver) in $receivers.enumerate() {
                    select = select.recv(&receiver.0, move |elem| SelectAnyResult {
                        result: elem,
                        index,
                    });
                }
                select
                    .wait_timeout($timeout)
                    .map_err(|_| RecvTimeoutError::Timeout)
            }
        };
    }
}

/// A wrapper on a bounded channel sender.
#[derive(Debug, Clone)]
pub struct BoundedChannelSender<T: ChannelItem>(Sender<T>);
/// A wrapper on a bounded channel receiver.
#[derive(Debug)]
pub struct BoundedChannelReceiver<T: ChannelItem>(Receiver<T>);

impl<T: ChannelItem> BoundedChannelSender<T> {
    /// Send a message in the channel, blocking if it's full.
    #[inline]
    pub fn send(&self, item: T) -> Result<()> {
        Ok(self.0.send(item)?)
    }
}

impl<T: ChannelItem> BoundedChannelReceiver<T> {
    /// Crate a new pair sender/receiver with limited capacity.
    pub fn new(cap: usize) -> (BoundedChannelSender<T>, BoundedChannelReceiver<T>) {
        let (sender, receiver) = bounded(cap);
        (
            BoundedChannelSender(sender),
            BoundedChannelReceiver(receiver),
        )
    }

    /// Block until a message is present in the channel and return it when ready.
    #[inline]
    pub fn recv(&self) -> Result<T, RecvError> {
        self.0.recv()
    }

    /// Block until a message is present in the channel and return it when ready.
    ///
    /// If the timeout expires an error is returned.
    #[inline]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.0.recv_timeout(timeout)
    }

    /// Receive a message from any sender of this receiver of the other provided receiver.
    ///
    /// The first message of the two is returned. If both receivers are ready one of them is chosen
    /// randomly (with an unspecified probability). It's guaranteed this function has the eventual
    /// fairness property.
    #[inline]
    pub fn select<T2: ChannelItem>(
        &self,
        other: &BoundedChannelReceiver<T2>,
    ) -> SelectResult<T, T2> {
        select_impl!(self, other)
    }

    /// Same as `select`, with a timeout.
    #[inline]
    pub fn select_timeout<T2: ChannelItem>(
        &self,
        other: &BoundedChannelReceiver<T2>,
        timeout: Duration,
    ) -> Result<SelectResult<T, T2>, RecvTimeoutError> {
        select_timeout_impl!(self, other, timeout)
    }

    /// Same as `select`, but takes multiple receivers to select from.
    #[allow(clippy::unnecessary_mut_passed)]
    #[inline]
    pub fn select_any<'a, I>(mut receivers: I) -> SelectAnyResult<T>
    where
        I: ExactSizeIterator + Iterator<Item = &'a BoundedChannelReceiver<T>> + Clone,
    {
        select_any_impl!(&mut receivers)
    }

    /// Same as `select_timeout`, but takes multiple receivers to select from.
    #[allow(clippy::unnecessary_mut_passed)]
    #[inline]
    pub fn select_any_timeout<'a, I>(
        mut receivers: I,
        timeout: Duration,
    ) -> Result<SelectAnyResult<T>, RecvTimeoutError>
    where
        I: ExactSizeIterator + Iterator<Item = &'a BoundedChannelReceiver<T>> + Clone,
    {
        select_any_timeout_impl!(&mut receivers, timeout)
    }
}

/// A wrapper on an unbounded channel sender.
#[derive(Debug, Clone)]
pub struct UnboundedChannelSender<T: ChannelItem>(Sender<T>);
/// A wrapper on an unbounded channel receiver.
#[derive(Debug)]
pub struct UnboundedChannelReceiver<T: ChannelItem>(Receiver<T>);

impl<T: ChannelItem> UnboundedChannelSender<T> {
    /// Send a message in the channel.
    #[inline]
    pub fn send(&self, item: T) -> Result<()> {
        Ok(self.0.send(item)?)
    }
}

impl<T: ChannelItem> UnboundedChannelReceiver<T> {
    /// Crate a new pair sender/receiver with unlimited capacity.
    pub fn new() -> (UnboundedChannelSender<T>, UnboundedChannelReceiver<T>) {
        let (sender, receiver) = unbounded();
        (
            UnboundedChannelSender(sender),
            UnboundedChannelReceiver(receiver),
        )
    }

    /// Block until a message is present in the channel and return it when ready.
    #[inline]
    pub fn recv(&self) -> Result<T, RecvError> {
        self.0.recv()
    }

    /// Block until a message is present in the channel and return it when ready.
    ///
    /// If the timeout expires an error is returned.
    #[inline]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.0.recv_timeout(timeout)
    }

    /// Receive a message from any sender of this receiver of the other provided receiver.
    ///
    /// The first message of the two is returned. If both receivers are ready one of them is chosen
    /// randomly (with an unspecified probability). It's guaranteed this function has the eventual
    /// fairness property.
    #[inline]
    pub fn select<T2: ChannelItem>(
        &self,
        other: &UnboundedChannelReceiver<T2>,
    ) -> SelectResult<T, T2> {
        select_impl!(self, other)
    }

    /// Same as `select`, with a timeout.
    #[inline]
    pub fn select_timeout<T2: ChannelItem>(
        &self,
        other: &UnboundedChannelReceiver<T2>,
        timeout: Duration,
    ) -> Result<SelectResult<T, T2>, RecvTimeoutError> {
        select_timeout_impl!(self, other, timeout)
    }

    /// Same as `select`, but takes multiple receivers to select from.
    #[allow(clippy::unnecessary_mut_passed)]
    #[inline]
    pub fn select_any<'a, I>(mut receivers: I) -> SelectAnyResult<T>
    where
        I: ExactSizeIterator + Iterator<Item = &'a BoundedChannelReceiver<T>> + Clone,
    {
        select_any_impl!(&mut receivers)
    }

    /// Same as `select_timeout`, but takes multiple receivers to select from.
    #[allow(clippy::unnecessary_mut_passed)]
    #[inline]
    pub fn select_any_timeout<'a, I>(
        mut receivers: I,
        timeout: Duration,
    ) -> Result<SelectAnyResult<T>, RecvTimeoutError>
    where
        I: ExactSizeIterator + Iterator<Item = &'a BoundedChannelReceiver<T>> + Clone,
    {
        select_any_timeout_impl!(&mut receivers, timeout)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use itertools::Itertools;

    use crate::channel::{BoundedChannelReceiver, SelectResult};

    const CHANNEL_CAPACITY: usize = 10;

    #[test]
    fn test_recv_local() {
        let (sender, receiver) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);

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
        let (sender, receiver) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);

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
        let (sender1, receiver1) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);
        let (sender2, receiver2) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);

        sender1.send(123).unwrap();

        let elem1 = receiver1.select(&receiver2);
        assert_eq!(elem1, SelectResult::A(Ok(123)));

        sender2.send("test".to_string()).unwrap();

        let elem2 = receiver1.select(&receiver2);
        assert_eq!(elem2, SelectResult::B(Ok("test".to_string())));
    }

    #[test]
    fn test_select_timeout_local() {
        let (sender1, receiver1) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);
        let (sender2, receiver2) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);

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
            let (sender1, receiver1) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);
            let (sender2, receiver2) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);

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
        let (sender1, receiver1) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);
        let (sender2, receiver2) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);

        let receivers = vec![receiver1, receiver2];

        sender1.send(123).unwrap();

        let elem1 = BoundedChannelReceiver::select_any(receivers.iter());
        assert_eq!(elem1.index, 0);
        assert_eq!(elem1.result, Ok(123));

        sender2.send(456).unwrap();

        let elem1 = BoundedChannelReceiver::select_any(receivers.iter());
        assert_eq!(elem1.index, 1);
        assert_eq!(elem1.result, Ok(456));

        drop(sender1);
        drop(sender2);

        let err = BoundedChannelReceiver::select_any(receivers.iter());
        assert!(err.result.is_err());
    }

    #[test]
    fn test_select_any_timeout_local() {
        let (sender1, receiver1) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);
        let (sender2, receiver2) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);

        let receivers = vec![receiver1, receiver2];

        sender1.send(123).unwrap();

        let elem1 =
            BoundedChannelReceiver::select_any_timeout(receivers.iter(), Duration::from_millis(1))
                .unwrap();
        assert_eq!(elem1.index, 0);
        assert_eq!(elem1.result, Ok(123));

        let timeout =
            BoundedChannelReceiver::select_any_timeout(receivers.iter(), Duration::from_millis(50));
        assert!(timeout.is_err());

        sender2.send(456).unwrap();

        let elem1 =
            BoundedChannelReceiver::select_any_timeout(receivers.iter(), Duration::from_millis(1))
                .unwrap();
        assert_eq!(elem1.index, 1);
        assert_eq!(elem1.result, Ok(456));

        drop(sender1);
        drop(sender2);

        let err = BoundedChannelReceiver::select_any(receivers.iter());
        assert!(err.result.is_err());
    }
}
