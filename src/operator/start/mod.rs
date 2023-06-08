use std::fmt::{Debug, Display};
use std::sync::Arc;
use std::time::Duration;

pub(crate) use binary::*;
pub(crate) use simple::*;

#[cfg(feature = "timestamp")]
use super::Timestamp;
use crate::block::{BlockStructure, Replication};
use crate::channel::RecvTimeoutError;
use crate::network::{Coord, NetworkDataIterator, NetworkMessage};
use crate::operator::iteration::IterationStateLock;
use crate::operator::source::Source;
use crate::operator::start::watermark_frontier::WatermarkFrontier;
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::scheduler::{BlockId, ExecutionMetadata};

mod binary;
mod simple;
mod watermark_frontier;

/// Trait that abstract the receiving part of the `Start`.
pub(crate) trait StartReceiver<Out>: Clone {
    /// Setup the internal state of the receiver.
    fn setup(&mut self, metadata: &mut ExecutionMetadata);

    /// Obtain the list of all the previous replicas.
    ///
    /// This list should contain all the replicas this receiver will receive data from.
    fn prev_replicas(&self) -> Vec<Coord>;

    /// The number of those replicas which are behind a cache, and therefore never will emit a
    /// `StreamElement::Terminate` message.
    fn cached_replicas(&self) -> usize;

    /// Try to receive a batch from the previous blocks, or fail with an error if the timeout
    /// expires.
    fn recv_timeout(&mut self, timeout: Duration) -> Result<NetworkMessage<Out>, RecvTimeoutError>;

    /// Receive a batch from the previous blocks waiting indefinitely.
    fn recv(&mut self) -> NetworkMessage<Out>;

    /// Like `Operator::structure`.
    fn structure(&self) -> BlockStructure;
}

pub(crate) type BinaryStartOperator<OutL, OutR> =
    Start<BinaryElement<OutL, OutR>, BinaryStartReceiver<OutL, OutR>>;

pub(crate) type SimpleStartOperator<Out> = Start<Out, SimpleStartReceiver<Out>>;

/// Each block should start with a `Start` operator, whose task is to read from the network,
/// receive from the previous operators and handle the watermark frontier.
///
/// There are different kinds of `Start`, the main difference is in the number of previous
/// blocks. With a `SimpleStartReceiver` the block is able to receive from the replicas of a
/// single block of the job graph. If the block needs the data from multiple blocks it should use
/// `MultipleStartReceiver` which is able to handle 2 previous blocks.
///
/// Following operators will receive the messages in an unspecified order but the watermark property
/// is followed. Note that the timestamps of the messages are not sorted, it's only guaranteed that
/// when a watermark is emitted, all the previous messages are already been emitted (in some order).
#[derive(Clone, Debug)]
pub(crate) struct Start<Out: ExchangeData, Receiver: StartReceiver<Out> + Send> {
    /// Execution metadata of this block.
    max_delay: Option<Duration>,

    coord: Option<Coord>,

    /// The actual receiver able to fetch messages from the network.
    receiver: Receiver,

    /// Inner iterator over batch items, contains coordinate of the sender
    batch_iter: Option<(Coord, NetworkDataIterator<StreamElement<Out>>)>,

    /// The number of `StreamElement::Terminate` messages yet to be received. When this value
    /// reaches zero this operator will emit the terminate.
    missing_terminate: usize,
    /// The number of `StreamElement::FlushAndRestart` messages yet to be received.
    missing_flush_and_restart: usize,
    /// The total number of replicas in the previous blocks. This is used for resetting
    /// `missing_flush_and_restart`.
    num_previous_replicas: usize,

    /// Whether the previous blocks timed out and the last batch has been flushed.
    ///
    /// The next time `next()` is called it will not wait the timeout asked by the batch mode.
    already_timed_out: bool,

    /// The current frontier of the watermarks from the previous replicas.
    watermark_frontier: WatermarkFrontier,

    /// Whether the iteration has ended and the current block has to wait for the local iteration
    /// leader to update the iteration state before letting the messages pass.
    wait_for_state: bool,
    state_lock: Option<Arc<IterationStateLock>>,
    state_generation: usize,
}

impl<Out: ExchangeData, Receiver: StartReceiver<Out> + Send> Display for Start<Out, Receiver> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]", std::any::type_name::<Out>())
    }
}

impl<Out: ExchangeData> Start<Out, SimpleStartReceiver<Out>> {
    /// Create a `Start` able to receive data only from a single previous block.
    pub(crate) fn single(
        previous_block_id: BlockId,
        state_lock: Option<Arc<IterationStateLock>>,
    ) -> SimpleStartOperator<Out> {
        Start::new(SimpleStartReceiver::new(previous_block_id), state_lock)
    }
}

impl<OutL: ExchangeData, OutR: ExchangeData>
    Start<BinaryElement<OutL, OutR>, BinaryStartReceiver<OutL, OutR>>
{
    /// Create a `Start` able to receive data from 2 previous blocks, setting up the cache.
    pub(crate) fn multiple(
        previous_block_id1: BlockId,
        previous_block_id2: BlockId,
        left_cache: bool,
        right_cache: bool,
        state_lock: Option<Arc<IterationStateLock>>,
    ) -> BinaryStartOperator<OutL, OutR> {
        Start::new(
            BinaryStartReceiver::new(
                previous_block_id1,
                previous_block_id2,
                left_cache,
                right_cache,
            ),
            state_lock,
        )
    }
}

impl<Out: ExchangeData, Receiver: StartReceiver<Out> + Send> Start<Out, Receiver> {
    fn new(receiver: Receiver, state_lock: Option<Arc<IterationStateLock>>) -> Self {
        Self {
            coord: Default::default(),
            max_delay: Default::default(),

            receiver,
            batch_iter: None,

            missing_terminate: Default::default(),
            missing_flush_and_restart: Default::default(),
            num_previous_replicas: 0,

            already_timed_out: Default::default(),

            watermark_frontier: Default::default(),

            wait_for_state: Default::default(),
            state_lock,
            state_generation: Default::default(),
        }
    }

    pub(crate) fn receiver(&self) -> &Receiver {
        &self.receiver
    }
}

impl<Out: ExchangeData, Receiver: StartReceiver<Out> + Send> Operator<Out>
    for Start<Out, Receiver>
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.receiver.setup(metadata);

        let prev_replicas = self.receiver.prev_replicas();
        self.num_previous_replicas = prev_replicas.len();
        self.missing_terminate = self.num_previous_replicas;
        self.missing_flush_and_restart = self.num_previous_replicas;
        self.watermark_frontier = WatermarkFrontier::new(prev_replicas);

        log::trace!(
            "{} initialized <{}>",
            metadata.coord,
            std::any::type_name::<Out>()
        );
        self.coord = Some(metadata.coord);
        self.max_delay = metadata.batch_mode.max_delay();
    }

    fn next(&mut self) -> StreamElement<Out> {
        let coord = self.coord.unwrap();

        loop {
            // all the previous blocks sent an end: we're done
            if self.missing_terminate == 0 {
                log::trace!("{} ended", coord);
                return StreamElement::Terminate;
            }
            if self.missing_flush_and_restart == 0 {
                log::trace!("{} flush_restart", coord);

                self.missing_flush_and_restart = self.num_previous_replicas;
                self.watermark_frontier.reset();
                // this iteration has ended, before starting the next one wait for the state update
                self.wait_for_state = true;
                self.state_generation += 2;
                return StreamElement::FlushAndRestart;
            }

            if let Some((sender, ref mut inner)) = self.batch_iter {
                let msg = match inner.next() {
                    None => {
                        // Current batch is finished
                        self.batch_iter = None;
                        continue;
                    }
                    Some(item) => {
                        match item {
                            StreamElement::Watermark(ts) => {
                                // update the frontier and return a watermark if necessary
                                match self.watermark_frontier.update(sender, ts) {
                                    Some(ts) => StreamElement::Watermark(ts), // ts is safe
                                    None => continue,
                                }
                            }
                            StreamElement::FlushAndRestart => {
                                // mark this replica as ended and let the frontier ignore it from now on
                                #[cfg(feature = "timestamp")]
                                {
                                    self.watermark_frontier.update(sender, Timestamp::MAX);
                                }
                                self.missing_flush_and_restart -= 1;
                                continue;
                            }
                            StreamElement::Terminate => {
                                self.missing_terminate -= 1;
                                log::trace!(
                                    "{} received terminate, {} left",
                                    coord,
                                    self.missing_terminate
                                );
                                continue;
                            }
                            _ => item,
                        }
                    }
                };

                // the previous iteration has ended, this message refers to the new iteration: we need to be
                // sure the state is set before we let this message pass
                if self.wait_for_state {
                    if let Some(lock) = self.state_lock.as_ref() {
                        lock.wait_for_update(self.state_generation);
                    }
                    self.wait_for_state = false;
                }
                return msg;
            }

            // Receive next batch
            let net_msg = match (self.already_timed_out, self.max_delay) {
                // check the timeout only if there is one and the last time we didn't timed out
                (false, Some(max_delay)) => {
                    match self.receiver.recv_timeout(max_delay) {
                        Ok(net_msg) => net_msg,
                        Err(_) => {
                            // timed out: tell the block to flush the current batch
                            // next time we wait indefinitely without the timeout since the batch is
                            // currently empty
                            self.already_timed_out = true;
                            // this is a fake batch, and its sender is meaningless and will be
                            // forget immediately
                            NetworkMessage::new_single(
                                StreamElement::FlushBatch,
                                Default::default(),
                            )
                        }
                    }
                }
                _ => {
                    self.already_timed_out = false;
                    self.receiver.recv()
                }
            };

            self.batch_iter = Some((net_msg.sender(), net_msg.into_iter()));
        }
    }

    fn structure(&self) -> BlockStructure {
        self.receiver.structure()
    }
}

impl<Out: ExchangeData, Receiver: StartReceiver<Out> + Send> Source<Out> for Start<Out, Receiver> {
    fn replication(&self) -> Replication {
        Replication::Unlimited
    }
}

#[cfg(test)]
mod tests {
    use crate::network::NetworkMessage;
    use crate::operator::{BinaryElement, Operator, Start, StreamElement, Timestamp};
    use crate::test::FakeNetworkTopology;

    #[cfg(feature = "timestamp")]
    fn ts(millis: u64) -> Timestamp {
        millis as i64
    }

    #[test]
    fn test_single() {
        let mut t = FakeNetworkTopology::new(1, 2);
        let (from1, sender1) = t.senders_mut()[0].pop().unwrap();
        let (from2, sender2) = t.senders_mut()[0].pop().unwrap();

        let mut start_block =
            Start::<i32, _>::single(sender1.receiver_endpoint.prev_block_id, None);
        start_block.setup(&mut t.metadata());

        sender1
            .send(NetworkMessage::new_batch(
                vec![StreamElement::Item(42), StreamElement::FlushAndRestart],
                from1,
            ))
            .unwrap();

        assert_eq!(StreamElement::Item(42), start_block.next());
        assert_eq!(StreamElement::FlushBatch, start_block.next());

        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart],
                from2,
            ))
            .unwrap();

        assert_eq!(StreamElement::FlushAndRestart, start_block.next());

        sender1
            .send(NetworkMessage::new_single(StreamElement::Terminate, from1))
            .unwrap();
        sender2
            .send(NetworkMessage::new_single(StreamElement::Terminate, from2))
            .unwrap();

        assert_eq!(StreamElement::Terminate, start_block.next());
    }

    #[test]
    #[cfg(feature = "timestamp")]
    fn test_single_watermark() {
        let mut t = FakeNetworkTopology::new(1, 2);
        let (from1, sender1) = t.senders_mut()[0].pop().unwrap();
        let (from2, sender2) = t.senders_mut()[0].pop().unwrap();

        let mut start_block =
            Start::<i32, _>::single(sender1.receiver_endpoint.prev_block_id, None);
        start_block.setup(&mut t.metadata());

        sender1
            .send(NetworkMessage::new_batch(
                vec![
                    StreamElement::Timestamped(42, ts(10)),
                    StreamElement::Watermark(ts(20)),
                ],
                from1,
            ))
            .unwrap();

        assert_eq!(StreamElement::Timestamped(42, ts(10)), start_block.next());
        assert_eq!(StreamElement::FlushBatch, start_block.next());

        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::Watermark(ts(100))],
                from2,
            ))
            .unwrap();

        assert_eq!(StreamElement::Watermark(ts(20)), start_block.next());
        assert_eq!(StreamElement::FlushBatch, start_block.next());

        sender1
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart],
                from1,
            ))
            .unwrap();
        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::Watermark(ts(110))],
                from2,
            ))
            .unwrap();
        assert_eq!(StreamElement::Watermark(ts(110)), start_block.next());
    }

    #[test]
    #[cfg(feature = "timestamp")]
    fn test_multiple_no_cache() {
        let mut t = FakeNetworkTopology::new(2, 1);
        let (from1, sender1) = t.senders_mut()[0].pop().unwrap();
        let (from2, sender2) = t.senders_mut()[1].pop().unwrap();

        let mut start_block = Start::<BinaryElement<i32, i32>, _>::multiple(
            from1.block_id,
            from2.block_id,
            false,
            false,
            None,
        );
        start_block.setup(&mut t.metadata());

        sender1
            .send(NetworkMessage::new_batch(
                vec![
                    StreamElement::Timestamped(42, ts(10)),
                    StreamElement::Watermark(ts(20)),
                ],
                from1,
            ))
            .unwrap();

        assert_eq!(
            StreamElement::Timestamped(BinaryElement::Left(42), ts(10)),
            start_block.next()
        );
        assert_eq!(StreamElement::FlushBatch, start_block.next());

        sender2
            .send(NetworkMessage::new_batch(
                vec![
                    StreamElement::Timestamped(69, ts(10)),
                    StreamElement::Watermark(ts(20)),
                ],
                from2,
            ))
            .unwrap();

        assert_eq!(
            StreamElement::Timestamped(BinaryElement::Right(69), ts(10)),
            start_block.next()
        );
        assert_eq!(StreamElement::Watermark(ts(20)), start_block.next());

        sender1
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart],
                from1,
            ))
            .unwrap();
        assert_eq!(
            StreamElement::Item(BinaryElement::LeftEnd),
            start_block.next()
        );
        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart],
                from2,
            ))
            .unwrap();
        assert_eq!(
            StreamElement::Item(BinaryElement::RightEnd),
            start_block.next()
        );
        assert_eq!(StreamElement::FlushAndRestart, start_block.next());
    }

    #[test]
    fn test_multiple_cache() {
        let mut t = FakeNetworkTopology::new(2, 1);
        let (from1, sender1) = t.senders_mut()[0].pop().unwrap();
        let (from2, sender2) = t.senders_mut()[1].pop().unwrap();

        let mut start_block = Start::<BinaryElement<i32, i32>, _>::multiple(
            from1.block_id,
            from2.block_id,
            true,
            false,
            None,
        );
        start_block.setup(&mut t.metadata());

        sender1
            .send(NetworkMessage::new_single(StreamElement::Item(42), from1))
            .unwrap();
        sender1
            .send(NetworkMessage::new_single(StreamElement::Item(43), from1))
            .unwrap();
        sender2
            .send(NetworkMessage::new_single(StreamElement::Item(69), from2))
            .unwrap();

        let mut recv = [start_block.next(), start_block.next(), start_block.next()];
        recv.sort(); // those messages can arrive unordered
        assert_eq!(StreamElement::Item(BinaryElement::Left(42)), recv[0]);
        assert_eq!(StreamElement::Item(BinaryElement::Left(43)), recv[1]);
        assert_eq!(StreamElement::Item(BinaryElement::Right(69)), recv[2]);

        sender1
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart, StreamElement::Terminate],
                from1,
            ))
            .unwrap();
        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart],
                from2,
            ))
            .unwrap();

        let mut recv = [start_block.next(), start_block.next()];
        recv.sort(); // those messages can arrive unordered
        assert_eq!(StreamElement::Item(BinaryElement::LeftEnd), recv[0]);
        assert_eq!(StreamElement::Item(BinaryElement::RightEnd), recv[1]);

        assert_eq!(StreamElement::FlushAndRestart, start_block.next());

        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::Item(6969), StreamElement::FlushAndRestart],
                from2,
            ))
            .unwrap();

        let mut recv = [
            start_block.next(),
            start_block.next(),
            start_block.next(),
            start_block.next(),
            start_block.next(),
        ];
        recv.sort(); // those messages can arrive unordered
        assert_eq!(StreamElement::Item(BinaryElement::Left(42)), recv[0]);
        assert_eq!(StreamElement::Item(BinaryElement::Left(43)), recv[1]);
        assert_eq!(StreamElement::Item(BinaryElement::Right(6969)), recv[2]);
        assert_eq!(StreamElement::Item(BinaryElement::LeftEnd), recv[3]);
        assert_eq!(StreamElement::Item(BinaryElement::RightEnd), recv[4]);

        assert_eq!(StreamElement::FlushAndRestart, start_block.next());

        sender2
            .send(NetworkMessage::new_single(StreamElement::Terminate, from2))
            .unwrap();

        assert_eq!(StreamElement::Terminate, start_block.next());
    }

    #[test]
    fn test_multiple_cache_other_side() {
        let mut t = FakeNetworkTopology::new(2, 1);
        let (from1, sender1) = t.senders_mut()[0].pop().unwrap();
        let (from2, sender2) = t.senders_mut()[1].pop().unwrap();

        let mut start_block = Start::<BinaryElement<i32, i32>, _>::multiple(
            from1.block_id,
            from2.block_id,
            false,
            true,
            None,
        );
        start_block.setup(&mut t.metadata());

        sender1
            .send(NetworkMessage::new_single(StreamElement::Item(42), from1))
            .unwrap();
        sender1
            .send(NetworkMessage::new_single(StreamElement::Item(43), from1))
            .unwrap();
        sender2
            .send(NetworkMessage::new_single(StreamElement::Item(69), from2))
            .unwrap();

        let mut recv = [start_block.next(), start_block.next(), start_block.next()];
        recv.sort(); // those messages can arrive unordered
        assert_eq!(StreamElement::Item(BinaryElement::Left(42)), recv[0]);
        assert_eq!(StreamElement::Item(BinaryElement::Left(43)), recv[1]);
        assert_eq!(StreamElement::Item(BinaryElement::Right(69)), recv[2]);

        sender1
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart, StreamElement::Terminate],
                from1,
            ))
            .unwrap();
        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart],
                from2,
            ))
            .unwrap();

        let mut recv = [start_block.next(), start_block.next()];
        recv.sort(); // those messages can arrive unordered
        assert_eq!(StreamElement::Item(BinaryElement::LeftEnd), recv[0]);
        assert_eq!(StreamElement::Item(BinaryElement::RightEnd), recv[1]);

        assert_eq!(StreamElement::FlushAndRestart, start_block.next());

        sender2
            .send(NetworkMessage::new_single(StreamElement::Terminate, from2))
            .unwrap();

        assert_eq!(StreamElement::Terminate, start_block.next());
    }
}
