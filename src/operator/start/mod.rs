use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

pub(crate) use multiple::*;
pub(crate) use single::*;

use crate::block::BlockStructure;
use crate::channel::RecvTimeoutError;
use crate::network::{Coord, NetworkMessage};
use crate::operator::source::Source;
use crate::operator::start::watermark_frontier::WatermarkFrontier;
use crate::operator::{ExchangeData, IterationStateLock, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::BlockId;

mod multiple;
mod single;
mod watermark_frontier;

/// Trait that abstract the receiving part of the `StartBlock`.
pub(crate) trait StartBlockReceiver<Out>: Clone {
    /// Setup the internal state of the receiver.
    fn setup(&mut self, metadata: ExecutionMetadata);

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

pub(crate) type MultipleStartBlockReceiverOperator<OutL, OutR> =
    StartBlock<TwoSidesItem<OutL, OutR>, MultipleStartBlockReceiver<OutL, OutR>>;

pub(crate) type SingleStartBlockReceiverOperator<Out> =
    StartBlock<Out, SingleStartBlockReceiver<Out>>;

/// Each block should start with a `StartBlock` operator, whose task is to read from the network,
/// receive from the previous operators and handle the watermark frontier.
///
/// There are different kinds of `StartBlock`, the main difference is in the number of previous
/// blocks. With a `SingleStartBlockReceiver` the block is able to receive from the replicas of a
/// single block of the job graph. If the block needs the data from multiple blocks it should use
/// `MultipleStartBlockReceiver` which is able to handle 2 previous blocks.
///
/// Following operators will receive the messages in an unspecified order but the watermark property
/// is followed. Note that the timestamps of the messages are not sorted, it's only guaranteed that
/// when a watermark is emitted, all the previous messages are already been emitted (in some order).
#[derive(Clone, Debug)]
pub(crate) struct StartBlock<Out: ExchangeData, Receiver: StartBlockReceiver<Out> + Send> {
    /// Execution metadata of this block.
    metadata: Option<ExecutionMetadata>,

    /// The actual receiver able to fetch messages from the network.
    receiver: Receiver,
    /// A stash of the received messages ready to be returned.
    buffer: VecDeque<StreamElement<Out>>,

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

impl<Out: ExchangeData> StartBlock<Out, SingleStartBlockReceiver<Out>> {
    /// Create a `StartBlock` able to receive data only from a single previous block.
    pub(crate) fn single(
        previous_block_id: BlockId,
        state_lock: Option<Arc<IterationStateLock>>,
    ) -> SingleStartBlockReceiverOperator<Out> {
        StartBlock::new(SingleStartBlockReceiver::new(previous_block_id), state_lock)
    }
}

impl<OutL: ExchangeData, OutR: ExchangeData>
    StartBlock<TwoSidesItem<OutL, OutR>, MultipleStartBlockReceiver<OutL, OutR>>
{
    /// Create a `StartBlock` able to receive data from 2 previous blocks..
    pub(crate) fn multiple(
        previous_block_id1: BlockId,
        previous_block_id2: BlockId,
        state_lock: Option<Arc<IterationStateLock>>,
    ) -> MultipleStartBlockReceiverOperator<OutL, OutR> {
        StartBlock::new(
            MultipleStartBlockReceiver::new(previous_block_id1, previous_block_id2),
            state_lock,
        )
    }
}

impl<Out: ExchangeData, Receiver: StartBlockReceiver<Out> + Send> StartBlock<Out, Receiver> {
    fn new(receiver: Receiver, state_lock: Option<Arc<IterationStateLock>>) -> Self {
        Self {
            metadata: Default::default(),

            receiver,
            buffer: Default::default(),

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

impl<Out: ExchangeData, Receiver: StartBlockReceiver<Out> + Send> Operator<Out>
    for StartBlock<Out, Receiver>
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.receiver.setup(metadata.clone());

        let prev_replicas = self.receiver.prev_replicas();
        self.num_previous_replicas = prev_replicas.len();
        self.missing_terminate = self.num_previous_replicas - self.receiver.cached_replicas();
        self.missing_flush_and_restart = self.num_previous_replicas;
        self.watermark_frontier = WatermarkFrontier::new(prev_replicas);

        debug!(
            "StartBlock {} of {} initialized",
            metadata.coord,
            std::any::type_name::<Out>()
        );
        self.metadata = Some(metadata);
    }

    fn next(&mut self) -> StreamElement<Out> {
        let metadata = self.metadata.as_ref().unwrap();

        // all the previous blocks sent an end: we're done
        if self.missing_terminate == 0 {
            info!("StartBlock for {} has ended", metadata.coord);
            return StreamElement::Terminate;
        }
        if self.missing_flush_and_restart == 0 {
            info!(
                "StartBlock for {} is emitting flush and restart",
                metadata.coord
            );

            self.missing_flush_and_restart = self.num_previous_replicas;
            self.watermark_frontier.reset();
            // this iteration has ended, before starting the next one wait for the state update
            self.wait_for_state = true;
            self.state_generation += 2;
            return StreamElement::FlushAndRestart;
        }

        while self.buffer.is_empty() {
            let max_delay = metadata.batch_mode.max_delay();
            let buf = match (self.already_timed_out, max_delay) {
                // check the timeout only if there is one and the last time we didn't timed out
                (false, Some(max_delay)) => {
                    match self.receiver.recv_timeout(max_delay) {
                        Ok(buf) => buf,
                        Err(_) => {
                            // timed out: tell the block to flush the current batch
                            // next time we wait indefinitely without the timeout since the batch is
                            // currently empty
                            self.already_timed_out = true;
                            // this is a fake batch, and its sender is meaningless and will be
                            // forget immediately
                            NetworkMessage::new(vec![StreamElement::FlushBatch], Default::default())
                        }
                    }
                }
                _ => {
                    self.already_timed_out = false;
                    self.receiver.recv()
                }
            };

            let sender = buf.sender();
            let batch = buf.batch();

            // use watermarks to update the frontier
            let watermark_frontier = &mut self.watermark_frontier;
            self.buffer = batch
                .into_iter()
                .filter_map(|item| match item {
                    StreamElement::Watermark(ts) => {
                        // update the frontier and return a watermark if necessary
                        watermark_frontier
                            .update(sender, ts)
                            .map(StreamElement::Watermark)
                    }
                    _ => Some(item),
                })
                .collect();
        }

        let message = self
            .buffer
            .pop_front()
            .expect("Previous block sent an empty message");
        if matches!(message, StreamElement::FlushAndRestart) {
            self.missing_flush_and_restart -= 1;
            debug!(
                "{} received an FlushAndRestart, {} more to come",
                metadata.coord, self.missing_flush_and_restart
            );
            return self.next();
        }
        if matches!(message, StreamElement::Terminate) {
            self.missing_terminate -= 1;
            debug!(
                "{} received a Terminate, {} more to come",
                metadata.coord, self.missing_terminate
            );
            return self.next();
        }
        // the previous iteration has ended, this message refers to the new iteration: we need to be
        // sure the state is set before we let this message pass
        if self.wait_for_state {
            if let Some(lock) = self.state_lock.as_ref() {
                lock.wait_for_update(self.state_generation);
            }
            self.wait_for_state = false;
        }
        message
    }

    fn to_string(&self) -> String {
        format!("[{}]", std::any::type_name::<Out>())
    }

    fn structure(&self) -> BlockStructure {
        self.receiver.structure()
    }
}

impl<Out: ExchangeData, Receiver: StartBlockReceiver<Out> + Send> Source<Out>
    for StartBlock<Out, Receiver>
{
    fn get_max_parallelism(&self) -> Option<usize> {
        None
    }
}
