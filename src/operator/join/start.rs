use std::any::TypeId;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::block::{BatchMode, BlockStructure, OperatorReceiver, OperatorStructure};
use crate::channel::SelectResult;
use crate::network::{NetworkMessage, NetworkReceiver, NetworkTopology, ReceiverEndpoint};
use crate::operator::{Data, ExchangeData, IterationStateLock, KeyerFn, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{BlockId, KeyValue};

/// A handy type for merging the two streams coming to a join.
///
/// `LeftEnd` and `RightEnd` are used to mark the end of their respective sides: it's equivalent to
/// a `StreamElement::FlushAndRestart` for one of the sides.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum JoinElement<Key, Out1, Out2> {
    /// An element from the left side.
    Left(KeyValue<Key, Out1>),
    /// An element from the right side.
    Right(KeyValue<Key, Out2>),
    /// The left side has ended.
    LeftEnd,
    /// The right side has ended.
    RightEnd,
}

/// Utility struct that handles the receiver of a single side.
#[derive(Debug, Derivative)]
#[derivative(Clone)]
struct SideReceiver<Key: Data, Out: ExchangeData, Keyer>
where
    Keyer: KeyerFn<Key, Out>,
{
    /// The actual receiver from which data is received.
    #[derivative(Clone(clone_with = "clone_none"))]
    receiver: Option<NetworkReceiver<Out>>,
    /// The id of the block that sends data into this side.
    prev_block_id: BlockId,

    /// The number of `StreamElement::Terminate` items yet to be received.
    missing_terminate: usize,
    /// The number of `StreamElement::FlushAndRestart` items yet to be received.
    missing_flush_and_restart: usize,
    /// The number of previous replicas the will send data to this one.
    num_prev: usize,

    /// The keyer function for extracting the key from a message of this side.
    keyer: Keyer,

    _k: PhantomData<Key>,
}

fn clone_none<T>(_: &Option<T>) -> Option<T> {
    None
}

impl<Key: Data, Out: ExchangeData, Keyer> SideReceiver<Key, Out, Keyer>
where
    Keyer: KeyerFn<Key, Out>,
{
    fn new(keyer: Keyer, prev_block_id: BlockId) -> Self {
        Self {
            receiver: None,
            missing_terminate: 0,
            missing_flush_and_restart: 0,
            keyer,
            prev_block_id,
            num_prev: 0,
            _k: Default::default(),
        }
    }

    fn setup(&mut self, metadata: &ExecutionMetadata, network: &mut NetworkTopology) {
        let endpoint = ReceiverEndpoint::new(metadata.coord, self.prev_block_id);
        let receiver = network.get_receiver(endpoint);
        self.receiver = Some(receiver);
        let in_type = TypeId::of::<Out>();
        for &(prev, typ) in metadata.prev.iter() {
            // ignore the data of the wrong type
            if in_type != typ {
                continue;
            }
            if prev.block_id == self.prev_block_id {
                self.num_prev += 1;
            }
        }
        self.missing_terminate = self.num_prev;
        self.reset();
    }

    /// Reset this struct, making it ready for the next iteration.
    fn reset(&mut self) {
        self.missing_flush_and_restart = self.num_prev;
    }

    /// Whether this operator has ended, i.e. all the previous operators have sent all the data.
    fn is_ended(&self) -> bool {
        self.missing_flush_and_restart == 0
    }

    /// Whether this operator has received `Terminate` from all the previous replicas.
    fn is_terminated(&self) -> bool {
        self.missing_terminate == 0
    }
}

/// The starting operator of a block with a join.
///
/// This will receive data from the two sides and merge the two streams into a single one that the
/// next operator (the local join operator) will be able to use more easily.
#[derive(Clone, Debug)]
pub(crate) struct JoinStartBlock<Key: Data, Out1: ExchangeData, Out2: ExchangeData, Keyer1, Keyer2>
where
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    /// The receiver for the left side.
    left: SideReceiver<Key, Out1, Keyer1>,
    /// The receiver for the right side.
    right: SideReceiver<Key, Out2, Keyer2>,
    /// Whether a timeout occurred so the operator flushes the batch and blocks for new messages.
    timed_out: bool,

    state_lock: Option<Arc<IterationStateLock>>,
    state_generation: usize,
    wait_for_state: bool,

    metadata: Option<ExecutionMetadata>,
    buffer: VecDeque<StreamElement<JoinElement<Key, Out1, Out2>>>,
}

impl<Key: Data, Out1: ExchangeData, Out2: ExchangeData, Keyer1, Keyer2>
    JoinStartBlock<Key, Out1, Out2, Keyer1, Keyer2>
where
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    pub(crate) fn new(
        keyer1: Keyer1,
        keyer2: Keyer2,
        prev_block_id1: BlockId,
        prev_block_id2: BlockId,
        state_lock: Option<Arc<IterationStateLock>>,
    ) -> Self {
        Self {
            left: SideReceiver::new(keyer1, prev_block_id1),
            right: SideReceiver::new(keyer2, prev_block_id2),
            timed_out: false,
            state_lock,
            state_generation: 0,
            wait_for_state: false,
            metadata: None,
            buffer: Default::default(),
        }
    }

    /// Process a new message from a given side.
    ///
    /// This will extract all the messages in the batch, filter the control items (flushes and
    /// terminates) and later wrap the items in the correct `JoinElement` variant.
    ///
    /// This will add the received messages in the buffer.
    fn process_side_messages<Out: ExchangeData, Keyer: KeyerFn<Key, Out>>(
        side: &mut SideReceiver<Key, Out, Keyer>,
        messages: NetworkMessage<Out>,
        wrap: fn((Key, Out)) -> JoinElement<Key, Out1, Out2>,
        end: JoinElement<Key, Out1, Out2>,
        buffer: &mut VecDeque<StreamElement<JoinElement<Key, Out1, Out2>>>,
    ) {
        let messages = messages.batch().into_iter().filter_map(|item| match item {
            StreamElement::FlushAndRestart => {
                side.missing_flush_and_restart -= 1;
                None
            }
            StreamElement::Terminate => {
                side.missing_terminate -= 1;
                None
            }
            item => Some(item.map(|item| {
                let key = (side.keyer)(&item);
                (key, item)
            })),
        });
        *buffer = messages.map(|item| item.map(wrap)).collect();
        if side.is_ended() {
            buffer.push_back(StreamElement::Item(end));
        }
    }

    /// Try to receive data from a single side, eventually timing out.
    ///
    /// This will add the received messages in the buffer.
    fn recv_side<Out: ExchangeData, Keyer: KeyerFn<Key, Out>>(
        side: &mut SideReceiver<Key, Out, Keyer>,
        timed_out: &mut bool,
        batch_mode: &BatchMode,
        wrap: fn((Key, Out)) -> JoinElement<Key, Out1, Out2>,
        end: JoinElement<Key, Out1, Out2>,
        buffer: &mut VecDeque<StreamElement<JoinElement<Key, Out1, Out2>>>,
    ) {
        let max_delay = batch_mode.max_delay();
        let receiver = side.receiver.as_mut().unwrap();
        let messages = match (*timed_out, max_delay) {
            (false, Some(max_delay)) => match receiver.recv_timeout(max_delay) {
                Ok(buf) => buf,
                Err(_) => {
                    *timed_out = true;
                    NetworkMessage::new(vec![StreamElement::FlushBatch], Default::default())
                }
            },
            _ => {
                *timed_out = false;
                receiver.recv().unwrap()
            }
        };
        Self::process_side_messages(side, messages, wrap, end, buffer);
    }

    /// Given that the buffer is empty, receive from the network the next batch and try to put some
    /// data into the buffer.
    ///
    /// This will add also the control messages (flushes and terminates) when needed, and receive
    /// from a single side if the other has already ended.
    ///
    /// When both sides are done, this will emit an extra `FlushAndRestart` message.
    ///
    /// If this returns `Some(elem)`, that message should be returned immediately.
    fn select(&mut self) -> Option<StreamElement<JoinElement<Key, Out1, Out2>>> {
        assert!(self.buffer.is_empty());
        let metadata = self.metadata.as_ref().unwrap();

        let batch_mode = &self.metadata.as_ref().unwrap().batch_mode;
        if self.left.is_terminated() && self.right.is_terminated() {
            // the stream is ended for everyone, just terminate
            debug!("JoinStartBlock at {} terminated", metadata.coord);
            return Some(StreamElement::Terminate);
        } else if self.left.is_ended() && self.right.is_ended() {
            // both the sides received flush and restart, reset
            debug!(
                "JoinStartBlock at {} received all flush and restart",
                metadata.coord
            );
            self.left.reset();
            self.right.reset();
            self.wait_for_state = true;
            self.state_generation += 2;
            return Some(StreamElement::FlushAndRestart);
        } else if self.left.is_ended() {
            // the left side has already ended, recv only from the right
            Self::recv_side(
                &mut self.right,
                &mut self.timed_out,
                batch_mode,
                JoinElement::Right,
                JoinElement::RightEnd,
                &mut self.buffer,
            );
        } else if self.right.is_ended() {
            // the right side has already ended, recv only from the left
            Self::recv_side(
                &mut self.left,
                &mut self.timed_out,
                batch_mode,
                JoinElement::Left,
                JoinElement::LeftEnd,
                &mut self.buffer,
            );
        } else {
            // both sides are still valid, select from both
            let left = self.left.receiver.as_ref().unwrap();
            let right = self.right.receiver.as_ref().unwrap();
            let result = match (self.timed_out, batch_mode.max_delay()) {
                (false, Some(max_delay)) => match left.select_timeout(right, max_delay) {
                    Ok(res) => res,
                    Err(_) => {
                        self.timed_out = true;
                        SelectResult::A(Ok(NetworkMessage::new(
                            vec![StreamElement::FlushBatch],
                            Default::default(),
                        )))
                    }
                },
                _ => {
                    self.timed_out = false;
                    left.select(right)
                }
            };
            match result {
                SelectResult::A(Ok(left)) => {
                    Self::process_side_messages(
                        &mut self.left,
                        left,
                        JoinElement::Left,
                        JoinElement::LeftEnd,
                        &mut self.buffer,
                    );
                }
                SelectResult::B(Ok(right)) => {
                    Self::process_side_messages(
                        &mut self.right,
                        right,
                        JoinElement::Right,
                        JoinElement::RightEnd,
                        &mut self.buffer,
                    );
                }
                _ => panic!("One of the receivers failed"),
            }
        }
        None
    }
}

impl<Key: Data, Out1: ExchangeData, Out2: ExchangeData, Keyer1, Keyer2>
    Operator<JoinElement<Key, Out1, Out2>> for JoinStartBlock<Key, Out1, Out2, Keyer1, Keyer2>
where
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        let mut network = metadata.network.lock().unwrap();
        self.left.setup(&metadata, &mut network);
        self.right.setup(&metadata, &mut network);
        drop(network);
        self.metadata = Some(metadata);
    }

    fn next(&mut self) -> StreamElement<JoinElement<Key, Out1, Out2>> {
        if let Some(item) = self.buffer.pop_front() {
            return item;
        }
        if self.wait_for_state {
            if let Some(lock) = self.state_lock.as_ref() {
                lock.wait_for_update(self.state_generation);
            }
            self.wait_for_state = false;
        }
        if let Some(elem) = self.select() {
            return elem;
        }
        self.next()
    }

    fn to_string(&self) -> String {
        format!(
            "JoinStartBlock<{}, {}, {}>",
            std::any::type_name::<Key>(),
            std::any::type_name::<Out1>(),
            std::any::type_name::<Out2>()
        )
    }

    fn structure(&self) -> BlockStructure {
        let mut operator =
            OperatorStructure::new::<JoinElement<Key, Out1, Out2>, _>("JoinStartBlock");
        if self.state_lock.is_some() {
            operator.subtitle = "has lock".to_string();
        }
        operator
            .receivers
            .push(OperatorReceiver::new::<Out1>(self.left.prev_block_id));
        operator
            .receivers
            .push(OperatorReceiver::new::<Out2>(self.right.prev_block_id));
        BlockStructure::default().add_operator(operator)
    }
}
