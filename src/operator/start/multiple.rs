use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::block::{BlockStructure, OperatorReceiver, OperatorStructure};
use crate::channel::{RecvTimeoutError, SelectResult};
use crate::network::{Batch, Coord, NetworkMessage};
use crate::operator::start::{SingleStartBlockReceiver, StartBlockReceiver};
use crate::operator::{Data, ExchangeData, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::BlockId;

/// This enum is an _either_ type, it contain either an element from the left part or an element
/// from the right part.
///
/// Since those two parts are merged the information about which one ends is lost, therefore there
/// are two extra variants to keep track of that.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum TwoSidesItem<OutL: Data, OutR: Data> {
    /// An element of the stream on the left.
    Left(OutL),
    /// An element of the stream on the right.
    Right(OutR),
    /// The left side has ended.
    LeftEnd,
    /// The right side has ended.
    RightEnd,
}

/// The actual receiver from one of the two sides.
#[derive(Clone, Debug)]
struct SideReceiver<Out: ExchangeData, Item: ExchangeData> {
    /// The internal receiver for this side.
    receiver: SingleStartBlockReceiver<Out>,
    /// The number of replicas this side has.
    num_replicas: usize,
    /// How many replicas from this side has not yet sent `StreamElement::FlushAndRestart`.
    missing_flush_and_restart: usize,
    /// Whether this side is cached (i.e. after it is fully read, it's just replayed indefinitely).
    cached: bool,
    /// The content of the cache, if any.
    cache: Vec<NetworkMessage<Item>>,
    /// Whether the cache has been fully populated.
    cache_done: bool,
    /// The index of the first element to return from the cache.
    cache_pointer: usize,
}

impl<Out: ExchangeData, Item: ExchangeData> SideReceiver<Out, Item> {
    fn new(previous_block_id: BlockId, cached: bool) -> Self {
        Self {
            receiver: SingleStartBlockReceiver::new(previous_block_id),
            num_replicas: 0,
            missing_flush_and_restart: 0,
            cached,
            cache: Default::default(),
            cache_done: false,
            cache_pointer: 0,
        }
    }

    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.receiver.setup(metadata);
        self.num_replicas = self.receiver.prev_replicas().len();
        self.missing_flush_and_restart = self.num_replicas;
    }

    fn reset(&mut self) {
        self.missing_flush_and_restart = self.num_replicas;
        self.cache_done = true;
        self.cache_pointer = 0;
    }

    fn is_ended(&self) -> bool {
        self.missing_flush_and_restart == 0
    }

    fn next_cached_item(&mut self) -> Option<NetworkMessage<Item>> {
        if self.cache_pointer >= self.cache.len() {
            None
        } else {
            self.cache_pointer += 1;
            Some(self.cache[self.cache_pointer - 1].clone())
        }
    }
}

/// This receiver is able to receive data from two previous blocks.
///
/// To do so it will first select on the two channels, and wrap each element into an enumeration
/// that discriminates the two sides.
#[derive(Clone, Debug)]
pub(crate) struct MultipleStartBlockReceiver<OutL: ExchangeData, OutR: ExchangeData> {
    left: SideReceiver<OutL, TwoSidesItem<OutL, OutR>>,
    right: SideReceiver<OutR, TwoSidesItem<OutL, OutR>>,
}

impl<OutL: ExchangeData, OutR: ExchangeData> MultipleStartBlockReceiver<OutL, OutR> {
    pub(super) fn new(
        left_block_id: BlockId,
        right_block_id: BlockId,
        left_cache: bool,
        right_cache: bool,
    ) -> Self {
        assert!(
            !(left_cache && right_cache),
            "At most one of the two sides can be cached"
        );
        Self {
            left: SideReceiver::new(left_block_id, left_cache),
            right: SideReceiver::new(right_block_id, right_cache),
        }
    }

    /// Process the incoming batch from one of the two sides.
    ///
    /// This will map all the elements of the batch into a new batch whose elements are wrapped in
    /// the variant of the correct side. Additionally this will search for
    /// `StreamElement::FlushAndRestart` messages and eventually emit the `LeftEnd`/`RightEnd`
    /// accordingly.
    fn process_side<Out: ExchangeData>(
        side: &mut SideReceiver<Out, TwoSidesItem<OutL, OutR>>,
        message: NetworkMessage<Out>,
        wrap: fn(Out) -> TwoSidesItem<OutL, OutR>,
        end: TwoSidesItem<OutL, OutR>,
    ) -> NetworkMessage<TwoSidesItem<OutL, OutR>> {
        let sender = message.sender();
        let data = message
            .batch()
            .into_iter()
            .flat_map(|item| {
                let mut res = Vec::new();
                if matches!(item, StreamElement::FlushAndRestart) {
                    side.missing_flush_and_restart -= 1;
                    // make sure to add this message before `FlushAndRestart`
                    if side.is_ended() {
                        res.push(StreamElement::Item(end.clone()));
                    }
                }
                // StreamElement::Terminate should not be put in the cache
                if !side.cached || !matches!(item, StreamElement::Terminate) {
                    res.push(item.map(wrap));
                }
                res
            })
            .collect::<Batch<_>>();
        let message = NetworkMessage::new(data, sender);
        if side.cached {
            side.cache.push(message.clone());
        }
        message
    }

    /// Receive from the previous sides the next batch, or fail with a timeout if provided.
    ///
    /// This will access only the needed side (i.e. if one of the sides ended, only the other is
    /// probed). This will try to use the cache if it's available.
    fn select(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<NetworkMessage<TwoSidesItem<OutL, OutR>>, RecvTimeoutError> {
        // both left and right received all the FlushAndRestart, prepare for the next iteration
        if self.left.is_ended() && self.right.is_ended() {
            self.left.reset();
            self.right.reset();
        }

        // some items are ready from the cache, use them
        // TODO: should this be fair with the other non-cached side?
        if self.left.cached && self.left.cache_done {
            if let Some(batch) = self.left.next_cached_item() {
                return Ok(batch);
            }
        }
        if self.right.cached && self.right.cache_done {
            if let Some(batch) = self.right.next_cached_item() {
                return Ok(batch);
            }
        }

        enum Side<L, R> {
            Left(L),
            Right(R),
        }

        let data = if self.left.is_ended() {
            Side::Right(if let Some(timeout) = timeout {
                self.right.receiver.recv_timeout(timeout)
            } else {
                Ok(self.right.receiver.recv())
            })
        } else if self.right.is_ended() {
            Side::Left(if let Some(timeout) = timeout {
                self.left.receiver.recv_timeout(timeout)
            } else {
                Ok(self.left.receiver.recv())
            })
        } else {
            let left = self.left.receiver.receiver.as_mut().unwrap();
            let right = self.right.receiver.receiver.as_mut().unwrap();
            let data = if let Some(timeout) = timeout {
                left.select_timeout(right, timeout)
            } else {
                Ok(left.select(right))
            };
            match data {
                Ok(SelectResult::A(left)) => {
                    Side::Left(left.map_err(|_| RecvTimeoutError::Timeout))
                }
                Ok(SelectResult::B(right)) => {
                    Side::Right(right.map_err(|_| RecvTimeoutError::Timeout))
                }
                // timeout
                Err(e) => Side::Left(Err(e)),
            }
        };

        match data {
            Side::Left(Ok(left)) => Ok(Self::process_side(
                &mut self.left,
                left,
                TwoSidesItem::Left,
                TwoSidesItem::LeftEnd,
            )),
            Side::Right(Ok(right)) => Ok(Self::process_side(
                &mut self.right,
                right,
                TwoSidesItem::Right,
                TwoSidesItem::RightEnd,
            )),
            Side::Left(Err(_)) | Side::Right(Err(_)) => Err(RecvTimeoutError::Timeout),
        }
    }
}

impl<OutL: ExchangeData, OutR: ExchangeData> StartBlockReceiver<TwoSidesItem<OutL, OutR>>
    for MultipleStartBlockReceiver<OutL, OutR>
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.left.setup(metadata.clone());
        self.right.setup(metadata);
    }

    fn prev_replicas(&self) -> Vec<Coord> {
        let mut previous = self.left.receiver.prev_replicas();
        previous.append(&mut self.right.receiver.prev_replicas());
        previous
    }

    fn cached_replicas(&self) -> usize {
        let mut cached = 0;
        if self.left.cached {
            cached += self.left.num_replicas
        }
        if self.right.cached {
            cached += self.right.num_replicas
        }
        cached
    }

    fn recv_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<NetworkMessage<TwoSidesItem<OutL, OutR>>, RecvTimeoutError> {
        self.select(Some(timeout))
    }

    fn recv(&mut self) -> NetworkMessage<TwoSidesItem<OutL, OutR>> {
        self.select(None).expect("receiver failed")
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<TwoSidesItem<OutL, OutR>, _>("StartBlock");
        operator.receivers.push(OperatorReceiver::new::<OutL>(
            self.left.receiver.previous_block_id,
        ));
        operator.receivers.push(OperatorReceiver::new::<OutR>(
            self.right.receiver.previous_block_id,
        ));

        BlockStructure::default().add_operator(operator)
    }
}
