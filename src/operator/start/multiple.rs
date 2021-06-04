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
struct SideReceiver<Out: ExchangeData> {
    /// The internal receiver for this side.
    receiver: SingleStartBlockReceiver<Out>,
    /// The number of replicas this side has.
    num_replicas: usize,
    /// How many replicas from this side has not yet sent `StreamElement::FlushAndRestart`.
    missing_flush_and_restart: usize,
}

impl<Out: ExchangeData> SideReceiver<Out> {
    fn new(previous_block_id: BlockId) -> Self {
        Self {
            receiver: SingleStartBlockReceiver::new(previous_block_id),
            num_replicas: 0,
            missing_flush_and_restart: 0,
        }
    }

    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.receiver.setup(metadata);
        self.num_replicas = self.receiver.prev_replicas().len();
        self.missing_flush_and_restart = self.num_replicas;
    }

    fn reset(&mut self) {
        self.missing_flush_and_restart = self.num_replicas;
    }

    fn is_ended(&self) -> bool {
        self.missing_flush_and_restart == 0
    }
}

/// This receiver is able to receive data from two previous blocks.
///
/// To do so it will first select on the two channels, and wrap each element into an enumeration
/// that discriminates the two sides.
#[derive(Clone, Debug)]
pub(crate) struct MultipleStartBlockReceiver<OutL: ExchangeData, OutR: ExchangeData> {
    left: SideReceiver<OutL>,
    right: SideReceiver<OutR>,
}

impl<OutL: ExchangeData, OutR: ExchangeData> MultipleStartBlockReceiver<OutL, OutR> {
    pub(super) fn new(left_block_id: BlockId, right_block_id: BlockId) -> Self {
        Self {
            left: SideReceiver::new(left_block_id),
            right: SideReceiver::new(right_block_id),
        }
    }

    /// Process the incoming batch from one of the two sides.
    ///
    /// This will map all the elements of the batch into a new batch whose elements are wrapped in
    /// the variant of the correct side. Additionally this will search for
    /// `StreamElement::FlushAndRestart` messages and eventually emit the `LeftEnd`/`RightEnd`
    /// accordingly.
    fn process_side<Out: ExchangeData>(
        side: &mut SideReceiver<Out>,
        message: NetworkMessage<Out>,
        wrap: fn(Out) -> TwoSidesItem<OutL, OutR>,
        end: TwoSidesItem<OutL, OutR>,
    ) -> NetworkMessage<TwoSidesItem<OutL, OutR>> {
        let sender = message.sender();
        let data = message
            .batch()
            .into_iter()
            .flat_map(move |item| {
                let mut res = Vec::new();
                if matches!(item, StreamElement::FlushAndRestart) {
                    side.missing_flush_and_restart -= 1;
                    // make sure to add this message before `FlushAndRestart`
                    if side.is_ended() {
                        res.push(StreamElement::Item(end.clone()));
                    }
                }
                res.push(item.map(wrap));
                res
            })
            .collect::<Batch<_>>();
        NetworkMessage::new(data, sender)
    }

    /// Receive from the previous sides the next batch, or fail with a timeout if provided.
    ///
    /// This will access only the needed side (i.e. if one of the sides ended, only the other is
    /// probed).
    fn select(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<NetworkMessage<TwoSidesItem<OutL, OutR>>, RecvTimeoutError> {
        // both left and right received all the FlushAndRestart, prepare for the next iteration
        if self.left.is_ended() && self.right.is_ended() {
            self.left.reset();
            self.right.reset();
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
        0 // TODO
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
