use std::sync::{Arc, Barrier};

use lazy_init::Lazy;

use crate::network::Coord;
use crate::operator::iteration::{IterationStateHandle, IterationStateLock, StateFeedback};
use crate::operator::start::{SingleStartBlockReceiverOperator, StartBlock};
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::scheduler::{BlockId, ExecutionMetadata};

use super::IterationResult;

/// Helper struct that handles the state of an iteration block.
///
/// This will keep track of the state locks and barriers for updating the state, as well as
/// receiving the state from the leader.
#[derive(Debug, Clone)]
pub(crate) struct IterationStateHandler<State: ExchangeData> {
    /// The coordinate of this replica.
    pub coord: Coord,

    /// Receiver of the new state from the leader.
    pub new_state_receiver: SingleStartBlockReceiverOperator<StateFeedback<State>>,
    /// The id of the block where `IterationLeader` is.
    pub leader_block_id: BlockId,

    /// Whether this `Replay` is the _local_ leader.
    ///
    /// The local leader is the one that sets the iteration state for all the local replicas.
    pub is_local_leader: bool,
    /// The number of replicas of this block on this host.
    pub num_local_replicas: usize,

    /// A reference to the state of the iteration that is visible to the loop operators.
    pub state_ref: IterationStateHandle<State>,

    /// A barrier for synchronizing all the local replicas before updating the state.
    ///
    /// This is a `Lazy` because at construction time we don't know the barrier size, we need to
    /// wait until at least until `setup` when we know how many replicas are present in the current
    /// host.
    pub state_barrier: Arc<Lazy<Barrier>>,
    /// The lock for the state of this iteration.
    pub state_lock: Arc<IterationStateLock>,
}

/// Given a list of replicas, deterministically select a leader between them.
fn select_leader(replicas: &[Coord]) -> Coord {
    *replicas.iter().min().unwrap()
}

impl<State: ExchangeData> IterationStateHandler<State> {
    pub(crate) fn new(
        leader_block_id: BlockId,
        state_ref: IterationStateHandle<State>,
        state_lock: Arc<IterationStateLock>,
    ) -> Self {
        Self {
            coord: Default::default(),
            is_local_leader: false,
            num_local_replicas: 0,

            new_state_receiver: StartBlock::single(leader_block_id, None),
            leader_block_id,
            state_ref,
            state_barrier: Arc::new(Default::default()),
            state_lock,
        }
    }

    pub(crate) fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        let local_replicas: Vec<_> = metadata
            .replicas
            .clone()
            .into_iter()
            .filter(|r| r.host_id == metadata.coord.host_id)
            .collect();
        self.is_local_leader = select_leader(&local_replicas) == metadata.coord;
        self.num_local_replicas = local_replicas.len();
        self.coord = metadata.coord;

        self.new_state_receiver.setup(metadata);
    }

    pub(crate) fn lock(&self) {
        self.state_lock.lock();
    }

    pub(crate) fn wait_leader(&mut self) -> IterationResult {
        let (should_continue, new_state) = loop {
            let message = self.new_state_receiver.next();
            match message {
                StreamElement::Item((should_continue, new_state)) => {
                    break (should_continue, new_state);
                }
                StreamElement::FlushBatch => {}
                StreamElement::FlushAndRestart => {}
                _ => unreachable!(
                    "Iterate received invalid message from IterationLeader: {}",
                    message.variant()
                ),
            }
        };

        // update the state only once per host
        if self.is_local_leader {
            // SAFETY: at this point we are sure that all the operators inside the loop have
            // finished and empty. This means that no calls to `.get` are possible until one Replay
            // block chooses to start. This cannot happen due to the barrier below and the state
            // lock.
            unsafe {
                self.state_ref.set(new_state);
            }
        }
        // make sure that the state is set before any replica on this host is able to start again,
        // reading the old state
        self.state_barrier
            .get_or_create(|| Barrier::new(self.num_local_replicas))
            .wait();

        if self.is_local_leader {
            // now the state has been set, accessing it is safe again
            self.state_lock.unlock();
        }

        should_continue
    }
}
