use std::any::TypeId;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

use lazy_init::Lazy;

use crate::block::{BlockStructure, NextStrategy, OperatorReceiver, OperatorStructure};
use crate::environment::StreamEnvironmentInner;
use crate::network::Coord;
use crate::operator::iteration::iteration_end::IterationEndBlock;
use crate::operator::iteration::leader::IterationLeader;
use crate::operator::{
    Data, EndBlock, IterationStateHandle, NewIterationState, Operator, StartBlock, StreamElement,
};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{BlockId, Stream};

/// This is the first operator of the chain of blocks inside an iteration.
///
/// If a new iteration should start, the initial dataset is replayed.
#[derive(Derivative)]
#[derivative(Debug, Clone)]
pub struct Replay<Out: Data, State: Data, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    /// The coordinate of this replica.
    coord: Coord,

    /// The chain of previous operators where the dataset to replay is read from.
    prev: OperatorChain,

    /// Receiver of the new state from the leader.
    new_state_receiver: StartBlock<NewIterationState<State>>,
    /// The content of the stream to replay.
    content: Vec<StreamElement<Out>>,
    /// The index inside `content` of the first message to be sent.
    content_index: usize,

    /// Whether the input stream has ended or not.
    has_input_ended: bool,

    /// Whether this `Replay` is the _local_ leader.
    ///
    /// The local leader is the one that sets the iteration state for all the local replicas.
    is_local_leader: bool,
    /// The number of replicas of this block on this host.
    num_local_replicas: usize,

    /// A reference to the state of the iteration that is visible to the loop operators.
    state_ref: IterationStateHandle<State>,

    /// A barrier for synchronizing all the local replicas before updating the state.
    ///
    /// This is a `Lazy` because at construction time we don't know the barrier size, we need to
    /// wait until at least until `setup` when we know how many replicas are present in the current
    /// host.
    state_barrier: Arc<Lazy<Barrier>>,
}

impl<Out: Data, State: Data, OperatorChain> Replay<Out, State, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    fn new(
        prev: OperatorChain,
        state_ref: IterationStateHandle<State>,
        leader_block_id: BlockId,
    ) -> Self {
        Self {
            // these fields will be set inside the `setup` method
            coord: Coord::new(0, 0, 0),
            is_local_leader: false,
            num_local_replicas: 0,

            prev,
            new_state_receiver: StartBlock::new(leader_block_id),
            content: Default::default(),
            content_index: 0,
            has_input_ended: false,
            state_ref,
            state_barrier: Arc::new(Lazy::new()),
        }
    }
}

/// Given a list of replicas, deterministically select a leader between them.
fn select_leader(replicas: &[Coord]) -> Coord {
    *replicas.iter().min().unwrap()
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn replay<Body, DeltaUpdate: Data + Default, State: Data, OperatorChain2>(
        self,
        num_iterations: usize,
        initial_state: State,
        body: Body,
        local_fold: impl Fn(DeltaUpdate, Out) -> DeltaUpdate + Send + Sync + 'static,
        global_fold: impl Fn(State, DeltaUpdate) -> State + Send + Sync + 'static,
        loop_condition: impl Fn(&mut State) -> bool + Send + Sync + 'static,
    ) -> Stream<State, impl Operator<State>>
    where
        Body: Fn(
            Stream<Out, Replay<Out, State, OperatorChain>>,
            IterationStateHandle<State>,
        ) -> Stream<Out, OperatorChain2>,
        OperatorChain2: Operator<Out> + Send + 'static,
    {
        let state = IterationStateHandle::new(initial_state.clone());
        let state_clone = state.clone();
        let env = self.env.clone();

        // the id of the block where IterationEndBlock is. At this moment we cannot know it, so we
        // store a fake value inside this and as soon as we know it we set it to the right value.
        let feedback_block_id = Arc::new(AtomicUsize::new(0));

        let output_stream = StreamEnvironmentInner::stream(
            env,
            IterationLeader::new(
                initial_state,
                num_iterations,
                global_fold,
                loop_condition,
                feedback_block_id.clone(),
            ),
        );
        let leader_block_id = output_stream.block.id;

        let iter_start = self.add_operator(|prev| Replay::new(prev, state, leader_block_id));
        let replay_block_id = iter_start.block.id;

        let iter_end = body(iter_start, state_clone)
            .key_by(|_| ())
            .fold(DeltaUpdate::default(), local_fold)
            .unkey()
            .map(|(_, delta_update)| delta_update);

        let iter_end = iter_end.add_operator(|prev| IterationEndBlock::new(prev, leader_block_id));
        let iteration_end_block_id = iter_end.block.id;

        let mut env = iter_end.env.borrow_mut();
        let scheduler = env.scheduler_mut();
        scheduler.add_block(iter_end.block);
        // connect the IterationEndBlock to the IterationLeader
        scheduler.connect_blocks(
            iteration_end_block_id,
            leader_block_id,
            TypeId::of::<DeltaUpdate>(),
        );
        scheduler.connect_blocks(
            leader_block_id,
            replay_block_id,
            TypeId::of::<NewIterationState<State>>(),
        );
        drop(env);

        // store the id of the block containing the IterationEndBlock
        feedback_block_id.store(iteration_end_block_id, Ordering::Release);

        // TODO: check parallelism and make sure the blocks are spawned on the same replicas

        // FIXME: this add_block is here just to make sure that the NextStrategy of output_stream
        //        is not changed by the following operators. This because the next strategy affects
        //        the connections made by the scheduler and if accidentally set to OnlyOne will
        //        break the connections.
        output_stream.add_block(EndBlock::new, NextStrategy::Random)
    }
}

impl<Out: Data, State: Data, OperatorChain> Operator<Out> for Replay<Out, State, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata.clone());
        self.new_state_receiver.setup(metadata.clone());

        let local_replicas: Vec<_> = metadata
            .replicas
            .clone()
            .into_iter()
            .filter(|r| r.host_id == metadata.coord.host_id)
            .collect();
        self.is_local_leader = select_leader(&local_replicas) == metadata.coord;
        self.num_local_replicas = local_replicas.len();
        self.coord = metadata.coord;
    }

    fn next(&mut self) -> StreamElement<Out> {
        if !self.has_input_ended {
            let item = self.prev.next();

            return match &item {
                StreamElement::FlushAndRestart => {
                    debug!(
                        "Replay at {} received all the input: {} elements total",
                        self.coord,
                        self.content.len()
                    );
                    self.has_input_ended = true;
                    self.content.push(StreamElement::FlushAndRestart);
                    // the first iteration has already happened
                    self.content_index = self.content.len();
                    StreamElement::FlushAndRestart
                }
                // messages to save for the replay
                StreamElement::Item(_)
                | StreamElement::Timestamped(_, _)
                | StreamElement::Watermark(_) => {
                    self.content.push(item.clone());
                    item
                }
                // messages to forward without replaying
                StreamElement::FlushBatch => item,
                StreamElement::Terminate => {
                    debug!("Replay at {} is terminating", self.coord);
                    item
                }
            };
        }

        // from here the input has for sure ended, so we need to replay it...

        // this iteration has not ended yet
        if self.content_index < self.content.len() {
            let item = self.content.get(self.content_index).unwrap().clone();
            self.content_index += 1;
            return item;
        }

        debug!("Replay at {} has ended the iteration", self.coord);

        // this iteration has ended, wait here for the leader
        let (should_continue, new_state) = loop {
            let message = self.new_state_receiver.next();
            match message {
                StreamElement::Item((should_continue, new_state)) => {
                    break (should_continue, new_state);
                }
                StreamElement::FlushBatch => {}
                StreamElement::FlushAndRestart => {}
                _ => unreachable!(
                    "Replay received invalid message from IterationLeader: {}",
                    message.variant()
                ),
            }
        };

        // update the state only once per host
        if self.is_local_leader {
            // SAFETY: at this point we are sure that all the operators inside the loop have
            // finished and empty. This means that no calls to `.get` are possible until one Replay
            // block chooses to start. This cannot happen due to the barrier below.
            unsafe {
                self.state_ref.set(new_state);
            }
        }
        // make sure that the state is set before any replica on this host is able to start again,
        // reading the old state
        self.state_barrier
            .get_or_create(|| Barrier::new(self.num_local_replicas))
            .wait();

        self.content_index = 0;

        // the loop has ended
        if !should_continue {
            debug!("Replay block at {} ended the iteration", self.coord);
            // cleanup so that if this is a nested iteration next time we'll be good to start again
            self.content.clear();
            self.has_input_ended = false;
            return self.next();
        }

        // This iteration has ended but IterEnd has already been sent. To avoid sending twice the
        // IterEnd recurse.
        self.next()
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> Replay<{}>",
            self.prev.to_string(),
            std::any::type_name::<Out>()
        )
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("Replay");
        operator
            .receivers
            .push(OperatorReceiver::new::<NewIterationState<State>>(
                self.new_state_receiver.prev()[0],
            ));
        self.prev.structure().add_operator(operator)
    }
}
