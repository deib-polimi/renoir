use std::any::TypeId;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

use lazy_init::Lazy;

use crate::block::{BlockStructure, Connection, NextStrategy, OperatorReceiver, OperatorStructure};
use crate::environment::StreamEnvironmentInner;
use crate::network::{Coord, NetworkMessage, NetworkSender, ReceiverEndpoint};
use crate::operator::source::Source;
use crate::operator::{Data, EndBlock, IterationStateHandle, Operator, StartBlock, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{BlockId, Stream};

type NewIterationState<State> = (bool, State);

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

#[derive(Derivative)]
#[derivative(Clone, Debug)]
struct ReplayLeader<DeltaUpdate: Data, State: Data> {
    /// The coordinates of this block.
    coord: Coord,

    /// The index of the current iteration (0-based).
    iteration_index: usize,
    /// The maximum number of iterations to perform.
    num_iterations: usize,

    /// The current global state of the iteration.
    ///
    /// It's an `Options` because the block needs to take ownership of it for processing it. It will
    /// be `None` only during that time frame.
    state: Option<State>,
    /// The initial value of the global state of the iteration.
    ///
    /// Will be used for resetting the global state after all the iterations complete.
    initial_state: State,

    /// The receiver from the `ReplayEndBlock`s at the end of the loop.
    ///
    /// This will be set inside `setup` when we will know the id of that block.
    delta_update_receiver: Option<StartBlock<DeltaUpdate>>,
    /// The number of replicas of `ReplayEndBlock`.
    num_receivers: usize,
    /// The id of the block where `ReplayEndBlock` is.
    ///
    /// This is a shared reference because when this block is constructed the tail of the iteration
    /// (i.e. the `ReplayEndBlock`) is not constructed yet. Therefore we cannot predict how many
    /// blocks there will be in between, and therefore we cannot know the block id.
    ///
    /// After constructing the entire iteration this shared variable will be set. This will happen
    /// before the call to `setup`.
    feedback_block_id: Arc<AtomicUsize>,
    /// The senders to the `Replay` block for the information about the new iteration.
    new_state_senders: Vec<NetworkSender<NetworkMessage<NewIterationState<State>>>>,
    /// Whether `next` should emit a `FlushAndRestart` in the next call.
    emit_flush_and_restart: bool,

    /// The function that combines the global state with a delta update.
    #[derivative(Debug = "ignore")]
    global_fold: Arc<dyn Fn(State, DeltaUpdate) -> State + Send + Sync>,
    /// A function that, given the global state, checks whether the iteration should continue.
    #[derivative(Debug = "ignore")]
    loop_condition: Arc<dyn Fn(&mut State) -> bool + Send + Sync>,
}

/// Similar to `EndBlock`, but tied specifically for the `Replay` iterations.
///
/// This block will receive the data (i.e. the `DeltaUpdate` already reduced) and send back to the
/// leader.
///
/// `EndBlock` cannot be used here since special care should be taken when the input stream is
/// empty.
#[derive(Debug, Clone)]
struct ReplayEndBlock<DeltaUpdate: Data, OperatorChain>
where
    OperatorChain: Operator<DeltaUpdate>,
{
    /// The chain of previous operators.
    ///
    /// At the end of this chain there should be the local reduction.
    prev: OperatorChain,
    /// Whether, since the last `IterEnd`, an element has been received.
    ///
    /// If two `IterEnd` are received in a row it means that the local reduction didn't happen since
    /// no item was present in the stream. A delta update should be sent to the leader nevertheless.
    has_received_item: bool,
    /// The block id of the block containing the `ReplayLeader` operator.
    leader_block_id: BlockId,
    /// The sender that points to the `Replay` leader for sending the `DeltaUpdate` messages.
    leader_sender: Option<NetworkSender<NetworkMessage<DeltaUpdate>>>,
    /// The coordinates of this block.
    coord: Coord,
}

impl<DeltaUpdate: Data, OperatorChain> ReplayEndBlock<DeltaUpdate, OperatorChain>
where
    OperatorChain: Operator<DeltaUpdate>,
{
    fn new(prev: OperatorChain, leader_block_id: BlockId) -> Self {
        Self {
            prev,
            has_received_item: false,
            leader_block_id,
            leader_sender: None,
            coord: Default::default(),
        }
    }
}

impl<DeltaUpdate: Data, State: Data> ReplayLeader<DeltaUpdate, State> {
    fn new(
        initial_state: State,
        num_iterations: usize,
        global_fold: impl Fn(State, DeltaUpdate) -> State + Send + Sync + 'static,
        loop_condition: impl Fn(&mut State) -> bool + Send + Sync + 'static,
        feedback_block_id: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            // these fields will be set inside the `setup` method
            delta_update_receiver: None,
            new_state_senders: Default::default(),
            coord: Coord::new(0, 0, 0),
            num_receivers: 0,

            num_iterations,
            iteration_index: 0,
            state: Some(initial_state.clone()),
            initial_state,
            feedback_block_id,
            emit_flush_and_restart: false,
            global_fold: Arc::new(global_fold),
            loop_condition: Arc::new(loop_condition),
        }
    }
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

        // the id of the block where ReplayEndBlock is. At this moment we cannot know it, so we
        // store a fake value inside this and as soon as we know it we set it to the right value.
        let feedback_block_id = Arc::new(AtomicUsize::new(0));

        let output_stream = StreamEnvironmentInner::stream(
            env,
            ReplayLeader::new(
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

        let iter_end = iter_end.add_operator(|prev| ReplayEndBlock::new(prev, leader_block_id));
        let replay_end_block_id = iter_end.block.id;

        let mut env = iter_end.env.borrow_mut();
        let scheduler = env.scheduler_mut();
        scheduler.add_block(iter_end.block);
        // connect the ReplayEndBlock to the ReplayLeader
        scheduler.connect_blocks(
            replay_end_block_id,
            leader_block_id,
            TypeId::of::<DeltaUpdate>(),
        );
        scheduler.connect_blocks(
            leader_block_id,
            replay_block_id,
            TypeId::of::<NewIterationState<State>>(),
        );
        drop(env);

        // store the id of the block containing the ReplayEndBlock
        feedback_block_id.store(replay_end_block_id, Ordering::Release);

        // TODO: check parallelism and make sure the blocks are spawned on the same replicas

        // FIXME: this add_block is here just to make sure that the NextStrategy of output_stream
        //        is not changed by the following operators. This because the next strategy affects
        //        the connections made by the scheduler and if accidentally set to OnlyOne will
        //        break the connections.
        output_stream.add_block(EndBlock::new, NextStrategy::Random)
    }
}

impl<DeltaUpdate: Data, OperatorChain> Operator<()> for ReplayEndBlock<DeltaUpdate, OperatorChain>
where
    DeltaUpdate: Default,
    OperatorChain: Operator<DeltaUpdate>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        let mut network = metadata.network.lock().unwrap();

        let replicas = network.replicas(self.leader_block_id);
        assert_eq!(
            replicas.len(),
            1,
            "The ReplayLeader block should not be replicated"
        );
        let leader = replicas.into_iter().next().unwrap();
        debug!("ReplayEndBlock {} has {} as leader", metadata.coord, leader);

        let sender = network.get_sender(ReceiverEndpoint::new(leader, metadata.coord.block_id));
        self.leader_sender = Some(sender);

        drop(network);

        self.coord = metadata.coord;
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<()> {
        let elem = self.prev.next();
        match &elem {
            StreamElement::Item(_) => {
                let message = NetworkMessage::new(vec![elem], self.coord);
                self.leader_sender.as_ref().unwrap().send(message).unwrap();
                self.has_received_item = true;
                StreamElement::Item(())
            }
            StreamElement::FlushAndRestart => {
                // If two FlushAndRestart have been received in a row it means that no message went
                // through the iteration inside this replica. Nevertheless the DeltaUpdate must be
                // sent to the leader.
                if !self.has_received_item {
                    let update = Default::default();
                    let message =
                        NetworkMessage::new(vec![StreamElement::Item(update)], self.coord);
                    let sender = self.leader_sender.as_ref().unwrap();
                    sender.send(message).unwrap();
                }
                self.has_received_item = false;
                StreamElement::FlushAndRestart
            }
            StreamElement::Terminate => {
                let message = NetworkMessage::new(vec![StreamElement::Terminate], self.coord);
                self.leader_sender.as_ref().unwrap().send(message).unwrap();
                StreamElement::Terminate
            }
            StreamElement::FlushBatch => elem.map(|_| unreachable!()),
            _ => unreachable!(),
        }
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> ReplayEndBlock<{}>",
            self.prev.to_string(),
            std::any::type_name::<DeltaUpdate>()
        )
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<DeltaUpdate, _>("ReplayEndBlock");
        operator.connections.push(Connection::new::<DeltaUpdate>(
            self.leader_block_id,
            &NextStrategy::OnlyOne,
        ));
        self.prev.structure().add_operator(operator)
    }
}

impl<DeltaUpdate: Data, State: Data> Operator<State> for ReplayLeader<DeltaUpdate, State> {
    fn setup(&mut self, metadata: ExecutionMetadata) {
        let mut network = metadata.network.lock().unwrap();
        self.coord = metadata.coord;
        self.new_state_senders = network
            .get_senders(metadata.coord)
            .into_iter()
            .map(|(_, s)| s)
            .collect();
        drop(network);

        // at this point the id of the block with ReplayEndBlock must be known
        let feedback_block_id = self.feedback_block_id.load(Ordering::Acquire);
        // get the receiver for the delta updates
        let mut delta_update_receiver = StartBlock::new(feedback_block_id);
        delta_update_receiver.setup(metadata);
        self.num_receivers = delta_update_receiver.num_prev();
        self.delta_update_receiver = Some(delta_update_receiver);
    }

    fn next(&mut self) -> StreamElement<State> {
        if self.emit_flush_and_restart {
            self.emit_flush_and_restart = false;
            return StreamElement::FlushAndRestart;
        }
        loop {
            debug!(
                "Leader {} is waiting for {} delta updates",
                self.coord, self.num_receivers
            );
            let mut missing_delta_updates = self.num_receivers;
            while missing_delta_updates > 0 {
                let update = self.delta_update_receiver.as_mut().unwrap().next();
                match update {
                    StreamElement::Item(delta_update) => {
                        missing_delta_updates -= 1;
                        debug!(
                            "Replay leader at {} received a delta update, {} missing",
                            self.coord, missing_delta_updates
                        );
                        self.state =
                            Some((self.global_fold)(self.state.take().unwrap(), delta_update));
                    }
                    StreamElement::Terminate => {
                        debug!("ReplayLeader {} received Terminate", self.coord);
                        return StreamElement::Terminate;
                    }
                    StreamElement::FlushAndRestart => {}
                    StreamElement::FlushBatch => {}
                    _ => unreachable!(
                        "ReplayLeader received an invalid message: {}",
                        update.variant()
                    ),
                }
            }
            // the loop condition may change the state
            let mut should_continue = (self.loop_condition)(self.state.as_mut().unwrap());
            debug!(
                "Replay leader at {} checked loop condition and resulted in {}",
                self.coord, should_continue
            );
            self.iteration_index += 1;
            if self.iteration_index >= self.num_iterations {
                debug!("Replay leader at {} reached iteration limit", self.coord);
                should_continue = false;
            }

            let to_return = if !should_continue {
                // reset the global state at the end of the iteration
                let to_return = self.state.take();
                self.state = Some(self.initial_state.clone());
                to_return
            } else {
                None
            };

            let new_state_message = (should_continue, self.state.clone().unwrap());
            for sender in &self.new_state_senders {
                let message = NetworkMessage::new(
                    vec![StreamElement::Item(new_state_message.clone())],
                    self.coord,
                );
                sender.send(message).unwrap();
            }

            if let Some(state) = to_return {
                assert!(!should_continue);
                self.emit_flush_and_restart = true;
                self.iteration_index = 0;
                return StreamElement::Item(state);
            }
        }
    }

    fn to_string(&self) -> String {
        format!("ReplayLeader<{}>", std::any::type_name::<State>())
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<State, _>("ReplayLeader");
        operator
            .connections
            .push(Connection::new::<NewIterationState<State>>(
                self.new_state_senders[0].receiver_endpoint.coord.block_id,
                &NextStrategy::OnlyOne,
            ));
        self.delta_update_receiver
            .as_ref()
            .unwrap()
            .structure()
            .add_operator(operator)
    }
}

impl<DeltaUpdate: Data, State: Data> Source<State> for ReplayLeader<DeltaUpdate, State> {
    fn get_max_parallelism(&self) -> Option<usize> {
        Some(1)
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
                    "Replay received invalid message from ReplayLeader: {}",
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
