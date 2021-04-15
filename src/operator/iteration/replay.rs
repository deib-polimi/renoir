use std::any::TypeId;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::block::{BlockStructure, Connection, NextStrategy, OperatorReceiver, OperatorStructure};
use crate::channel::{BoundedChannelReceiver, BoundedChannelSender};
use crate::environment::StreamEnvironmentInner;
use crate::network::{Coord, NetworkMessage, NetworkReceiver, NetworkSender, ReceiverEndpoint};
use crate::operator::source::ChannelSource;
use crate::operator::{Data, EndBlock, IterationStateHandle, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{BlockId, Stream};

/// The capacity of the local channel that sends the final state to the output block.
const OUTPUT_CHANNEL_CAPACITY: usize = 2;

/// Message exchanged between `Replay` and `ReplayEndBlock` signaling the delta updates and the
/// information for the next iteration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(deserialize = ""))]
enum DeltaUpdateMessage<DeltaUpdate: Data, State: Data> {
    /// This variant is sent from a `ReplayEndBlock` to the `Replay` leader with the local state
    /// reduced.
    DeltaUpdate(DeltaUpdate),
    /// This variant is sent from the `Replay` leader to the `Replay` followers after an iteration
    /// completes.
    ///
    /// The first field is `true` if the next iteration should be executed, the second field is the
    /// new global state.
    NextIteration(bool, State),
}

/// This is the first operator of the chain of blocks inside an iteration.
///
/// This operator can work in two modes:
///
/// - the leader (i.e. the replica selected via `select_leader`) will perform the global reduction
///   synchronizing all the replicas of the iteration. After computing the new global state it will
///   send it to all the followers signaling the start of the next iteration.
/// - the followers after an iteration completes wait for the new state from the leader and later
///   start the new iteration.
///
/// In both cases this operator reads from its predecessor the dataset till the end is reached
/// (either `End` or `IterEnd` is received). When the dataset ends it sends `IterEnd` inside the
/// loop and, according to the role, checks whether the next iteration should start again.
///
/// If a new iteration should start, the initial dataset is replayed.
#[derive(Derivative)]
#[derivative(Debug, Clone)]
pub struct Replay<Out: Data, DeltaUpdate: Data, State: Data, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    /// The coordinate of this replica.
    coord: Coord,

    /// The chain of previous operators where the dataset to replay is read from.
    prev: OperatorChain,
    /// A receiver from which the `Replay` will receive the `DeltaUpdateMessage`.
    ///
    /// If this `Replay` is a leader this will receive from all the `ReplayEndBlock`. If this is a
    /// follower, this will receive from the leader.
    ///
    /// This will be set inside `setup`, when this block won't be cloned anymore. Cloning with
    /// `None` is therefore safe.
    #[derivative(Clone(clone_with = "clone_with_none"))]
    feedback: Option<NetworkReceiver<NetworkMessage<DeltaUpdateMessage<DeltaUpdate, State>>>>,
    /// The id of the block where `ReplayEndBlock` is.
    ///
    /// This is a shared reference because when this block is constructed the tail of the iteration
    /// (i.e. the `ReplayEndBlock`) is not constructed yet. Therefore we cannot predict how many
    /// blocks there will be in between, and therefore we cannot know the block id.
    ///
    /// After constructing the entire iteration this shared variable will be set. This will happen
    /// before the call to `setup`.
    feedback_block_id: Arc<AtomicUsize>,
    /// If this `Replay` is the leader, this will contain the senders to all the following `Replay`
    /// blocks.
    ///
    /// It is used to send the `DeltaUpdateMessage::NextIteration` messages.
    senders: Vec<NetworkSender<NetworkMessage<DeltaUpdateMessage<DeltaUpdate, State>>>>,
    /// The sender used to send the final state to the output block.
    output_sender: BoundedChannelSender<StreamElement<State>>,
    /// The identifier of the block where the final state should be sent to.
    output_block_id: BlockId,

    /// The content of the stream to replay.
    content: Vec<StreamElement<Out>>,
    /// The index inside `content` of the first message to be sent.
    content_index: usize,

    /// The index of the current iteration (0-based).
    iteration_index: usize,
    /// The maximum number of iterations to perform.
    num_iterations: usize,

    /// Whether the input stream has ended or not.
    ///
    /// This will be `None` if items are still expected to arrive from `prev`. When `End` or
    /// `IterEnd` has been received it is stored here. When all the iterations end, this will be
    /// returned, allowing for nested iterations.
    has_ended: Option<StreamElement<Out>>,

    /// Whether this `Replay` is the leader.
    is_leader: bool,
    /// The number of replicas of this block.
    num_replicas: usize,

    /// The current global state of the iteration.
    state: Option<State>,
    /// A reference to the state of the iteration that is visible to the loop operators.
    state_ref: IterationStateHandle<State>,

    /// The function that combines the global state with a delta update.
    #[derivative(Debug = "ignore")]
    global_fold: Arc<dyn Fn(State, DeltaUpdate) -> State + Send + Sync>,
    /// A function that, given the global state, checks whether the iteration should continue.
    #[derivative(Debug = "ignore")]
    loop_condition: Arc<dyn Fn(&mut State) -> bool + Send + Sync>,
}

fn clone_with_none<T>(_: &Option<T>) -> Option<T> {
    None
}

/// Similar to `EndBlock`, but tied specifically for the `Replay` iterations.
///
/// This block will receive the data (i.e. the `DeltaUpdate` already reduced) and send back to the
/// leader.
///
/// `EndBlock` cannot be used here since special care should be taken when the input stream is
/// empty.
#[derive(Debug, Clone)]
struct ReplayEndBlock<DeltaUpdate: Data, State: Data, OperatorChain>
where
    OperatorChain: Operator<DeltaUpdateMessage<DeltaUpdate, State>>,
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
    /// The block id of the block containing the `Replay` operator.
    replay_block_id: BlockId,
    /// The sender that points to the `Replay` leader for sending the
    /// `DeltaUpdateMessage::DeltaUpdate` messages.
    sender: Option<NetworkSender<NetworkMessage<DeltaUpdateMessage<DeltaUpdate, State>>>>,
}

impl<DeltaUpdate: Data, State: Data, OperatorChain>
    ReplayEndBlock<DeltaUpdate, State, OperatorChain>
where
    OperatorChain: Operator<DeltaUpdateMessage<DeltaUpdate, State>>,
{
    fn new(prev: OperatorChain, replay_block_id: BlockId) -> Self {
        Self {
            prev,
            has_received_item: false,
            replay_block_id,
            sender: None,
        }
    }
}

impl<Out: Data, DeltaUpdate: Data, State: Data, OperatorChain>
    Replay<Out, DeltaUpdate, State, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    #[allow(clippy::too_many_arguments)]
    fn new(
        prev: OperatorChain,
        initial_state: State,
        state_ref: IterationStateHandle<State>,
        num_iterations: usize,
        global_fold: impl Fn(State, DeltaUpdate) -> State + Send + Sync + 'static,
        loop_condition: impl Fn(&mut State) -> bool + Send + Sync + 'static,
        feedback_block_id: Arc<AtomicUsize>,
        output_sender: BoundedChannelSender<StreamElement<State>>,
        output_block_id: BlockId,
    ) -> Self {
        Self {
            // these fields will be set inside the `setup` method
            coord: Coord::new(0, 0, 0),
            feedback: None,
            senders: Default::default(),
            is_leader: false,
            num_replicas: 0,

            prev,
            feedback_block_id,
            output_block_id,
            output_sender,
            content: Default::default(),
            content_index: 0,
            iteration_index: 0,
            num_iterations,
            has_ended: None,
            state: Some(initial_state),
            state_ref,
            global_fold: Arc::new(global_fold),
            loop_condition: Arc::new(loop_condition),
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
            Stream<Out, Replay<Out, DeltaUpdate, State, OperatorChain>>,
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

        let (output_sender, output_receiver) = BoundedChannelReceiver::new(OUTPUT_CHANNEL_CAPACITY);
        let output_stream =
            StreamEnvironmentInner::stream(env, ChannelSource::new(output_receiver));

        let iter_start = self.add_operator(|prev| {
            Replay::new(
                prev,
                initial_state,
                state,
                num_iterations,
                Box::new(global_fold),
                Box::new(loop_condition),
                feedback_block_id.clone(),
                output_sender,
                output_stream.block.id,
            )
        });
        let iter_in_id = iter_start.block.id;

        let iter_end = body(iter_start, state_clone)
            .key_by(|_| ())
            .fold(DeltaUpdate::default(), local_fold)
            .unkey()
            .map(|(_, x)| DeltaUpdateMessage::<DeltaUpdate, State>::DeltaUpdate(x));

        /// Close the loop by connecting the provided stream to the block containing the `Replay`
        /// operator.
        fn close_loop<
            State: Data,
            OperatorChain: Operator<DeltaUpdateMessage<DeltaUpdate, State>> + Send + 'static,
            DeltaUpdate: Data + Default,
        >(
            stream: Stream<DeltaUpdateMessage<DeltaUpdate, State>, OperatorChain>,
            replay_block_id: BlockId,
        ) -> BlockId {
            let iter_end = stream.add_operator(|prev| ReplayEndBlock::new(prev, replay_block_id));
            let replay_end_block_id = iter_end.block.id;

            let mut env = iter_end.env.borrow_mut();
            let scheduler = env.scheduler_mut();
            scheduler.add_block(iter_end.block);
            // connect the ReplayEndBlock to the Replay
            scheduler.connect_blocks(
                replay_end_block_id,
                replay_block_id,
                TypeId::of::<DeltaUpdateMessage<DeltaUpdate, State>>(),
            );
            // the Replay block needs a self-loop
            scheduler.connect_blocks(
                replay_block_id,
                replay_block_id,
                TypeId::of::<DeltaUpdateMessage<DeltaUpdate, State>>(),
            );
            replay_end_block_id
        }

        // The body didn't create any new block, we need to make a new block and separate Replay
        // from ReplayEndBlock to avoid nasty deadlocks in the leader worker.
        // If there are more replicas than the capacity of the local channel for the feedback it's
        // possible that the leader's call to send from ReplayEndBlock would block finding the
        // channel full. But if that block is the same as the leader's Replay, a deadlock occurs
        // since that channel cannot be consumed.
        let replay_end_block_id = if iter_end.block.id == iter_in_id {
            close_loop(
                iter_end.add_block(EndBlock::new, NextStrategy::OnlyOne),
                iter_in_id,
            )
        } else {
            close_loop(iter_end, iter_in_id)
        };

        // store the id of the block containing the ReplayEndBlock
        feedback_block_id.store(replay_end_block_id, Ordering::Release);

        // TODO: check parallelism and make sure the blocks are spawned on the same replicas

        output_stream
    }
}

impl<DeltaUpdate: Data, State: Data, OperatorChain> Operator<()>
    for ReplayEndBlock<DeltaUpdate, State, OperatorChain>
where
    DeltaUpdate: Default,
    OperatorChain: Operator<DeltaUpdateMessage<DeltaUpdate, State>>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        let mut network = metadata.network.lock().unwrap();

        let replicas = network.replicas(self.replay_block_id);
        let leader = select_leader(&replicas);
        debug!("ReplayEndBlock {} has {} as leader", metadata.coord, leader);

        let sender = network.get_sender(ReceiverEndpoint::new(leader, metadata.coord.block_id));
        self.sender = Some(sender);

        drop(network);

        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<()> {
        let elem = self.prev.next();
        match &elem {
            StreamElement::Item(_) => {
                self.sender.as_ref().unwrap().send(vec![elem]).unwrap();
                self.has_received_item = true;
                StreamElement::Item(())
            }
            StreamElement::IterEnd => {
                // If two IterEnd has been received in a row it means that no message went through
                // the iteration inside this replica. Nevertheless the DeltaUpdate must be sent to
                // the leader.
                if !self.has_received_item {
                    let update = DeltaUpdateMessage::DeltaUpdate(Default::default());
                    self.sender
                        .as_ref()
                        .unwrap()
                        .send(vec![StreamElement::Item(update)])
                        .unwrap();
                }
                self.has_received_item = false;
                StreamElement::IterEnd
            }
            StreamElement::FlushBatch | StreamElement::End => elem.map(|_| unreachable!()),
            _ => unreachable!(),
        }
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> ReplayEndBlock<{}>",
            self.prev.to_string(),
            std::any::type_name::<DeltaUpdateMessage<DeltaUpdate, State>>()
        )
    }

    fn structure(&self) -> BlockStructure {
        let mut operator =
            OperatorStructure::new::<DeltaUpdateMessage<DeltaUpdate, State>, _>("ReplayEndBlock");
        operator
            .connections
            .push(Connection::new::<DeltaUpdateMessage<DeltaUpdate, State>>(
                self.replay_block_id,
                &NextStrategy::OnlyOne,
            ));
        self.prev.structure().add_operator(operator)
    }
}

impl<Out: Data, DeltaUpdate: Data, State: Data, OperatorChain> Operator<Out>
    for Replay<Out, DeltaUpdate, State, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata.clone());

        let leader = select_leader(&metadata.replicas);
        debug!("Replay block {} has {} as leader", metadata.coord, leader);
        self.is_leader = leader == metadata.coord;

        let mut network = metadata.network.lock().unwrap();
        if self.is_leader {
            for replica in &metadata.replicas {
                // the leader won't send the message back to itself
                if *replica == metadata.coord {
                    continue;
                }
                let endpoint = ReceiverEndpoint::new(*replica, metadata.coord.block_id);
                self.senders.push(network.get_sender(endpoint));
            }
            // at this point the id of the block with ReplayEndBlock must be known
            let feedback_block_id = self.feedback_block_id.load(Ordering::Acquire);
            // the leader receives the messages from the ReplayEndBlocks
            self.feedback = Some(
                network.get_receiver(ReceiverEndpoint::new(metadata.coord, feedback_block_id)),
            );
        } else {
            // the followers receive the messages from the leader
            self.feedback =
                Some(network.get_receiver(ReceiverEndpoint::new(metadata.coord, leader.block_id)));
        }
        self.num_replicas = metadata.replicas.len();
        self.coord = metadata.coord;
    }

    fn next(&mut self) -> StreamElement<Out> {
        // input has not ended yet
        if self.has_ended.is_none() {
            let item = self.prev.next();

            return match &item {
                StreamElement::End | StreamElement::IterEnd => {
                    debug!(
                        "Replay at {} received all the input: {} elements total",
                        self.coord,
                        self.content.len()
                    );
                    self.has_ended = Some(item);
                    self.content.push(StreamElement::IterEnd);
                    // the first iteration has already happened
                    self.content_index = self.content.len();
                    StreamElement::IterEnd
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
            };
        }

        // from here the input has for sure ended, so we need to replay it...

        // this iteration has not ended yet
        if self.content_index < self.content.len() {
            let item = self.content.get(self.content_index).unwrap().clone();
            self.content_index += 1;
            return item;
        }

        debug!(
            "Replay at {} has ended the iteration {}/{}",
            self.coord, self.iteration_index, self.num_iterations
        );

        // this iteration has ended
        let should_continue = if self.is_leader {
            // extract the current state for the reduction, it will be put back after the fold
            let mut state: State = self.state.take().expect("The state got lost");
            debug!(
                "Leader {} is waiting for {} delta updates",
                self.coord, self.num_replicas
            );
            for i in 0..self.num_replicas {
                let delta_update = loop {
                    let mut batch = self.feedback.as_ref().unwrap().recv().unwrap();
                    match batch.swap_remove(0) {
                        StreamElement::Item(DeltaUpdateMessage::DeltaUpdate(item)) => break item,
                        // the StartBlock may generate this messages, we should ignore them
                        StreamElement::FlushBatch => continue,
                        _ => unreachable!(
                            "Leader received something other than an Item(DeltaUpdate) from the feedback"
                        ),
                    };
                };
                debug!(
                    "Replay leader at {} received {}/{} delta updates",
                    self.coord,
                    i + 1,
                    self.num_replicas
                );
                state = (self.global_fold)(state, delta_update);
            }
            // the loop condition may change the state
            let should_continue = (self.loop_condition)(&mut state);
            self.state_ref.set(state.clone());
            debug!(
                "Replay leader at {} checked loop condition and resulted in {}",
                self.coord, should_continue
            );

            let new_state_message = DeltaUpdateMessage::<DeltaUpdate, State>::NextIteration(
                should_continue,
                state.clone(),
            );
            self.state = Some(state);
            for sender in &self.senders {
                sender
                    .send(vec![StreamElement::Item(new_state_message.clone())])
                    .unwrap();
            }
            should_continue
        } else {
            let (should_continue, new_state) = loop {
                let mut batch = self.feedback.as_ref().unwrap().recv().unwrap();
                match batch.swap_remove(0) {
                    StreamElement::Item(DeltaUpdateMessage::NextIteration(
                        should_continue,
                        new_state,
                    )) => break (should_continue, new_state),
                    elem => {
                        unreachable!(
                            "Leader sent something other than an Item(NextIteration): {}",
                            elem.variant()
                        )
                    }
                };
            };
            self.state_ref.set(new_state.clone());
            self.state = Some(new_state);
            should_continue
        };

        self.content_index = 0;
        self.iteration_index += 1;

        // the loop has ended
        if !should_continue || self.iteration_index >= self.num_iterations {
            // cleanup so that if this is a nested iteration next time we'll be good to start again
            self.content.clear();
            self.iteration_index = 0;
            // send the output state to the output block if this Replay is the leader
            if self.is_leader {
                self.output_sender
                    .send(StreamElement::Item(self.state.clone().unwrap()))
                    .unwrap();
                self.output_sender
                    .send(self.has_ended.clone().unwrap().map(|_| unreachable!()))
                    .unwrap();
            }
            return self.has_ended.take().unwrap();
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
        // feedback from ReplayEndBlock
        operator.receivers.push(OperatorReceiver::new::<
            DeltaUpdateMessage<DeltaUpdate, State>,
        >(self.feedback_block_id.load(Ordering::Acquire)));
        // self loop
        operator.receivers.push(OperatorReceiver::new::<
            DeltaUpdateMessage<DeltaUpdate, State>,
        >(self.coord.block_id));
        operator
            .connections
            .push(Connection::new::<DeltaUpdateMessage<DeltaUpdate, State>>(
                self.coord.block_id,
                &NextStrategy::OnlyOne,
            ));
        // output
        operator.connections.push(Connection::new::<State>(
            self.output_block_id,
            &NextStrategy::OnlyOne,
        ));
        self.prev.structure().add_operator(operator)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[test]
    fn test_replay_no_blocks_in_between() {
        let n = 20u64;
        let n_iter = 5;
        // a number of threads > n implies that at least one replica will receive 0 items
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(n as usize + 2));

        let source = source::IteratorSource::new(0..n);
        let state = env
            .stream(source)
            .shuffle()
            .map(|x| x)
            // the body of this iteration does not split the block (it's just a map)
            .replay(
                n_iter,
                1,
                |s, state| s.map(move |x| x * state.get()),
                |delta: u64, x| delta + x,
                |old_state, delta| old_state + delta,
                |state| {
                    *state -= 1;
                    true
                },
            )
            .collect_vec();
        env.execute();

        let res = state.get().unwrap();
        assert_eq!(res.len(), 1);
        let res = res.into_iter().next().unwrap();

        let mut state = 1;
        for _ in 0..n_iter {
            let s: u64 = (0..n).map(|x| x * state).sum();
            state = state + s - 1;
        }

        assert_eq!(res, state);
    }

    #[test]
    fn test_replay_with_shuffle() {
        let n = 20u64;
        let n_iter = 5;
        // a number of threads > n implies that at least one replica will receive 0 items
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(n as usize + 2));

        let source = source::IteratorSource::new(0..n);
        let state = env
            .stream(source)
            .shuffle()
            .map(|x| x)
            // the body of this iteration will split the block (there is a shuffle)
            .replay(
                n_iter,
                1,
                |s, state| s.map(move |x| x * state.get()).shuffle(),
                |delta: u64, x| delta + x,
                |old_state, delta| old_state + delta,
                |state| {
                    *state -= 1;
                    true
                },
            )
            .collect_vec();
        env.execute();

        let res = state.get().unwrap();
        assert_eq!(res.len(), 1);
        let res = res.into_iter().next().unwrap();

        let mut state = 1;
        for _ in 0..n_iter {
            let s: u64 = (0..n).map(|x| x * state).sum();
            state = state + s - 1;
        }

        assert_eq!(res, state);
    }
}
