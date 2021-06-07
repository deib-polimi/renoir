use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::block::{BlockStructure, Connection, NextStrategy, OperatorStructure};
use crate::network::{Coord, NetworkMessage, NetworkSender};
use crate::operator::iteration::NewIterationState;
use crate::operator::source::Source;
use crate::operator::start::{SingleStartBlockReceiverOperator, StartBlock, StartBlockReceiver};
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::profiler::{get_profiler, Profiler};
use crate::scheduler::ExecutionMetadata;

/// The leader block of an iteration.
///
/// This block is the synchronization point for the distributed iterations. At the end of each
/// iteration the `IterationEndBlock` will send a `DeltaUpdate` each to this block. When all the
/// `DeltaUpdate`s are received the new state is computed and the loop condition is evaluated.
///
/// After establishing if a new iteration should start this block will send a message to all the
/// "iteration blocks" (i.e. the blocks that manage the body of the iteration). When all the
/// iterations have been completed this block will produce the final state of the iteration,
/// followed by a `StreamElement::FlushAndReset`.
#[derive(Derivative)]
#[derivative(Clone, Debug)]
pub struct IterationLeader<DeltaUpdate: ExchangeData, State: ExchangeData, Global, LoopCond>
where
    Global: Fn(&mut State, DeltaUpdate) + Send + Clone,
    LoopCond: Fn(&mut State) -> bool + Send + Clone,
{
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

    /// The receiver from the `IterationEndBlock`s at the end of the loop.
    ///
    /// This will be set inside `setup` when we will know the id of that block.
    delta_update_receiver: Option<SingleStartBlockReceiverOperator<DeltaUpdate>>,
    /// The number of replicas of `IterationEndBlock`.
    num_receivers: usize,
    /// The id of the block where `IterationEndBlock` is.
    ///
    /// This is a shared reference because when this block is constructed the tail of the iteration
    /// (i.e. the `IterationEndBlock`) is not constructed yet. Therefore we cannot predict how many
    /// blocks there will be in between, and therefore we cannot know the block id.
    ///
    /// After constructing the entire iteration this shared variable will be set. This will happen
    /// before the call to `setup`.
    feedback_block_id: Arc<AtomicUsize>,
    /// The senders to the start block of the iteration for the information about the new iteration.
    new_state_senders: Vec<NetworkSender<NewIterationState<State>>>,
    /// Whether `next` should emit a `FlushAndRestart` in the next call.
    emit_flush_and_restart: bool,

    /// The function that combines the global state with a delta update.
    #[derivative(Debug = "ignore")]
    global_fold: Global,
    /// A function that, given the global state, checks whether the iteration should continue.
    #[derivative(Debug = "ignore")]
    loop_condition: LoopCond,
}

impl<DeltaUpdate: ExchangeData, State: ExchangeData, Global, LoopCond>
    IterationLeader<DeltaUpdate, State, Global, LoopCond>
where
    Global: Fn(&mut State, DeltaUpdate) + Send + Clone,
    LoopCond: Fn(&mut State) -> bool + Send + Clone,
{
    pub fn new(
        initial_state: State,
        num_iterations: usize,
        global_fold: Global,
        loop_condition: LoopCond,
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
            global_fold,
            loop_condition,
        }
    }
}

impl<DeltaUpdate: ExchangeData, State: ExchangeData, Global, LoopCond> Operator<State>
    for IterationLeader<DeltaUpdate, State, Global, LoopCond>
where
    Global: Fn(&mut State, DeltaUpdate) + Send + Clone,
    LoopCond: Fn(&mut State) -> bool + Send + Clone,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        let mut network = metadata.network.lock().unwrap();
        self.coord = metadata.coord;
        self.new_state_senders = network
            .get_senders(metadata.coord)
            .into_iter()
            .map(|(_, s)| s)
            .collect();
        drop(network);

        // at this point the id of the block with IterationEndBlock must be known
        let feedback_block_id = self.feedback_block_id.load(Ordering::Acquire);
        // get the receiver for the delta updates
        let mut delta_update_receiver = StartBlock::single(feedback_block_id, None);
        delta_update_receiver.setup(metadata);
        self.num_receivers = delta_update_receiver.receiver().prev_replicas().len();
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
                            "IterationLeader at {} received a delta update, {} missing",
                            self.coord, missing_delta_updates
                        );
                        (self.global_fold)(self.state.as_mut().unwrap(), delta_update);
                    }
                    StreamElement::Terminate => {
                        debug!("IterationLeader {} received Terminate", self.coord);
                        return StreamElement::Terminate;
                    }
                    StreamElement::FlushAndRestart => {}
                    StreamElement::FlushBatch => {}
                    _ => unreachable!(
                        "IterationLeader received an invalid message: {}",
                        update.variant()
                    ),
                }
            }
            // the loop condition may change the state
            let mut should_continue = (self.loop_condition)(self.state.as_mut().unwrap());
            debug!(
                "IterationLeader at {} checked loop condition and resulted in {}",
                self.coord, should_continue
            );
            self.iteration_index += 1;
            if self.iteration_index >= self.num_iterations {
                debug!("IterationLeader at {} reached iteration limit", self.coord);
                should_continue = false;
            }

            get_profiler().iteration_boundary(self.coord.block_id);

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
        format!("IterationLeader<{}>", std::any::type_name::<State>())
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<State, _>("IterationLeader");
        operator
            .connections
            .push(Connection::new::<NewIterationState<State>, _>(
                self.new_state_senders[0].receiver_endpoint.coord.block_id,
                &NextStrategy::only_one(),
            ));
        self.delta_update_receiver
            .as_ref()
            .unwrap()
            .structure()
            .add_operator(operator)
    }
}

impl<DeltaUpdate: ExchangeData, State: ExchangeData, Global, LoopCond> Source<State>
    for IterationLeader<DeltaUpdate, State, Global, LoopCond>
where
    Global: Fn(&mut State, DeltaUpdate) + Send + Clone,
    LoopCond: Fn(&mut State) -> bool + Send + Clone,
{
    fn get_max_parallelism(&self) -> Option<usize> {
        Some(1)
    }
}
