use std::fmt::Display;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::block::{BlockStructure, Connection, NextStrategy, OperatorStructure, Replication};
use crate::network::{Coord, NetworkMessage, NetworkSender};
use crate::operator::iteration::{IterationResult, StateFeedback};
use crate::operator::source::Source;
use crate::operator::start::{SimpleStartOperator, Start, StartReceiver};
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::profiler::{get_profiler, Profiler};
use crate::scheduler::{BlockId, ExecutionMetadata};

/// The leader block of an iteration.
///
/// This block is the synchronization point for the distributed iterations. At the end of each
/// iteration the `IterationEnd` will send a `DeltaUpdate` each to this block. When all the
/// `DeltaUpdate`s are received the new state is computed and the loop condition is evaluated.
///
/// After establishing if a new iteration should start this block will send a message to all the
/// "iteration blocks" (i.e. the blocks that manage the body of the iteration). When all the
/// iterations have been completed this block will produce the final state of the iteration,
/// followed by a `StreamElement::FlushAndReset`.
#[derive(Derivative)]
#[derivative(Clone, Debug)]
pub struct IterationLeader<StateUpdate: ExchangeData, State: ExchangeData, Global, LoopCond>
where
    Global: Fn(&mut State, StateUpdate) + Send + Clone,
    LoopCond: Fn(&mut State) -> bool + Send + Clone,
{
    /// The coordinates of this block.
    coord: Coord,

    /// The index of the current iteration (0-based).
    iteration_index: usize,
    /// The maximum number of iterations to perform.
    max_iterations: usize,

    /// The current global state of the iteration.
    ///
    /// It's an `Options` because the block needs to take ownership of it for processing it. It will
    /// be `None` only during that time frame.
    state: Option<State>,
    /// The initial value of the global state of the iteration.
    ///
    /// Will be used for resetting the global state after all the iterations complete.
    initial_state: State,

    /// The receiver from the `IterationEnd`s at the end of the loop.
    ///
    /// This will be set inside `setup` when we will know the id of that block.
    state_update_receiver: Option<SimpleStartOperator<StateUpdate>>,
    /// The number of replicas of `IterationEnd`.
    num_receivers: usize,
    /// The id of the block where `IterationEnd` is.
    ///
    /// This is a shared reference because when this block is constructed the tail of the iteration
    /// (i.e. the `IterationEnd`) is not constructed yet. Therefore we cannot predict how many
    /// blocks there will be in between, and therefore we cannot know the block id.
    ///
    /// After constructing the entire iteration this shared variable will be set. This will happen
    /// before the call to `setup`.
    feedback_block_id: Arc<AtomicUsize>,
    /// The senders to the start block of the iteration for the information about the new iteration.
    feedback_senders: Vec<NetworkSender<StateFeedback<State>>>,
    /// Whether `next` should emit a `FlushAndRestart` in the next call.
    flush_and_restart: bool,

    /// The function that combines the global state with a delta update.
    #[derivative(Debug = "ignore")]
    global_fold: Global,
    /// A function that, given the global state, checks whether the iteration should continue.
    #[derivative(Debug = "ignore")]
    loop_condition: LoopCond,
}

impl<DeltaUpdate: ExchangeData, State: ExchangeData, Global, LoopCond> Display
    for IterationLeader<DeltaUpdate, State, Global, LoopCond>
where
    Global: Fn(&mut State, DeltaUpdate) + Send + Clone,
    LoopCond: Fn(&mut State) -> bool + Send + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IterationLeader<{}>", std::any::type_name::<State>())
    }
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
            state_update_receiver: None,
            feedback_senders: Default::default(),
            coord: Coord::new(0, 0, 0),
            num_receivers: 0,

            max_iterations: num_iterations,
            iteration_index: 0,
            state: Some(initial_state.clone()),
            initial_state,
            feedback_block_id,
            flush_and_restart: false,
            global_fold,
            loop_condition,
        }
    }

    fn process_updates(&mut self) -> Option<StreamElement<State>> {
        let mut missing_state_updates = self.num_receivers;
        let rx = self.state_update_receiver.as_mut().unwrap();
        while missing_state_updates > 0 {
            match rx.next() {
                StreamElement::Item(state_update) => {
                    missing_state_updates -= 1;
                    log::trace!(
                        "iter_leader delta_update {}, {} left",
                        self.coord,
                        missing_state_updates
                    );
                    (self.global_fold)(self.state.as_mut().unwrap(), state_update);
                }
                StreamElement::Terminate => {
                    log::trace!("iter_leader terminate {}", self.coord);
                    return Some(StreamElement::Terminate);
                }
                StreamElement::FlushAndRestart | StreamElement::FlushBatch => {}
                update => unreachable!(
                    "IterationLeader received an invalid message: {}",
                    update.variant()
                ),
            }
        }
        None
    }

    /// Returns Some if it's the last loop and the state should be returned
    fn final_result(&mut self) -> Option<State> {
        let loop_condition = (self.loop_condition)(self.state.as_mut().unwrap());
        let more_iterations = self.iteration_index < self.max_iterations;
        let should_continue = loop_condition && more_iterations;

        if !loop_condition {
            log::trace!("iter_leader finish_condition {}", self.coord,);
        }
        if !more_iterations {
            log::trace!("iter_leader finish_max_iter {}", self.coord);
        }

        if should_continue {
            None
        } else {
            // reset the global state at the end of the iteration
            let state = self.state.take();
            self.state = Some(self.initial_state.clone());
            state
        }
    }
}

impl<DeltaUpdate: ExchangeData, State: ExchangeData, Global, LoopCond> Operator<State>
    for IterationLeader<DeltaUpdate, State, Global, LoopCond>
where
    Global: Fn(&mut State, DeltaUpdate) + Send + Clone,
    LoopCond: Fn(&mut State) -> bool + Send + Clone,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.coord = metadata.coord;
        self.feedback_senders = metadata
            .network
            .get_senders(metadata.coord)
            .into_iter()
            .map(|(_, s)| s)
            .collect();

        // at this point the id of the block with IterationEnd must be known
        let feedback_block_id = self.feedback_block_id.load(Ordering::Acquire) as BlockId;
        // get the receiver for the delta updates
        let mut delta_update_receiver = Start::single(feedback_block_id, None);
        delta_update_receiver.setup(metadata);
        self.num_receivers = delta_update_receiver.receiver().prev_replicas().len();
        self.state_update_receiver = Some(delta_update_receiver);
    }

    fn next(&mut self) -> StreamElement<State> {
        if self.flush_and_restart {
            self.flush_and_restart = false;
            return StreamElement::FlushAndRestart;
        }
        loop {
            log::trace!(
                "iter_leader {} {} delta updates left",
                self.coord,
                self.num_receivers
            );
            if let Some(value) = self.process_updates() {
                return value;
            }

            get_profiler().iteration_boundary(self.coord.block_id);
            self.iteration_index += 1;
            let result = self.final_result();

            let state_feedback = (
                IterationResult::from_condition(result.is_none()),
                self.state.clone().unwrap(),
            );
            for sender in &self.feedback_senders {
                let message = NetworkMessage::new_single(
                    StreamElement::Item(state_feedback.clone()),
                    self.coord,
                );
                sender.send(message).unwrap();
            }

            if let Some(state) = result {
                self.flush_and_restart = true;
                self.iteration_index = 0;
                return StreamElement::Item(state);
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<State, _>("IterationLeader");
        operator
            .connections
            .push(Connection::new::<StateFeedback<State>, _>(
                self.feedback_senders[0].receiver_endpoint.coord.block_id,
                &NextStrategy::only_one(),
            ));
        self.state_update_receiver
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
    fn replication(&self) -> Replication {
        Replication::One
    }
}
