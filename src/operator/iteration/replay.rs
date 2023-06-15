use std::any::TypeId;
use std::fmt::Display;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::block::{BlockStructure, NextStrategy, OperatorReceiver, OperatorStructure};
use crate::environment::StreamEnvironmentInner;
use crate::network::Coord;
use crate::operator::end::End;
use crate::operator::iteration::iteration_end::IterationEnd;
use crate::operator::iteration::leader::IterationLeader;
use crate::operator::iteration::state_handler::IterationStateHandler;
use crate::operator::iteration::{
    IterationResult, IterationStateHandle, IterationStateLock, StateFeedback,
};
use crate::operator::{Data, ExchangeData, Operator, StreamElement};
use crate::scheduler::{BlockId, ExecutionMetadata};
use crate::stream::Stream;

/// This is the first operator of the chain of blocks inside an iteration.
///
/// If a new iteration should start, the initial dataset is replayed.
#[derive(Debug, Clone)]
pub struct Replay<Out: Data, State: ExchangeData, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    /// The coordinate of this replica.
    coord: Coord,

    /// Helper structure that manages the iteration's state.
    state: IterationStateHandler<State>,

    /// The chain of previous operators where the dataset to replay is read from.
    prev: OperatorChain,

    /// The content of the stream to replay.
    content: Vec<StreamElement<Out>>,
    /// The index inside `content` of the first message to be sent.
    content_index: usize,

    /// Whether the input stream has ended or not.
    input_finished: bool,
}

impl<Out: Data, State: ExchangeData, OperatorChain> Display for Replay<Out, State, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Replay<{}>",
            self.prev,
            std::any::type_name::<Out>()
        )
    }
}

impl<Out: Data, State: ExchangeData, OperatorChain> Replay<Out, State, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    fn new(
        prev: OperatorChain,
        state_ref: IterationStateHandle<State>,
        leader_block_id: BlockId,
        state_lock: Arc<IterationStateLock>,
    ) -> Self {
        Self {
            // these fields will be set inside the `setup` method
            coord: Coord::new(0, 0, 0),

            prev,
            content: Default::default(),
            content_index: 0,
            input_finished: false,
            state: IterationStateHandler::new(leader_block_id, state_ref, state_lock),
        }
    }

    fn input_next(&mut self) -> Option<StreamElement<Out>> {
        if self.input_finished {
            return None;
        }

        let item = match self.prev.next() {
            StreamElement::FlushAndRestart => {
                log::debug!(
                    "Replay at {} received all the input: {} elements total",
                    self.coord,
                    self.content.len()
                );
                self.input_finished = true;
                self.content.push(StreamElement::FlushAndRestart);
                // the first iteration has already happened
                self.content_index = self.content.len();
                // since this moment accessing the state for the next iteration must wait
                self.state.lock();
                StreamElement::FlushAndRestart
            }
            // messages to save for the replay
            el @ StreamElement::Item(_)
            | el @ StreamElement::Timestamped(_, _)
            | el @ StreamElement::Watermark(_) => {
                self.content.push(el.clone());
                el
            }
            // messages to forward without replaying
            StreamElement::FlushBatch => StreamElement::FlushBatch,
            StreamElement::Terminate => {
                log::debug!("Replay at {} is terminating", self.coord);
                StreamElement::Terminate
            }
        };
        Some(item)
    }

    fn wait_update(&mut self) -> StateFeedback<State> {
        let state_receiver = self.state.state_receiver().unwrap();
        // TODO: check if affected by deadlock like iterate was in commit eb481da525850febe7cfb0963c6f3285252ecfaa
        // If there is the possibility of input staying still in the channel
        // waiting for the state, the iteration may deadlock
        // to solve instead of blocking on the state receiver,
        // a select must be performed allowing inputs to be stashed and
        // be pulled off the channel
        loop {
            let message = state_receiver.recv().unwrap();
            assert!(message.num_items() == 1);

            match message.into_iter().next().unwrap() {
                StreamElement::Item((should_continue, new_state)) => {
                    return (should_continue, new_state);
                }
                StreamElement::FlushBatch => {}
                StreamElement::FlushAndRestart => {}
                m => unreachable!(
                    "Iterate received invalid message from IterationLeader: {}",
                    m.variant()
                ),
            }
        }
    }
}

impl<Out: Data, State: ExchangeData + Sync, OperatorChain> Operator<Out>
    for Replay<Out, State, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.coord = metadata.coord;
        self.prev.setup(metadata);
        self.state.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Out> {
        loop {
            if let Some(value) = self.input_next() {
                return value;
            }
            // replay

            // this iteration has not ended yet
            if self.content_index < self.content.len() {
                let item = self.content[self.content_index].clone();
                self.content_index += 1;
                if matches!(item, StreamElement::FlushAndRestart) {
                    // since this moment accessing the state for the next iteration must wait
                    self.state.lock();
                }
                return item;
            }

            log::debug!("Replay at {} has ended the iteration", self.coord);

            self.content_index = 0;

            let state_update = self.wait_update();

            if let IterationResult::Finished = self.state.wait_sync_state(state_update) {
                log::debug!("Replay block at {} ended the iteration", self.coord);
                // cleanup so that if this is a nested iteration next time we'll be good to start again
                self.content.clear();
                self.input_finished = false;
            }

            // This iteration has ended but FlushAndRestart has already been sent. To avoid sending
            // twice the FlushAndRestart repeat.
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("Replay");
        operator
            .receivers
            .push(OperatorReceiver::new::<StateFeedback<State>>(
                self.state.leader_block_id,
            ));
        self.prev.structure().add_operator(operator)
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Construct an iterative dataflow where the input stream is repeatedly fed inside a cycle,
    /// i.e. what comes into the cycle is _replayed_ at every iteration.
    ///
    /// This iteration is stateful, this means that all the replicas have a read-only access to the
    /// _iteration state_. The initial value of the state is given as parameter. When an iteration
    /// ends all the elements are reduced locally at each replica producing a `DeltaUpdate`. Those
    /// delta updates are later reduced on a single node that, using the `global_fold` function will
    /// compute the state for the next iteration. This state is also used in `loop_condition` to
    /// check whether the next iteration should start or not. `loop_condition` is also allowed to
    /// mutate the state.
    ///
    /// The initial value of `DeltaUpdate` is initialized with [`Default::default()`].
    ///
    /// The content of the loop has a new scope: it's defined by the `body` function that takes as
    /// parameter the stream of data coming inside the iteration and a reference to the state. This
    /// function should return the stream of the data that exits from the loop (that will be fed
    /// back).
    ///
    /// This construct produces a single stream with a single element: the final state of the
    /// iteration.
    ///
    /// **Note**: due to an internal limitation, it's not currently possible to add an iteration
    /// operator when the stream has limited parallelism. This means, for example, that after a
    /// non-parallel source you have to add a shuffle.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new(0..3)).shuffle();
    /// let state = s.replay(
    ///     3, // at most 3 iterations
    ///     0, // the initial state is zero
    ///     |s, state| s.map(|n| n + 10),
    ///     |delta: &mut i32, n| *delta += n,
    ///     |state, delta| *state += delta,
    ///     |_state| true,
    /// );
    /// let state = state.collect_vec();
    /// env.execute_blocking();
    ///
    /// assert_eq!(state.get().unwrap(), vec![3 * (10 + 11 + 12)]);
    /// ```
    pub fn replay<
        Body,
        DeltaUpdate: ExchangeData + Default,
        State: ExchangeData + Sync,
        OperatorChain2,
    >(
        self,
        num_iterations: usize,
        initial_state: State,
        body: Body,
        local_fold: impl Fn(&mut DeltaUpdate, Out) + Send + Clone + 'static,
        global_fold: impl Fn(&mut State, DeltaUpdate) + Send + Clone + 'static,
        loop_condition: impl Fn(&mut State) -> bool + Send + Clone + 'static,
    ) -> Stream<State, impl Operator<State>>
    where
        Body: FnOnce(
            Stream<Out, Replay<Out, State, OperatorChain>>,
            IterationStateHandle<State>,
        ) -> Stream<Out, OperatorChain2>,
        OperatorChain2: Operator<Out> + 'static,
    {
        // this is required because if the iteration block is not present on all the hosts, the ones
        // without it won't receive the state updates.
        assert!(
            self.block.scheduler_requirements.replication.is_unlimited(),
            "Cannot have an iteration block with limited parallelism"
        );

        let state = IterationStateHandle::new(initial_state.clone());
        let state_clone = state.clone();
        let env = self.env.clone();

        // the id of the block where IterationEnd is. At this moment we cannot know it, so we
        // store a fake value inside this and as soon as we know it we set it to the right value.
        let feedback_block_id = Arc::new(AtomicUsize::new(0));

        let mut output_stream = StreamEnvironmentInner::stream(
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
        // the output stream is outside this loop, so it doesn't have the lock for this state
        output_stream.block.iteration_ctx = self.block.iteration_ctx.clone();

        // the lock for synchronizing the access to the state of this iteration
        let state_lock = Arc::new(IterationStateLock::default());

        let mut iter_start =
            self.add_operator(|prev| Replay::new(prev, state, leader_block_id, state_lock.clone()));
        let replay_block_id = iter_start.block.id;

        // save the stack of the iteration for checking the stream returned by the body
        iter_start.block.iteration_ctx.push(state_lock);
        let pre_iter_stack = iter_start.block.iteration_ctx();

        let mut iter_end = body(iter_start, state_clone)
            .key_by(|_| ())
            .fold(DeltaUpdate::default(), local_fold)
            .drop_key();

        let post_iter_stack = iter_end.block.iteration_ctx();
        if pre_iter_stack != post_iter_stack {
            panic!("The body of the iteration should return the stream given as parameter");
        }
        iter_end.block.iteration_ctx.pop().unwrap();

        let iter_end = iter_end.add_operator(|prev| IterationEnd::new(prev, leader_block_id));
        let iteration_end_block_id = iter_end.block.id;

        let mut env = iter_end.env.lock();
        let scheduler = env.scheduler_mut();
        scheduler.schedule_block(iter_end.block);
        // connect the IterationEnd to the IterationLeader
        scheduler.connect_blocks(
            iteration_end_block_id,
            leader_block_id,
            TypeId::of::<DeltaUpdate>(),
        );
        scheduler.connect_blocks(
            leader_block_id,
            replay_block_id,
            TypeId::of::<StateFeedback<State>>(),
        );
        drop(env);

        // store the id of the block containing the IterationEnd
        feedback_block_id.store(iteration_end_block_id as usize, Ordering::Release);

        // TODO: check parallelism and make sure the blocks are spawned on the same replicas

        // FIXME: this add_block is here just to make sure that the NextStrategy of output_stream
        //        is not changed by the following operators. This because the next strategy affects
        //        the connections made by the scheduler and if accidentally set to OnlyOne will
        //        break the connections.
        output_stream.split_block(End::new, NextStrategy::random())
    }
}
