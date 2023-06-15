use std::any::TypeId;
use std::collections::VecDeque;
use std::fmt::Display;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::block::{
    Block, BlockStructure, Connection, NextStrategy, OperatorReceiver, OperatorStructure,
    Replication,
};
use crate::channel::RecvError::Disconnected;
use crate::channel::SelectResult;
use crate::environment::StreamEnvironmentInner;
use crate::network::{Coord, NetworkMessage, NetworkReceiver, NetworkSender, ReceiverEndpoint};
use crate::operator::end::End;
use crate::operator::iteration::iteration_end::IterationEnd;
use crate::operator::iteration::leader::IterationLeader;
use crate::operator::iteration::state_handler::IterationStateHandler;
use crate::operator::iteration::{
    IterationResult, IterationStateHandle, IterationStateLock, StateFeedback,
};
use crate::operator::source::Source;
use crate::operator::start::Start;
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::scheduler::{BlockId, ExecutionMetadata};
use crate::stream::Stream;

fn clone_with_default<T: Default>(_: &T) -> T {
    T::default()
}

/// This is the first operator of the chain of blocks inside an iteration.
///
/// After an iteration what comes out of the loop will come back inside for the next iteration.
#[derive(Derivative)]
#[derivative(Debug, Clone)]
pub struct Iterate<Out: ExchangeData, State: ExchangeData> {
    /// The coordinate of this replica.
    coord: Coord,

    /// Helper structure that manages the iteration's state.
    state: IterationStateHandler<State>,

    /// The receiver of the data coming from the previous iteration of the loop.
    #[derivative(Clone(clone_with = "clone_with_default"))]
    input_receiver: Option<NetworkReceiver<Out>>,

    #[derivative(Clone(clone_with = "clone_with_default"))]
    feedback_receiver: Option<NetworkReceiver<Out>>,

    /// The id of the block that handles the feedback connection.
    feedback_end_block_id: Arc<AtomicUsize>,
    input_block_id: BlockId,
    /// The sender that will feed the data to the output of the iteration.
    output_sender: Option<NetworkSender<Out>>,
    /// The id of the block where the output of the iteration comes out.
    output_block_id: Arc<AtomicUsize>,

    /// The content of the stream to put back in the loop.
    content: VecDeque<StreamElement<Out>>,

    /// Used to store outside input arriving early
    input_stash: VecDeque<StreamElement<Out>>,
    /// The content to feed in the loop in the next iteration.
    feedback_content: VecDeque<StreamElement<Out>>,

    /// Whether the input stream has ended or not.
    input_finished: bool,
}

impl<Out: ExchangeData, State: ExchangeData> Iterate<Out, State> {
    fn new(
        state_ref: IterationStateHandle<State>,
        input_block_id: BlockId,
        leader_block_id: BlockId,
        feedback_end_block_id: Arc<AtomicUsize>,
        output_block_id: Arc<AtomicUsize>,
        state_lock: Arc<IterationStateLock>,
    ) -> Self {
        Self {
            // these fields will be set inside the `setup` method
            coord: Coord::new(0, 0, 0),
            input_receiver: None,
            feedback_receiver: None,
            feedback_end_block_id,
            input_block_id,
            output_sender: None,
            output_block_id,

            content: Default::default(),
            input_stash: Default::default(),
            feedback_content: Default::default(),
            input_finished: false,
            state: IterationStateHandler::new(leader_block_id, state_ref, state_lock),
        }
    }

    fn next_input(&mut self) -> Option<StreamElement<Out>> {
        let item = self.input_stash.pop_front()?;

        let el = match &item {
            StreamElement::FlushAndRestart => {
                log::debug!("input finished for iterate {}", self.coord);
                self.input_finished = true;
                // since this moment accessing the state for the next iteration must wait
                self.state.lock();
                StreamElement::FlushAndRestart
            }
            StreamElement::Item(_)
            | StreamElement::Timestamped(_, _)
            | StreamElement::Watermark(_)
            | StreamElement::FlushBatch => item,
            StreamElement::Terminate => {
                log::debug!("Iterate at {} is terminating", self.coord);
                let message = NetworkMessage::new_single(StreamElement::Terminate, self.coord);
                self.output_sender.as_ref().unwrap().send(message).unwrap();
                item
            }
        };
        Some(el)
    }

    fn next_stored(&mut self) -> Option<StreamElement<Out>> {
        let item = self.content.pop_front()?;
        if matches!(item, StreamElement::FlushAndRestart) {
            // since this moment accessing the state for the next iteration must wait
            self.state.lock();
        }
        Some(item)
    }

    fn feedback_finished(&self) -> bool {
        matches!(
            self.feedback_content.back(),
            Some(StreamElement::FlushAndRestart)
        )
    }

    pub(crate) fn input_or_feedback(&mut self) {
        let rx_feedback = self.feedback_receiver.as_ref().unwrap();

        if let Some(rx_input) = self.input_receiver.as_ref() {
            match rx_input.select(rx_feedback) {
                SelectResult::A(Ok(msg)) => {
                    self.input_stash.extend(msg.into_iter());
                }
                SelectResult::B(Ok(msg)) => {
                    self.feedback_content.extend(msg.into_iter());
                }
                SelectResult::A(Err(Disconnected)) => {
                    self.input_receiver = None;
                    self.input_or_feedback();
                }
                SelectResult::B(Err(Disconnected)) => {
                    log::error!("feedback_receiver disconnected!");
                    panic!("feedback_receiver disconnected!");
                }
            }
        } else {
            self.feedback_content
                .extend(rx_feedback.recv().unwrap().into_iter());
        }
    }

    pub(crate) fn wait_update(&mut self) -> StateFeedback<State> {
        // We need to stash inputs that arrive early to avoid deadlocks

        let rx_state = self.state.state_receiver().unwrap();
        loop {
            let state_msg = if let Some(rx_input) = self.input_receiver.as_ref() {
                match rx_state.select(rx_input) {
                    SelectResult::A(Ok(state_msg)) => state_msg,
                    SelectResult::A(Err(Disconnected)) => {
                        log::error!("state_receiver disconnected!");
                        panic!("state_receiver disconnected!");
                    }
                    SelectResult::B(Ok(msg)) => {
                        self.input_stash.extend(msg.into_iter());
                        continue;
                    }
                    SelectResult::B(Err(Disconnected)) => {
                        self.input_receiver = None;
                        continue;
                    }
                }
            } else {
                rx_state.recv().unwrap()
            };

            assert!(state_msg.num_items() == 1);

            match state_msg.into_iter().next().unwrap() {
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

impl<Out: ExchangeData, State: ExchangeData + Sync> Operator<Out> for Iterate<Out, State> {
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.coord = metadata.coord;

        let endpoint = ReceiverEndpoint::new(metadata.coord, self.input_block_id);
        self.input_receiver = Some(metadata.network.get_receiver(endpoint));

        let feedback_end_block_id = self.feedback_end_block_id.load(Ordering::Acquire) as BlockId;
        let feedback_endpoint = ReceiverEndpoint::new(metadata.coord, feedback_end_block_id);
        self.feedback_receiver = Some(metadata.network.get_receiver(feedback_endpoint));

        let output_block_id = self.output_block_id.load(Ordering::Acquire) as BlockId;
        let output_endpoint = ReceiverEndpoint::new(
            Coord::new(
                output_block_id,
                metadata.coord.host_id,
                metadata.coord.replica_id,
            ),
            metadata.coord.block_id,
        );
        self.output_sender = Some(metadata.network.get_sender(output_endpoint));

        self.state.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Out> {
        loop {
            // try to make progress on the feedback
            while let Ok(message) = self.feedback_receiver.as_ref().unwrap().try_recv() {
                self.feedback_content.extend(&mut message.into_iter());
            }

            if !self.input_finished {
                while self.input_stash.is_empty() {
                    self.input_or_feedback();
                }

                return self.next_input().unwrap();
            }

            if !self.content.is_empty() {
                return self.next_stored().unwrap();
            }

            while !self.feedback_finished() {
                self.input_or_feedback();
            }

            // All feedback received

            log::debug!("Iterate at {} has finished the iteration", self.coord);
            assert!(self.content.is_empty());
            std::mem::swap(&mut self.content, &mut self.feedback_content);

            let state_update = self.wait_update();

            if let IterationResult::Finished = self.state.wait_sync_state(state_update) {
                log::debug!("Iterate block at {} finished", self.coord,);
                // cleanup so that if this is a nested iteration next time we'll be good to start again
                self.input_finished = false;

                let message =
                    NetworkMessage::new_batch(self.content.drain(..).collect(), self.coord);
                self.output_sender.as_ref().unwrap().send(message).unwrap();
            }

            // This iteration has ended but FlushAndRestart has already been sent. To avoid sending
            // twice the FlushAndRestart repeat.
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("Iterate");
        operator
            .receivers
            .push(OperatorReceiver::new::<StateFeedback<State>>(
                self.state.leader_block_id,
            ));
        operator.receivers.push(OperatorReceiver::new::<Out>(
            self.feedback_end_block_id.load(Ordering::Acquire) as BlockId,
        ));
        operator
            .receivers
            .push(OperatorReceiver::new::<Out>(self.input_block_id));
        let output_block_id = self.output_block_id.load(Ordering::Acquire);
        operator.connections.push(Connection::new::<Out, _>(
            output_block_id as BlockId,
            &NextStrategy::only_one(),
        ));
        BlockStructure::default().add_operator(operator)
    }
}

impl<Out: ExchangeData, State: ExchangeData + Sync> Display for Iterate<Out, State> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Iterate<{}>", std::any::type_name::<Out>())
    }
}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Construct an iterative dataflow where the input stream is fed inside a cycle. What comes
    /// out of the loop will be fed back at the next iteration.
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
    /// This construct produces two stream:
    ///
    /// - the first is a stream with a single item: the final state of the iteration
    /// - the second if the set of elements that exited the loop during the last iteration (i.e. the
    ///   ones that should have been fed back in the next iteration).
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
    /// let (state, items) = s.iterate(
    ///     3, // at most 3 iterations
    ///     0, // the initial state is zero
    ///     |s, state| s.map(|n| n + 10),
    ///     |delta: &mut i32, n| *delta += n,
    ///     |state, delta| *state += delta,
    ///     |_state| true,
    /// );
    /// let state = state.collect_vec();
    /// let items = items.collect_vec();
    /// env.execute_blocking();
    ///
    /// assert_eq!(state.get().unwrap(), vec![10 + 11 + 12 + 20 + 21 + 22 + 30 + 31 + 32]);
    /// assert_eq!(items.get().unwrap(), vec![30, 31, 32]);
    /// ```
    pub fn iterate<
        Body,
        StateUpdate: ExchangeData + Default,
        State: ExchangeData + Sync,
        OperatorChain2,
    >(
        self,
        num_iterations: usize,
        initial_state: State,
        body: Body,
        local_fold: impl Fn(&mut StateUpdate, Out) + Send + Clone + 'static,
        global_fold: impl Fn(&mut State, StateUpdate) + Send + Clone + 'static,
        loop_condition: impl Fn(&mut State) -> bool + Send + Clone + 'static,
    ) -> (
        Stream<State, impl Operator<State>>,
        Stream<Out, impl Operator<Out>>,
    )
    where
        Body: FnOnce(
            Stream<Out, Iterate<Out, State>>,
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
        let shared_delta_update_end_block_id = Arc::new(AtomicUsize::new(0));
        let shared_feedback_end_block_id = Arc::new(AtomicUsize::new(0));
        let shared_output_block_id = Arc::new(AtomicUsize::new(0));

        // prepare the stream with the IterationLeader block, this will provide the state output
        let mut leader_stream = StreamEnvironmentInner::stream(
            env.clone(),
            IterationLeader::new(
                initial_state,
                num_iterations,
                global_fold,
                loop_condition,
                shared_delta_update_end_block_id.clone(),
            ),
        );
        let leader_block_id = leader_stream.block.id;
        // the output stream is outside this loop, so it doesn't have the lock for this state
        leader_stream.block.iteration_ctx = self.block.iteration_ctx.clone();

        // the lock for synchronizing the access to the state of this iteration
        let state_lock = Arc::new(IterationStateLock::default());

        let input_block_id = self.block.id;
        let batch_mode = self.block.batch_mode;
        let mut input =
            self.add_operator(|prev| End::new(prev, NextStrategy::only_one(), batch_mode));
        input.block.is_only_one_strategy = true;

        let iterate_block_id = {
            let mut env = env.lock();
            env.new_block_id()
        };
        let iter_source = Iterate::new(
            state,
            input_block_id,
            leader_block_id,
            shared_feedback_end_block_id.clone(),
            shared_output_block_id.clone(),
            state_lock.clone(),
        );

        let mut iter_start = Stream {
            block: Block::new(
                iterate_block_id,
                iter_source,
                batch_mode,
                input.block.iteration_ctx.clone(),
            ),
            env: env.clone(),
        };

        iter_start.block.iteration_ctx.push(state_lock.clone());
        // save the stack of the iteration for checking the stream returned by the body
        let pre_iter_stack = iter_start.block.iteration_ctx();

        // prepare the stream that will output the content of the loop
        let output = StreamEnvironmentInner::stream(
            env.clone(),
            Start::single(
                iterate_block_id,
                iter_start.block.iteration_ctx.last().cloned(),
            ),
        );
        let output_block_id = output.block.id;

        // attach the body of the loop to the Iterate operator
        let body = body(iter_start, state_clone);

        // Split the body of the loop in 2: the end block of the loop must ignore the output stream
        // since it's manually handled by the Iterate operator.
        let mut body = body.split_block(
            move |prev, next_strategy, batch_mode| {
                let mut end = End::new(prev, next_strategy, batch_mode);
                end.ignore_destination(output_block_id);
                end
            },
            NextStrategy::only_one(),
        );
        let body_block_id = body.block.id;

        let post_iter_stack = body.block.iteration_ctx();
        if pre_iter_stack != post_iter_stack {
            panic!("The body of the iteration should return the stream given as parameter");
        }
        body.block.iteration_ctx.pop().unwrap();

        // First split of the body: the data will be reduced into delta updates
        let state_update_end = StreamEnvironmentInner::stream(
            env.clone(),
            Start::single(body.block.id, Some(state_lock)),
        )
        .key_by(|_| ())
        .fold(StateUpdate::default(), local_fold)
        .drop_key()
        .add_operator(|prev| IterationEnd::new(prev, leader_block_id));
        let state_update_end_block_id = state_update_end.block.id;

        // Second split of the body: the data will be fed back to the Iterate block
        let batch_mode = body.block.batch_mode;
        let mut feedback_end = body.add_operator(|prev| {
            let mut end = End::new(prev, NextStrategy::only_one(), batch_mode);
            end.mark_feedback(iterate_block_id);
            end
        });
        feedback_end.block.is_only_one_strategy = true;
        let feedback_end_block_id = feedback_end.block.id;

        let mut env = env.lock();
        let scheduler = env.scheduler_mut();
        scheduler.schedule_block(state_update_end.block);
        scheduler.schedule_block(feedback_end.block);
        scheduler.schedule_block(input.block);
        scheduler.connect_blocks(input_block_id, iterate_block_id, TypeId::of::<Out>());
        // connect the end of the loop to the IterationEnd
        scheduler.connect_blocks(
            body_block_id,
            state_update_end_block_id,
            TypeId::of::<Out>(),
        );
        // connect the IterationEnd to the IterationLeader
        scheduler.connect_blocks(
            state_update_end_block_id,
            leader_block_id,
            TypeId::of::<StateUpdate>(),
        );
        // connect the IterationLeader to the Iterate
        scheduler.connect_blocks(
            leader_block_id,
            iterate_block_id,
            TypeId::of::<StateFeedback<State>>(),
        );
        // connect the feedback
        scheduler.connect_blocks(feedback_end_block_id, iterate_block_id, TypeId::of::<Out>());
        // connect the output stream
        scheduler.connect_blocks_fragile(iterate_block_id, output_block_id, TypeId::of::<Out>());
        drop(env);

        // store the id of the blocks we now know
        shared_delta_update_end_block_id
            .store(state_update_end_block_id as usize, Ordering::Release);
        shared_feedback_end_block_id.store(feedback_end_block_id as usize, Ordering::Release);
        shared_output_block_id.store(output_block_id as usize, Ordering::Release);

        // TODO: check parallelism and make sure the blocks are spawned on the same replicas

        // FIXME: this add_block is here just to make sure that the NextStrategy of output_stream
        //        is not changed by the following operators. This because the next strategy affects
        //        the connections made by the scheduler and if accidentally set to OnlyOne will
        //        break the connections.
        (
            leader_stream.split_block(End::new, NextStrategy::random()),
            output,
        )
    }
}

impl<Out: ExchangeData, State: ExchangeData + Sync> Source<Out> for Iterate<Out, State> {
    fn replication(&self) -> Replication {
        Replication::Unlimited
    }
}
