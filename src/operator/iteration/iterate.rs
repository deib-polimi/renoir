use std::any::TypeId;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::block::{BlockStructure, Connection, NextStrategy, OperatorReceiver, OperatorStructure};
use crate::environment::StreamEnvironmentInner;
use crate::network::{Coord, NetworkMessage, NetworkReceiver, NetworkSender, ReceiverEndpoint};
use crate::operator::iteration::iteration_end::IterationEndBlock;
use crate::operator::iteration::leader::IterationLeader;
use crate::operator::iteration::state_handler::IterationStateHandler;
use crate::operator::{
    Data, EndBlock, IterationStateHandle, IterationStateLock, NewIterationState, Operator,
    StartBlock, StreamElement,
};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{BlockId, Stream};

fn clone_with_default<T: Default>(_: &T) -> T {
    T::default()
}

/// This is the first operator of the chain of blocks inside an iteration.
///
/// After an iteration what comes out of the loop will come back inside for the next iteration.
#[derive(Derivative)]
#[derivative(Debug, Clone)]
pub struct Iterate<Out: Data, State: Data, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    /// The coordinate of this replica.
    coord: Coord,

    /// The chain of previous operators where the initial dataset is read from.
    prev: OperatorChain,

    /// Helper structure that manages the iteration's state.
    state: IterationStateHandler<State>,

    /// The receiver of the data coming from the previous iteration of the loop.
    #[derivative(Clone(clone_with = "clone_with_default"))]
    feedback_receiver: Option<NetworkReceiver<Out>>,
    /// The id of the block that handles the feedback connection.
    feedback_end_block_id: Arc<AtomicUsize>,
    /// The sender that will feed the data to the output of the iteration.
    output_sender: Option<NetworkSender<Out>>,
    /// The id of the block where the output of the iteration comes out.
    output_block_id: Arc<AtomicUsize>,

    /// The content of the stream to put back in the loop.
    content: VecDeque<StreamElement<Out>>,
    /// The index inside `content` of the first message to be sent.
    content_index: usize,
    /// The content to feed in the loop in the next iteration.
    next_content: VecDeque<StreamElement<Out>>,

    /// Whether the input stream has ended or not.
    has_input_ended: bool,
}

impl<Out: Data, State: Data, OperatorChain> Iterate<Out, State, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    fn new(
        prev: OperatorChain,
        state_ref: IterationStateHandle<State>,
        leader_block_id: BlockId,
        feedback_end_block_id: Arc<AtomicUsize>,
        output_block_id: Arc<AtomicUsize>,
        state_lock: Arc<IterationStateLock>,
    ) -> Self {
        Self {
            // these fields will be set inside the `setup` method
            coord: Coord::new(0, 0, 0),
            feedback_receiver: None,
            feedback_end_block_id,
            output_sender: None,
            output_block_id,

            prev,
            content: Default::default(),
            content_index: 0,
            next_content: Default::default(),
            has_input_ended: false,
            state: IterationStateHandler::new(leader_block_id, state_ref, state_lock),
        }
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn iterate<Body, DeltaUpdate: Data + Default, State: Data, OperatorChain2>(
        self,
        num_iterations: usize,
        initial_state: State,
        body: Body,
        local_fold: impl Fn(&mut DeltaUpdate, Out) + Send + Clone + 'static,
        global_fold: impl Fn(&mut State, DeltaUpdate) + Send + Clone + 'static,
        loop_condition: impl Fn(&mut State) -> bool + Send + Clone + 'static,
    ) -> (
        Stream<State, impl Operator<State>>,
        Stream<Out, impl Operator<Out>>,
    )
    where
        Body: Fn(
            Stream<Out, Iterate<Out, State, OperatorChain>>,
            IterationStateHandle<State>,
        ) -> Stream<Out, OperatorChain2>,
        OperatorChain2: Operator<Out> + 'static,
    {
        // this is required because if the iteration block is not present on all the hosts, the ones
        // without it won't receive the state updates.
        assert!(
            self.block.scheduler_requirements.max_parallelism.is_none(),
            "Cannot have an iteration block with limited parallelism"
        );

        let state = IterationStateHandle::new(initial_state.clone());
        let state_clone = state.clone();
        let env = self.env.clone();

        // the id of the block where IterationEndBlock is. At this moment we cannot know it, so we
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
        leader_stream.block.iteration_state_lock_stack =
            self.block.iteration_state_lock_stack.clone();

        // the lock for synchronizing the access to the state of this iteration
        let state_lock = Arc::new(IterationStateLock::default());

        // prepare the loop body stream
        let mut iter_start = self.add_operator(|prev| {
            Iterate::new(
                prev,
                state,
                leader_block_id,
                shared_feedback_end_block_id.clone(),
                shared_output_block_id.clone(),
                state_lock.clone(),
            )
        });
        let iterate_block_id = iter_start.block.id;

        // prepare the stream that will output the content of the loop
        let output = StreamEnvironmentInner::stream(
            env.clone(),
            StartBlock::new(
                iterate_block_id,
                iter_start.block.iteration_state_lock_stack.last().cloned(),
            ),
        );
        let output_block_id = output.block.id;

        iter_start
            .block
            .iteration_state_lock_stack
            .push(state_lock.clone());
        // attach the body of the loop to the Iterate operator
        let body_end = body(iter_start, state_clone);

        // Split the body of the loop in 2: the end block of the loop must ignore the output stream
        // since it's manually handled by the Iterate operator.
        let mut body_end = body_end.add_block(
            |prev, next_strategy, batch_mode| {
                let mut end = EndBlock::new(prev, next_strategy, batch_mode);
                end.ignore_destination(output_block_id);
                end
            },
            NextStrategy::OnlyOne,
        );
        let body_end_block_id = body_end.block.id;
        body_end.block.iteration_state_lock_stack.pop().unwrap();

        // First split of the body: the data will be reduced into delta updates
        let delta_update_end = StreamEnvironmentInner::stream(
            env.clone(),
            StartBlock::new(body_end_block_id, Some(state_lock)),
        )
        .key_by(|_| ())
        .fold(DeltaUpdate::default(), local_fold)
        .unkey()
        .map(|(_, delta_update)| delta_update)
        .add_operator(|prev| IterationEndBlock::new(prev, leader_block_id));
        let delta_update_end_block_id = delta_update_end.block.id;

        // Second split of the body: the data will be fed back to the Iterate block
        let batch_mode = body_end.block.batch_mode;
        let mut feedback_end = body_end.add_operator(|prev| {
            let mut end = EndBlock::new(prev, NextStrategy::OnlyOne, batch_mode);
            end.mark_feedback(iterate_block_id);
            end
        });
        feedback_end.block.is_only_one_strategy = true;
        let feedback_end_block_id = feedback_end.block.id;

        let mut env = env.borrow_mut();
        let scheduler = env.scheduler_mut();
        scheduler.add_block(delta_update_end.block);
        scheduler.add_block(feedback_end.block);
        // connect the end of the loop to the IterationEndBlock
        scheduler.connect_blocks(
            body_end_block_id,
            delta_update_end_block_id,
            TypeId::of::<Out>(),
        );
        // connect the IterationEndBlock to the IterationLeader
        scheduler.connect_blocks(
            delta_update_end_block_id,
            leader_block_id,
            TypeId::of::<DeltaUpdate>(),
        );
        // connect the IterationLeader to the Iterate
        scheduler.connect_blocks(
            leader_block_id,
            iterate_block_id,
            TypeId::of::<NewIterationState<State>>(),
        );
        // connect the feedback
        scheduler.connect_blocks(feedback_end_block_id, iterate_block_id, TypeId::of::<Out>());
        // connect the output stream
        scheduler.connect_blocks_fragile(iterate_block_id, output_block_id, TypeId::of::<Out>());
        drop(env);

        // store the id of the blocks we now know
        shared_delta_update_end_block_id.store(delta_update_end_block_id, Ordering::Release);
        shared_feedback_end_block_id.store(feedback_end_block_id, Ordering::Release);
        shared_output_block_id.store(output_block_id, Ordering::Release);

        // TODO: check parallelism and make sure the blocks are spawned on the same replicas

        // FIXME: this add_block is here just to make sure that the NextStrategy of output_stream
        //        is not changed by the following operators. This because the next strategy affects
        //        the connections made by the scheduler and if accidentally set to OnlyOne will
        //        break the connections.
        (
            leader_stream.add_block(EndBlock::new, NextStrategy::Random),
            output,
        )
    }
}

impl<Out: Data, State: Data, OperatorChain> Operator<Out> for Iterate<Out, State, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.coord = metadata.coord;

        let mut network = metadata.network.lock().unwrap();
        let feedback_end_block_id = self.feedback_end_block_id.load(Ordering::Acquire);
        let feedback_endpoint = ReceiverEndpoint::new(metadata.coord, feedback_end_block_id);
        self.feedback_receiver = Some(network.get_receiver(feedback_endpoint));

        let output_block_id = self.output_block_id.load(Ordering::Acquire);
        let output_endpoint = ReceiverEndpoint::new(
            Coord::new(
                output_block_id,
                metadata.coord.host_id,
                metadata.coord.replica_id,
            ),
            metadata.coord.block_id,
        );
        self.output_sender = Some(network.get_sender(output_endpoint));
        drop(network);

        self.prev.setup(metadata.clone());
        self.state.setup(metadata.clone());
    }

    fn next(&mut self) -> StreamElement<Out> {
        // try to make progress on the feedback
        while let Ok(message) = self.feedback_receiver.as_ref().unwrap().try_recv() {
            self.next_content.append(&mut message.batch().into());
        }

        if !self.has_input_ended {
            let item = self.prev.next();
            return match &item {
                StreamElement::FlushAndRestart => {
                    debug!(
                        "Iterate at {} received all the input: {} elements total",
                        self.coord,
                        self.content.len()
                    );
                    self.has_input_ended = true;
                    // since this moment accessing the state for the next iteration must wait
                    self.state.lock();
                    StreamElement::FlushAndRestart
                }
                StreamElement::Item(_)
                | StreamElement::Timestamped(_, _)
                | StreamElement::Watermark(_)
                | StreamElement::FlushBatch => item,
                StreamElement::Terminate => {
                    debug!("Iterate at {} is terminating", self.coord);
                    let message = NetworkMessage::new(vec![StreamElement::Terminate], self.coord);
                    self.output_sender.as_ref().unwrap().send(message).unwrap();
                    item
                }
            };
        } else if !self.content.is_empty() {
            let item = self.content.pop_front().unwrap();
            if matches!(item, StreamElement::FlushAndRestart) {
                // since this moment accessing the state for the next iteration must wait
                self.state.lock();
            }
            return item;
        }

        // make sure to consume all the feedback
        while !matches!(
            self.next_content.back(),
            Some(StreamElement::FlushAndRestart)
        ) {
            let message = self.feedback_receiver.as_ref().unwrap().recv().unwrap();
            self.next_content.append(&mut message.batch().into());
        }

        debug!("Iterate at {} has ended the iteration", self.coord);

        // make sure not to lose anything
        debug_assert!(self.content.is_empty());
        // the next iteration
        std::mem::swap(&mut self.content, &mut self.next_content);

        // this iteration has ended, wait here for the leader
        let should_continue = self.state.wait_leader();

        if !should_continue {
            debug!(
                "Iterate block at {} ended the iteration, producing: {:?}",
                self.coord,
                self.content.iter().map(|x| x.variant()).collect::<Vec<_>>()
            );
            // cleanup so that if this is a nested iteration next time we'll be good to start again
            self.has_input_ended = false;

            let message = NetworkMessage::new(self.content.drain(..).collect(), self.coord);
            self.output_sender.as_ref().unwrap().send(message).unwrap();
        }

        // This iteration has ended but FlushAndRestart has already been sent. To avoid sending
        // twice the FlushAndRestart recurse.
        self.next()
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> Iterate<{}>",
            self.prev.to_string(),
            std::any::type_name::<Out>()
        )
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("Iterate");
        operator
            .receivers
            .push(OperatorReceiver::new::<NewIterationState<State>>(
                self.state.leader_block_id,
            ));
        operator.receivers.push(OperatorReceiver::new::<Out>(
            self.feedback_end_block_id.load(Ordering::Acquire),
        ));
        let output_block_id = self.output_block_id.load(Ordering::Acquire);
        operator.connections.push(Connection::new::<Out>(
            output_block_id,
            &NextStrategy::OnlyOne,
        ));
        self.prev.structure().add_operator(operator)
    }
}
