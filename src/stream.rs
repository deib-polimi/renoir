use std::any::TypeId;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use itertools::Itertools;

use crate::block::{BatchMode, InnerBlock, NextStrategy};
use crate::environment::StreamEnvironmentInner;
use crate::operator::{Data, EndBlock, IterationStateLock, Operator, WindowDescription};
use crate::operator::{DataKey, StartBlock};
use std::marker::PhantomData;

/// Identifier of a block in the job graph.
pub type BlockId = usize;

/// On keyed streams, this is the type of the items of the stream.
pub type KeyValue<Key, Value> = (Key, Value);

/// A Stream represents a chain of operators that work on a flow of data. The type of the elements
/// entering the stream is `In`, while the type of the outgoing elements is `Out`.
///
/// Internally a stream is composed by a chain of blocks, each of which can be seen as a simpler
/// stream with input and output types.
///
/// A block is internally composed of a chain of operators, nested like the `Iterator` from `std`.
/// The type of the chain inside the block is `OperatorChain` and it's required as type argument of
/// the stream. This type only represents the chain inside the last block of the stream, not all the
/// blocks inside of it.
pub struct Stream<Out: Data, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    /// The last block inside the stream.
    pub(crate) block: InnerBlock<Out, OperatorChain>,
    /// A reference to the environment this stream lives in.
    pub(crate) env: Rc<RefCell<StreamEnvironmentInner>>,
}

/// A `KeyedStream` is like a set of `Stream`s, each of which partitioned by some `Key`. Internally
/// it's just a stream whose elements are `KeyValue` pairs and the operators behave following the
/// `KeyedStream` semantics.
///
/// The type of the `Key` must be a valid key inside an hashmap.
pub struct KeyedStream<Key: DataKey, Out: Data, OperatorChain>(
    pub Stream<KeyValue<Key, Out>, OperatorChain>,
)
where
    OperatorChain: Operator<KeyValue<Key, Out>>;

/// A `WindowedStream` is a data stream where elements are divided in multiple groups called
/// windows. Internally, a `WindowedStream` is just a `KeyedWindowedStream` where each element is
/// assigned to the same key `()`.
pub struct WindowedStream<Out: Data, OperatorChain, WinOut: Data, WinDescr>
where
    OperatorChain: Operator<KeyValue<(), Out>>,
    WinDescr: WindowDescription<(), WinOut>,
{
    pub(crate) inner: KeyedWindowedStream<(), Out, OperatorChain, WinOut, WinDescr>,
}

/// A `KeyedWindowedStream` is a data stream partitioned by `Key`, where elements of each partition
/// are divided in groups called windows.
/// Windows are handled independently for each partition of the stream.
pub struct KeyedWindowedStream<Key: DataKey, Out: Data, OperatorChain, WinOut: Data, WinDescr>
where
    OperatorChain: Operator<KeyValue<Key, Out>>,
    WinDescr: WindowDescription<Key, WinOut>,
{
    pub(crate) inner: KeyedStream<Key, Out, OperatorChain>,
    pub(crate) descr: WinDescr,
    pub(crate) _win_out: PhantomData<WinOut>,
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send + 'static,
{
    /// Add a new operator to the current chain inside the stream. This consumes the stream and
    /// returns a new one with the operator added.
    ///
    /// `get_operator` is a function that is given the previous chain of operators and should return
    /// the new chain of operators. The new chain cannot be simply passed as argument since it is
    /// required to do a partial move of the `InnerBlock` structure.
    #[doc(hidden)]
    pub fn add_operator<NewOut: Data, Op, GetOp>(self, get_operator: GetOp) -> Stream<NewOut, Op>
    where
        Op: Operator<NewOut> + 'static,
        GetOp: FnOnce(OperatorChain) -> Op,
    {
        Stream {
            block: InnerBlock {
                id: self.block.id,
                operators: get_operator(self.block.operators),
                batch_mode: self.block.batch_mode,
                iteration_state_lock_stack: self.block.iteration_state_lock_stack,
                is_only_one_strategy: false,
                scheduler_requirements: self.block.scheduler_requirements,
                _out_type: Default::default(),
            },
            env: self.env,
        }
    }

    /// Add a new block to the stream, closing and registering the previous one. The new block is
    /// connected to the previous one.
    ///
    /// `get_end_operator` is used to extend the operator chain of the old block with the last
    /// operator (e.g. `operator::EndBlock`, `operator::GroupByEndOperator`). The end operator must
    /// be an `Operator<()>`.
    ///
    /// The new block is initialized with a `StartBlock`.
    pub(crate) fn add_block<GetEndOp, Op>(
        self,
        get_end_operator: GetEndOp,
        next_strategy: NextStrategy<Out>,
    ) -> Stream<Out, StartBlock<Out>>
    where
        Op: Operator<()> + Send + 'static,
        GetEndOp: FnOnce(OperatorChain, NextStrategy<Out>, BatchMode) -> Op,
    {
        let batch_mode = self.block.batch_mode;
        let state_lock = self.block.iteration_state_lock_stack.clone();
        let mut old_stream =
            self.add_operator(|prev| get_end_operator(prev, next_strategy.clone(), batch_mode));
        old_stream.block.is_only_one_strategy = matches!(next_strategy, NextStrategy::OnlyOne);
        let mut env = old_stream.env.borrow_mut();
        let old_id = old_stream.block.id;
        let new_id = env.new_block();
        let scheduler = env.scheduler_mut();
        scheduler.add_block(old_stream.block);
        scheduler.connect_blocks(old_id, new_id, TypeId::of::<Out>());
        drop(env);

        let start_block = StartBlock::new(old_id, state_lock.last().cloned());
        Stream {
            block: InnerBlock::new(new_id, start_block, batch_mode, state_lock),
            env: old_stream.env,
        }
    }

    /// Similar to `.add_block`, but with 2 incoming blocks.
    ///
    /// This will add a new Y connection between two blocks. The two incoming blocks will be closed
    /// and a new one will be created with the 2 previous ones coming into it.
    ///
    /// This won't add any network shuffle, hence the next strategy will be `OnlyOne`. For this
    /// reason the 2 input streams must have the same parallelism, otherwise this function panics.
    ///
    /// The start operator of the new block must support multiple inputs: the provided function
    /// will be called with the ids of the 2 input blocks and should return the new start operator
    /// of the new block.
    pub(crate) fn add_y_connection<Out2, OperatorChain2, NewOut, StartOperator, GetStartOp>(
        self,
        oth: Stream<Out2, OperatorChain2>,
        get_start_operator: GetStartOp,
    ) -> Stream<NewOut, StartOperator>
    where
        Out2: Data,
        OperatorChain2: Operator<Out2> + Send + 'static,
        NewOut: Data,
        StartOperator: Operator<NewOut>,
        GetStartOp: Fn(BlockId, BlockId, Option<Arc<IterationStateLock>>) -> StartOperator,
    {
        let batch_mode = self.block.batch_mode;
        let scheduler_requirements1 = self.block.scheduler_requirements.clone();
        let scheduler_requirements2 = oth.block.scheduler_requirements.clone();
        if scheduler_requirements1.max_parallelism != scheduler_requirements2.max_parallelism {
            panic!(
                "The parallelism of the 2 blocks coming inside a Y connection must be equal. \
                On the left ({}) is {:?}, on the right ({}) is {:?}",
                self.block.to_string(),
                scheduler_requirements1.max_parallelism,
                oth.block.to_string(),
                scheduler_requirements2.max_parallelism
            );
        }
        let state_lock1 = &self.block.iteration_state_lock_stack;
        let state_lock2 = &oth.block.iteration_state_lock_stack;
        if state_lock1.iter().map(|s| Arc::as_ptr(s)).collect_vec()
            != state_lock2.iter().map(|s| Arc::as_ptr(s)).collect_vec()
        {
            panic!("The state locks of the 2 blocks coming inside a Y connection must be equal");
        }
        let state_lock = state_lock1.clone();

        // close previous blocks
        let mut old_stream1 =
            self.add_operator(|prev| EndBlock::new(prev, NextStrategy::OnlyOne, batch_mode));
        let mut old_stream2 =
            oth.add_operator(|prev| EndBlock::new(prev, NextStrategy::OnlyOne, batch_mode));
        old_stream1.block.is_only_one_strategy = true;
        old_stream2.block.is_only_one_strategy = true;

        let mut env = old_stream1.env.borrow_mut();
        let old_id1 = old_stream1.block.id;
        let old_id2 = old_stream2.block.id;
        let new_id = env.new_block();

        // add and connect the old blocks with the new one
        let scheduler = env.scheduler_mut();
        scheduler.add_block(old_stream1.block);
        scheduler.add_block(old_stream2.block);
        scheduler.connect_blocks(old_id1, new_id, TypeId::of::<Out>());
        scheduler.connect_blocks(old_id2, new_id, TypeId::of::<Out2>());
        drop(env);

        let mut new_stream = Stream {
            block: InnerBlock::new(
                new_id,
                get_start_operator(old_id1, old_id2, state_lock.last().cloned()),
                batch_mode,
                state_lock,
            ),
            env: old_stream1.env,
        };
        // make sure the new block has the same parallelism of the previous one
        new_stream.block.scheduler_requirements = scheduler_requirements1;
        new_stream
    }

    /// Clone the given block, taking care of connecting the new block to the same previous blocks
    /// of the original one.
    pub(crate) fn clone(&mut self) -> Self {
        let mut env = self.env.borrow_mut();
        let prev_nodes = env.scheduler_mut().prev_blocks(self.block.id).unwrap();
        let new_id = env.new_block();

        for (prev_node, typ) in prev_nodes.into_iter() {
            env.scheduler_mut().connect_blocks(prev_node, new_id, typ);
        }
        drop(env);

        let mut new_block = self.block.clone();
        new_block.id = new_id;
        Stream {
            block: new_block,
            env: self.env.clone(),
        }
    }

    /// Like `add_block` but without creating a new block. Therefore this closes the current stream
    /// and just add the last block to the scheduler.
    pub(crate) fn finalize_block(self) {
        let mut env = self.env.borrow_mut();
        info!("Finalizing block id={}", self.block.id);
        env.scheduler_mut().add_block(self.block);
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub(crate) fn add_operator<NewOut: Data, Op, GetOp>(
        self,
        get_operator: GetOp,
    ) -> KeyedStream<Key, NewOut, Op>
    where
        Op: Operator<KeyValue<Key, NewOut>> + 'static,
        GetOp: FnOnce(OperatorChain) -> Op,
    {
        KeyedStream(self.0.add_operator(get_operator))
    }
}
