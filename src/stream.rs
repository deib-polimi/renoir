use parking_lot::Mutex;

use std::marker::PhantomData;
use std::sync::Arc;

use crate::block::{BatchMode, Block, NextStrategy, SchedulerRequirements};
use crate::environment::StreamEnvironmentInner;
use crate::operator::end::End;
use crate::operator::iteration::IterationStateLock;
use crate::operator::source::Source;
use crate::operator::window::WindowDescription;
use crate::operator::DataKey;
use crate::operator::Start;
use crate::operator::{Data, ExchangeData, KeyerFn, Operator};
use crate::scheduler::BlockId;

/// A Stream represents a chain of operators that work on a flow of data. The type of the elements
/// that is leaving the stream is `Out`.
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
    pub(crate) block: Block<Out, OperatorChain>,
    /// A reference to the environment this stream lives in.
    pub(crate) env: Arc<Mutex<StreamEnvironmentInner>>,
}

/// A [`KeyedStream`] is like a set of [`Stream`]s, each of which partitioned by some `Key`. Internally
/// it's just a stream whose elements are `(K, V)` pairs and the operators behave following the
/// [`KeyedStream`] semantics.
///
/// The type of the `Key` must be a valid key inside an hashmap.
pub struct KeyedStream<Key: Data, Out: Data, OperatorChain>(pub Stream<(Key, Out), OperatorChain>)
where
    OperatorChain: Operator<(Key, Out)>;

/// A [`WindowedStream`] is a data stream partitioned by `Key`, where elements of each partition
/// are divided in groups called windows.
/// Each element can be assigned to one or multiple windows.
///
/// Windows are handled independently for each partition of the stream.
/// Each partition may be processed in parallel.
///
/// The windowing logic is implemented through 3 traits:
/// - A [`WindowDescription`] contains the parameters and logic that characterize the windowing strategy,
///   when given a `WindowAccumulator` it instantiates a `WindowManager`.
/// - A [`WindowManger`](crate::operator::window::WindowManager) is the struct responsible for creating
///   the windows and forwarding the input elements to the correct window which will should pass it to
///   its `WindowAccumulator`.
/// - A [`WindowAccumulator`](crate::operator::window::WindowAccumulator) contains the logic that should
///   be applied to the elements of each window.
///
/// There are a set of provided window descriptions with their respective managers:
///  - [`EventTimeWindow`][crate::operator::window::EventTimeWindow]
///  - [`ProcessingTimeWindow`][crate::operator::window::ProcessingTimeWindow]
///  - [`CountWindow`][crate::operator::window::CountWindow]
///  - [`SessionWindow`][crate::operator::window::SessionWindow]
///  - [`TransactionWindow`][crate::operator::window::TransactionWindow]
///
pub struct WindowedStream<K: DataKey, I: Data, Op, O: Data, WinDescr>
where
    Op: Operator<(K, I)>,
    WinDescr: WindowDescription<I>,
{
    pub(crate) inner: KeyedStream<K, I, Op>,
    pub(crate) descr: WinDescr,
    pub(crate) _win_out: PhantomData<O>,
}

impl<I: Data, Op> Stream<I, Op>
where
    Op: Operator<I> + 'static,
{
    /// Add a new operator to the current chain inside the stream. This consumes the stream and
    /// returns a new one with the operator added.
    ///
    /// `get_operator` is a function that is given the previous chain of operators and should return
    /// the new chain of operators. The new chain cannot be simply passed as argument since it is
    /// required to do a partial move of the `InnerBlock` structure.
    ///
    /// **Note**: this is an advanced function that manipulates the block structure. Probably it is
    /// not what you are looking for.
    pub fn add_operator<O: Data, Op2, GetOp>(self, get_operator: GetOp) -> Stream<O, Op2>
    where
        Op2: Operator<O> + 'static,
        GetOp: FnOnce(Op) -> Op2,
    {
        Stream {
            block: self.block.add_operator(get_operator),
            env: self.env,
        }
    }

    /// Add a new block to the stream, closing and registering the previous one. The new block is
    /// connected to the previous one.
    ///
    /// `get_end_operator` is used to extend the operator chain of the old block with the last
    /// operator (e.g. `operator::End`, `operator::GroupByEndOperator`). The end operator must
    /// be an `Operator<()>`.
    ///
    /// The new block is initialized with a `Start`.
    pub(crate) fn split_block<GetEndOp, Op2, IndexFn>(
        self,
        get_end_operator: GetEndOp,
        next_strategy: NextStrategy<I, IndexFn>,
    ) -> Stream<I, impl Operator<I>>
    where
        IndexFn: KeyerFn<u64, I>,
        I: ExchangeData,
        Op2: Operator<()> + 'static,
        GetEndOp: FnOnce(Op, NextStrategy<I, IndexFn>, BatchMode) -> Op2,
    {
        let Stream { block, env } = self;
        // Clone parameters for new block
        let batch_mode = block.batch_mode;
        let iteration_ctx = block.iteration_ctx.clone();
        // Add end operator
        let mut block =
            block.add_operator(|prev| get_end_operator(prev, next_strategy.clone(), batch_mode));
        block.is_only_one_strategy = matches!(next_strategy, NextStrategy::OnlyOne);

        // Close old block
        let mut env_lock = env.lock();
        let prev_id = env_lock.close_block(block);
        // Create new block
        let source = Start::single(prev_id, iteration_ctx.last().cloned());
        let new_block = env_lock.new_block(source, batch_mode, iteration_ctx);
        // Connect blocks
        env_lock.connect_blocks::<I>(prev_id, new_block.id);

        drop(env_lock);
        Stream {
            block: new_block,
            env,
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
    pub(crate) fn binary_connection<I2, Op2, O, S, Fs, Fi, Fj>(
        self,
        oth: Stream<I2, Op2>,
        get_start_operator: Fs,
        next_strategy1: NextStrategy<I, Fi>,
        next_strategy2: NextStrategy<I2, Fj>,
    ) -> Stream<O, S>
    where
        I: ExchangeData,
        I2: ExchangeData,
        Fi: KeyerFn<u64, I>,
        Fj: KeyerFn<u64, I2>,
        Op2: Operator<I2> + 'static,
        O: Data,
        S: Operator<O> + Source<O>,
        Fs: FnOnce(BlockId, BlockId, bool, bool, Option<Arc<IterationStateLock>>) -> S,
    {
        let Stream { block: b1, env } = self;
        let Stream { block: b2, .. } = oth;

        let batch_mode = b1.batch_mode;
        let is_one_1 = matches!(next_strategy1, NextStrategy::OnlyOne);
        let is_one_2 = matches!(next_strategy2, NextStrategy::OnlyOne);
        let sched_1 = b1.scheduler_requirements.clone();
        let sched_2 = b2.scheduler_requirements.clone();
        if is_one_1 && is_one_2 && sched_1.replication != sched_2.replication {
            panic!(
                "The parallelism of the 2 blocks coming inside a Y connection must be equal. \
                On the left ({}) is {:?}, on the right ({}) is {:?}",
                b1, sched_1.replication, b2, sched_2.replication
            );
        }

        let iter_ctx_1 = b1.iteration_ctx();
        let iter_ctx_2 = b2.iteration_ctx();
        let (iteration_ctx, left_cache, right_cache) = if iter_ctx_1 == iter_ctx_2 {
            (b1.iteration_ctx.clone(), false, false)
        } else {
            if !iter_ctx_1.is_empty() && !iter_ctx_2.is_empty() {
                panic!("Side inputs are supported only if one of the streams is coming from outside any iteration");
            }
            if iter_ctx_1.is_empty() {
                // self is the side input, cache it
                (b2.iteration_ctx.clone(), true, false)
            } else {
                // oth is the side input, cache it
                (b1.iteration_ctx.clone(), false, true)
            }
        };

        // close previous blocks

        let mut b1 = b1.add_operator(|prev| End::new(prev, next_strategy1, batch_mode));
        let mut b2 = b2.add_operator(|prev| End::new(prev, next_strategy2, batch_mode));
        b1.is_only_one_strategy = is_one_1;
        b2.is_only_one_strategy = is_one_2;

        let mut env_lock = env.lock();
        let id_1 = b1.id;
        let id_2 = b2.id;

        env_lock.close_block(b1);
        env_lock.close_block(b2);

        let source = get_start_operator(
            id_1,
            id_2,
            left_cache,
            right_cache,
            iteration_ctx.last().cloned(),
        );

        let mut new_block = env_lock.new_block(source, batch_mode, iteration_ctx);
        let id_new = new_block.id;

        env_lock.connect_blocks::<I>(id_1, id_new);
        env_lock.connect_blocks::<I2>(id_2, id_new);

        drop(env_lock);

        // make sure the new block has the same parallelism of the previous one with OnlyOne
        // strategy
        new_block.scheduler_requirements = match (is_one_1, is_one_2) {
            (true, _) => sched_1,
            (_, true) => sched_2,
            _ => SchedulerRequirements::default(),
        };

        Stream {
            block: new_block,
            env,
        }
    }

    /// Clone the given block, taking care of connecting the new block to the same previous blocks
    /// of the original one.
    pub(crate) fn clone(&mut self) -> Self {
        let mut env = self.env.lock();
        let prev_nodes = env.scheduler_mut().prev_blocks(self.block.id).unwrap();
        let new_id = env.new_block_id();

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
        let mut env = self.env.lock();
        env.scheduler_mut().schedule_block(self.block);
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<(Key, Out)> + 'static,
{
    pub(crate) fn add_operator<NewOut: Data, Op, GetOp>(
        self,
        get_operator: GetOp,
    ) -> KeyedStream<Key, NewOut, Op>
    where
        Op: Operator<(Key, NewOut)> + 'static,
        GetOp: FnOnce(OperatorChain) -> Op,
    {
        KeyedStream(self.0.add_operator(get_operator))
    }
}

impl<K: DataKey, V: Data, OperatorChain> Stream<(K, V), OperatorChain>
where
    OperatorChain: Operator<(K, V)>,
{
    /// TODO DOCS
    pub fn to_keyed(self) -> KeyedStream<K, V, OperatorChain> {
        KeyedStream(self)
    }
}
