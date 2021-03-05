use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::block::{BatchMode, InnerBlock, NextStrategy};
use crate::environment::StreamEnvironmentInner;
use crate::operator::Operator;
use crate::operator::StartBlock;

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
pub struct Stream<In, Out, OperatorChain>
where
    In: Clone + Serialize + DeserializeOwned + Send + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out>,
{
    /// The last block inside the stream.
    pub(crate) block: InnerBlock<In, Out, OperatorChain>,
    /// A reference to the environment this stream lives in.
    pub(crate) env: Rc<RefCell<StreamEnvironmentInner>>,
}

/// A `KeyedStream` is like a set of `Stream`s, each of which partitioned by some `Key`. Internally
/// it's just a stream whose elements are `KeyValue` pairs and the operators behave following the
/// `KeyedStream` semantics.
///
/// The type of the `Key` must be a valid key inside an hashmap.
pub struct KeyedStream<In, Key, Out, OperatorChain>(
    pub Stream<In, KeyValue<Key, Out>, OperatorChain>,
)
where
    In: Clone + Serialize + DeserializeOwned + Send + 'static,
    Key: Clone + Serialize + DeserializeOwned + Send + Hash + Eq + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>>;

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    In: Clone + Serialize + DeserializeOwned + Send + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
    /// Add a new operator to the current chain inside the stream. This consumes the stream and
    /// returns a new one with the operator added.
    ///
    /// `get_operator` is a function that is given the previous chain of operators and should return
    /// the new chain of operators. The new chain cannot be simply passed as argument since it is
    /// required to do a partial move of the `InnerBlock` structure.
    pub(crate) fn add_operator<NewOut, Op, GetOp>(
        self,
        get_operator: GetOp,
    ) -> Stream<In, NewOut, Op>
    where
        NewOut: Clone + Serialize + DeserializeOwned + Send + 'static,
        Op: Operator<NewOut> + 'static,
        GetOp: FnOnce(OperatorChain) -> Op,
    {
        Stream {
            block: InnerBlock {
                id: self.block.id,
                operators: get_operator(self.block.operators),
                next_strategy: self.block.next_strategy.into(),
                batch_mode: self.block.batch_mode,
                scheduler_requirements: self.block.scheduler_requirements,
                _in_type: Default::default(),
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
    ) -> Stream<Out, Out, StartBlock<Out>>
    where
        Op: Operator<()> + Send + 'static,
        GetEndOp: FnOnce(OperatorChain, NextStrategy<Out>, BatchMode) -> Op,
    {
        let next_strategy = self.block.next_strategy.clone();
        let batch_mode = self.block.batch_mode;
        let old_stream =
            self.add_operator(|prev| get_end_operator(prev, next_strategy, batch_mode));
        let mut env = old_stream.env.borrow_mut();
        let old_id = old_stream.block.id;
        let new_id = env.new_block();
        let scheduler = env.scheduler_mut();
        scheduler.add_block(old_stream.block);
        scheduler.connect_blocks(old_id, new_id);
        drop(env);
        Stream {
            block: InnerBlock::new(new_id, StartBlock::default()),
            env: old_stream.env,
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

impl<In, Key, Out, OperatorChain> KeyedStream<In, Key, Out, OperatorChain>
where
    In: Clone + Serialize + DeserializeOwned + Send + 'static,
    Key: Clone + Serialize + DeserializeOwned + Send + Hash + Eq + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub(crate) fn add_operator<NewOut, Op, GetOp>(
        self,
        get_operator: GetOp,
    ) -> KeyedStream<In, Key, NewOut, Op>
    where
        NewOut: Clone + Serialize + DeserializeOwned + Send + 'static,
        Op: Operator<KeyValue<Key, NewOut>> + 'static,
        GetOp: FnOnce(OperatorChain) -> Op,
    {
        KeyedStream(self.0.add_operator(get_operator))
    }
}
