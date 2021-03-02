use std::cell::RefCell;
use std::rc::Rc;

use crate::block::InnerBlock;
use crate::environment::StreamEnvironmentInner;
use crate::operator::EndBlock;
use crate::operator::Operator;
use crate::operator::StartBlock;
use std::hash::Hash;

pub type BlockId = usize;

pub type KeyValue<Key, Value> = (Key, Value);

pub struct Stream<In, Out, OperatorChain>
where
    In: Clone + Send + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out>,
{
    pub block_id: BlockId,
    pub block: InnerBlock<In, Out, OperatorChain>,
    pub env: Rc<RefCell<StreamEnvironmentInner>>,
}

pub struct KeyedStream<In, Key, Out, OperatorChain>(
    pub Stream<In, KeyValue<Key, Out>, OperatorChain>,
)
where
    In: Clone + Send + 'static,
    Key: Clone + Send + Hash + Eq + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>>;

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    In: Clone + Send + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn max_parallelism(mut self, max_parallelism: usize) -> Self {
        self.block.max_parallelism = Some(max_parallelism);
        self
    }

    pub fn add_operator<NewOut, Op, GetOp>(self, get_operator: GetOp) -> Stream<In, NewOut, Op>
    where
        NewOut: Clone + Send + 'static,
        Op: Operator<NewOut> + 'static,
        GetOp: FnOnce(OperatorChain) -> Op,
    {
        Stream {
            block_id: self.block_id,
            block: InnerBlock {
                id: self.block_id,
                operators: get_operator(self.block.operators),
                next_strategy: self.block.next_strategy,
                max_parallelism: self.block.max_parallelism,
                _in_type: Default::default(),
                _out_type: Default::default(),
            },
            env: self.env,
        }
    }

    pub fn add_block(self) -> Stream<Out, Out, StartBlock<Out>> {
        let next_strategy = self.block.next_strategy;
        let old_stream = self.add_operator(|prev| EndBlock::new(prev, next_strategy));
        let mut env = old_stream.env.borrow_mut();
        let new_id = env.new_block();
        let scheduler = env.scheduler_mut();
        scheduler.add_block(old_stream.block);
        scheduler.connect_blocks(old_stream.block_id, new_id);
        drop(env);
        Stream {
            block_id: new_id,
            block: InnerBlock::new(new_id, StartBlock::new()),
            env: old_stream.env,
        }
    }

    pub fn finalize_block(self) {
        let mut env = self.env.borrow_mut();
        info!("Finalizing block id={}", self.block_id);
        env.scheduler_mut().add_block(self.block);
    }
}

impl<In, Key, Out, OperatorChain> KeyedStream<In, Key, Out, OperatorChain>
where
    In: Clone + Send + 'static,
    Key: Clone + Send + Hash + Eq + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn add_operator<NewOut, Op, GetOp>(
        self,
        get_operator: GetOp,
    ) -> KeyedStream<In, Key, NewOut, Op>
    where
        NewOut: Clone + Send + 'static,
        Op: Operator<KeyValue<Key, NewOut>> + 'static,
        GetOp: FnOnce(OperatorChain) -> Op,
    {
        KeyedStream(self.0.add_operator(get_operator))
    }
}
