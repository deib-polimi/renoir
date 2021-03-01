use std::cell::RefCell;
use std::rc::Rc;

use crate::block::InnerBlock;
use crate::environment::StreamEnvironmentInner;
use crate::operator::sink::EndBlock;
use crate::operator::source::StartBlock;
use crate::operator::Operator;

pub type BlockId = usize;

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

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    In: Clone + Send + 'static,
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
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
                execution_metadata: self.block.execution_metadata,
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
        let new_id = env.block_count;
        env.block_count += 1;
        let scheduler = env.scheduler_mut();
        scheduler.add_block(old_stream.block);
        scheduler.connect_blocks(old_stream.block_id, new_id);
        drop(env);
        info!("Creating a new block, id={}", new_id);
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
