use std::cell::RefCell;
use std::rc::Rc;

use crate::block::InnerBlock;
use crate::environment::StreamEnvironmentInner;
use crate::operator::source::StartBlock;
use crate::operator::Operator;

pub type BlockId = usize;

pub struct Stream<In, Out, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    pub block_id: BlockId,
    pub block: InnerBlock<In, Out, OperatorChain>,
    pub env: Rc<RefCell<StreamEnvironmentInner>>,
}

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    In: Send + 'static,
    Out: Send + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn add_operator<NewOut, Op, GetOp>(self, get_operator: GetOp) -> Stream<In, NewOut, Op>
    where
        Op: Operator<NewOut> + 'static,
        GetOp: FnOnce(OperatorChain) -> Op,
    {
        Stream {
            block_id: self.block_id,
            block: InnerBlock {
                id: self.block_id,
                operators: get_operator(self.block.operators),
                next_strategy: self.block.next_strategy,
                execution_metadata: self.block.execution_metadata,
                _in_type: Default::default(),
                _out_type: Default::default(),
            },
            env: self.env,
        }
    }

    pub fn add_block(self) -> Stream<Out, Out, StartBlock<Out>> {
        let new_id = {
            let mut env = self.env.borrow_mut();
            let new_id = env.block_count;
            env.block_count += 1;
            env.scheduler.add_block(self.block);
            env.scheduler.connect_blocks(self.block_id, new_id);
            info!("Creating a new block, id={}", new_id);
            new_id
        };
        Stream {
            block_id: new_id,
            block: InnerBlock::new(new_id, StartBlock::new()),
            env: self.env,
        }
    }

    pub fn finalize_block(self) {
        let mut env = self.env.borrow_mut();
        info!("Finalizing block id={}", self.block_id);
        env.scheduler.add_block(self.block);
    }
}
