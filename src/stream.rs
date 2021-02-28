use std::cell::RefCell;
use std::rc::Rc;

use crate::block::{ExecutionMetadataRef, InnerBlock};
use crate::environment::StreamEnvironmentInner;
use crate::operator::source::StartBlock;
use crate::operator::Operator;
use crate::worker::spawn_worker;

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
            info!("Adding new block, id={}", new_id);
            // connect the last block to the new one
            env.next_blocks
                .entry(self.block_id)
                .or_default()
                .push(new_id);
            info!("Connecting blocks: {} -> {}", self.block_id, new_id);
            // spawn the worker of the block
            let start_handle = spawn_worker(self.block);
            env.start_handles.insert(self.block_id, start_handle);
            new_id
        };
        let metadata = ExecutionMetadataRef::default();
        Stream {
            block_id: new_id,
            block: InnerBlock::new(new_id, StartBlock::new(metadata.clone()), metadata),
            env: self.env,
        }
    }

    pub fn finalize_block(self) {
        let mut env = self.env.borrow_mut();
        info!("Finalizing block id={}", self.block_id);
        // spawn the worker of the block
        let start_handle = spawn_worker(self.block);
        env.start_handles.insert(self.block_id, start_handle);
    }
}
