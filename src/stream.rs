use std::cell::RefCell;
use std::rc::Rc;

use crate::block::InnerBlock;
use crate::environment::StreamEnvironmentInner;
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
