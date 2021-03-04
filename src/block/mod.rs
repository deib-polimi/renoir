use std::marker::PhantomData;

use crate::operator::Operator;
use crate::stream::BlockId;

/// The next strategy used at the end of a block.
///
/// A block in the job graph may have many next blocks. Each of them will receive the message, which
/// of their replica will receive it depends on the value of the next strategy.
#[derive(Debug, Clone, Copy)]
pub(crate) enum NextStrategy {
    /// Only one of the replicas will receive the message:
    ///
    /// - if the block is not replicated, the only replica will receive the message
    /// - if the next block is replicated as much as the current block the corresponding replica
    ///   will receive the message
    /// - otherwise the execution graph is malformed  
    OnlyOne,
    /// A random replica will receive the message.
    Random,
    /// Among the next replica, the one is selected based on the hash of the key of the message.
    GroupBy,
}

/// A chain of operators that will be run inside the same host. The block takes as input elements of
/// type `In` and produces elements of type `Out`.
///
/// The type `In` is used to make sure the blocks are connected following the correct type.
///
/// `OperatorChain` is the type of the chain of operators inside the block. It must be an operator
/// that yields values of type `Out`.
#[derive(Clone)]
pub(crate) struct InnerBlock<In, Out, OperatorChain>
where
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out>,
{
    /// The identifier of the block inside the environment.
    pub(crate) id: BlockId,
    /// The current chain of operators.
    pub(crate) operators: OperatorChain,
    /// The strategy to use for sending to the next blocks in the stream.
    pub(crate) next_strategy: NextStrategy,
    /// If some of the operators inside the chain require a limit on the parallelism of this node,
    /// it is stored here. `None` means that the scheduler is allowed to spawn as many copies of
    /// this block as it likes.
    ///
    /// The value specified is only an upper bound, the scheduler is allowed to spawn less blocks,
    pub(crate) max_parallelism: Option<usize>,

    pub _in_type: PhantomData<In>,
    pub _out_type: PhantomData<Out>,
}

impl<In, Out, OperatorChain> InnerBlock<In, Out, OperatorChain>
where
    Out: Clone + Send + 'static,
    OperatorChain: Operator<Out>,
{
    pub fn new(id: BlockId, operators: OperatorChain) -> Self {
        Self {
            id,
            operators,
            next_strategy: NextStrategy::OnlyOne,
            max_parallelism: None,
            _in_type: Default::default(),
            _out_type: Default::default(),
        }
    }

    /// Limit the maximum parallelism of this block.
    pub fn max_parallelism(&mut self, max_parallelism: usize) {
        if let Some(old) = self.max_parallelism {
            self.max_parallelism = Some(old.min(max_parallelism));
        } else {
            self.max_parallelism = Some(max_parallelism);
        }
    }

    pub fn to_string(&self) -> String {
        self.operators.to_string()
    }
}
