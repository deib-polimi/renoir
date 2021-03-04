use std::fmt::{Display, Formatter};
use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use serde::Serialize;

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
#[derive(Debug, Clone)]
pub(crate) struct InnerBlock<In, Out, OperatorChain>
where
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out>,
{
    /// The identifier of the block inside the environment.
    pub(crate) id: BlockId,
    /// The current chain of operators.
    pub(crate) operators: OperatorChain,
    /// The strategy to use for sending to the next blocks in the stream.
    pub(crate) next_strategy: NextStrategy,
    /// The set of requirements that the block imposes on the scheduler.
    pub(crate) scheduler_requirements: SchedulerRequirements,

    pub _in_type: PhantomData<In>,
    pub _out_type: PhantomData<Out>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SchedulerRequirements {
    /// If some of the operators inside the chain require a limit on the parallelism of this node,
    /// it is stored here. `None` means that the scheduler is allowed to spawn as many copies of
    /// this block as it likes.
    ///
    /// The value specified is only an upper bound, the scheduler is allowed to spawn less blocks,
    pub(crate) max_parallelism: Option<usize>,
    /// Like `max_parallelism`, but counting only the replicas inside each host,
    pub(crate) max_local_parallelism: Option<usize>,
}

impl<In, Out, OperatorChain> InnerBlock<In, Out, OperatorChain>
where
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out>,
{
    pub fn new(id: BlockId, operators: OperatorChain) -> Self {
        Self {
            id,
            operators,
            next_strategy: NextStrategy::OnlyOne,
            scheduler_requirements: Default::default(),
            _in_type: Default::default(),
            _out_type: Default::default(),
        }
    }
}

impl<In, Out, OperatorChain> Display for InnerBlock<In, Out, OperatorChain>
where
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.operators.to_string())
    }
}

impl SchedulerRequirements {
    /// Limit the maximum parallelism of this block.
    pub(crate) fn max_parallelism(&mut self, max_parallelism: usize) {
        if let Some(old) = self.max_parallelism {
            self.max_parallelism = Some(old.min(max_parallelism));
        } else {
            self.max_parallelism = Some(max_parallelism);
        }
    }

    /// Limit the maximum parallelism of this block inside each host.
    pub(crate) fn max_local_parallelism(&mut self, max_local_parallelism: usize) {
        if let Some(old) = self.max_parallelism {
            self.max_local_parallelism = Some(old.max(max_local_parallelism));
        } else {
            self.max_local_parallelism = Some(max_local_parallelism);
        }
    }
}
