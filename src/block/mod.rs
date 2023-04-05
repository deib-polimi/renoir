use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;

pub use batcher::BatchMode;
pub(crate) use batcher::*;
pub(crate) use graph_generator::*;
pub(crate) use next_strategy::*;
pub(crate) use structure::*;

use crate::operator::iteration::IterationStateLock;
use crate::operator::{Data, Operator};
use crate::scheduler::BlockId;
use crate::CoordUInt;

mod batcher;
mod graph_generator;
mod next_strategy;
pub mod structure;

/// A chain of operators that will be run inside the same host. The block takes as input elements of
/// type `In` and produces elements of type `Out`.
///
/// The type `In` is used to make sure the blocks are connected following the correct type.
///
/// `OperatorChain` is the type of the chain of operators inside the block. It must be an operator
/// that yields values of type `Out`.
#[derive(Debug, Clone)]
pub(crate) struct Block<Out: Data, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    /// The identifier of the block inside the environment.
    pub(crate) id: BlockId,
    /// The current chain of operators.
    pub(crate) operators: OperatorChain,
    /// The batch mode of this block.
    pub(crate) batch_mode: BatchMode,
    /// This block may be inside a number of iteration loops, this stack keeps track of the state
    /// lock for each of them.
    pub(crate) iteration_ctx: Vec<Arc<IterationStateLock>>,
    /// Whether this block has `NextStrategy::OnlyOne`.
    pub(crate) is_only_one_strategy: bool,
    /// The set of requirements that the block imposes on the scheduler.
    pub(crate) scheduler_requirements: SchedulerRequirements,

    pub _out_type: PhantomData<Out>,
}

impl<Out: Data, OperatorChain> Block<Out, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    /// Add an operator to the end of the block
    pub fn add_operator<NewOut: Data, Op, GetOp>(self, get_operator: GetOp) -> Block<NewOut, Op>
    where
        Op: Operator<NewOut> + 'static,
        GetOp: FnOnce(OperatorChain) -> Op,
    {
        Block {
            id: self.id,
            operators: get_operator(self.operators),
            batch_mode: self.batch_mode,
            iteration_ctx: self.iteration_ctx,
            is_only_one_strategy: false,
            scheduler_requirements: self.scheduler_requirements,
            _out_type: Default::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SchedulerRequirements {
    /// If some of the operators inside the chain require a limit on the parallelism of this node,
    /// it is stored here. `None` means that the scheduler is allowed to spawn as many copies of
    /// this block as it likes.
    ///
    /// The value specified is only an upper bound, the scheduler is allowed to spawn less blocks,
    pub(crate) max_parallelism: Option<CoordUInt>,
}

impl<Out: Data, OperatorChain> Block<Out, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    pub fn new(
        id: BlockId,
        operators: OperatorChain,
        batch_mode: BatchMode,
        iteration_ctx: Vec<Arc<IterationStateLock>>,
    ) -> Self {
        Self {
            id,
            operators,
            batch_mode,
            iteration_ctx,
            is_only_one_strategy: false,
            scheduler_requirements: Default::default(),
            _out_type: Default::default(),
        }
    }

    /// Obtain a vector of opaque items representing the stack of iterations.
    ///
    /// An empty vector is returned when the block is outside any iterations, more than one element
    /// if it's inside nested iterations.
    pub(crate) fn iteration_stack(&self) -> Vec<*const ()> {
        self.iteration_ctx
            .iter()
            .map(|s| Arc::as_ptr(s) as *const ())
            .collect()
    }
}

impl<Out: Data, OperatorChain> Display for Block<Out, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.operators)
    }
}

impl SchedulerRequirements {
    /// Limit the maximum parallelism of this block.
    pub(crate) fn max_parallelism(&mut self, max_parallelism: CoordUInt) {
        if let Some(old) = self.max_parallelism {
            self.max_parallelism = Some(old.min(max_parallelism));
        } else {
            self.max_parallelism = Some(max_parallelism);
        }
    }
}

/// Hashing function for group by operations
pub fn group_by_hash<T: Hash>(item: &T) -> u64 {
    let mut hasher = wyhash::WyHash::with_seed(0x0123456789abcdef);
    item.hash(&mut hasher);
    hasher.finish()
}

/// Hasher used for internal hashmaps that have coordinates as keys
/// (optimized for small keys)
pub type CoordHasherBuilder = fxhash::FxBuildHasher;

/// Hasher used for StreamElement keys
/// (for all around good performance)
pub type GroupHasherBuilder = core::hash::BuildHasherDefault<wyhash::WyHash>;
