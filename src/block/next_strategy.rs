use std::hash::Hash;
use std::marker::PhantomData;

use nanorand::{tls_rng, Rng};

use crate::operator::{ExchangeData, KeyerFn};

use super::group_by_hash;

/// The next strategy used at the end of a block.
///
/// A block in the job graph may have many next blocks. Each of them will receive the message, which
/// of their replica will receive it depends on the value of the next strategy.
pub(crate) enum NextStrategy<Out, IndexFn = fn(&Out) -> u64>
where
    IndexFn: KeyerFn<u64, Out>,
{
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
    GroupBy(IndexFn, PhantomData<Out>),
    /// Every following replica will receive every message.
    All,
}

impl<Out, IndexFn> std::fmt::Debug for NextStrategy<Out, IndexFn>
where
    IndexFn: KeyerFn<u64, Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OnlyOne => write!(f, "OnlyOne"),
            Self::Random => write!(f, "Random"),
            Self::GroupBy(_, _) => write!(f, "GroupBy"),
            Self::All => write!(f, "All"),
        }
    }
}

impl<Out, IndexFn: Clone> Clone for NextStrategy<Out, IndexFn>
where
    IndexFn: KeyerFn<u64, Out>,
{
    fn clone(&self) -> Self {
        match self {
            Self::OnlyOne => Self::OnlyOne,
            Self::Random => Self::Random,
            Self::GroupBy(idx, _) => Self::GroupBy(idx.clone(), PhantomData),
            Self::All => Self::All,
        }
    }
}

impl<Out: ExchangeData> NextStrategy<Out> {
    /// Build a `NextStrategy` from a keyer function.
    pub(crate) fn group_by<Key: Hash, Keyer>(
        keyer: Keyer,
    ) -> NextStrategy<Out, impl KeyerFn<u64, Out>>
    where
        Keyer: KeyerFn<Key, Out>,
    {
        NextStrategy::GroupBy(
            move |item: &Out| group_by_hash(&keyer(item)),
            Default::default(),
        )
    }

    /// Returns `NextStrategy::All` with default `IndexFn`.
    pub(crate) fn all() -> NextStrategy<Out> {
        NextStrategy::All
    }

    /// Returns `NextStrategy::OnlyOne` with default `IndexFn`.
    pub(crate) fn only_one() -> NextStrategy<Out> {
        NextStrategy::OnlyOne
    }

    /// Returns `NextStrategy::Random` with default `IndexFn`.
    pub(crate) fn random() -> NextStrategy<Out> {
        NextStrategy::Random
    }
}

impl<Out: ExchangeData, IndexFn> NextStrategy<Out, IndexFn>
where
    IndexFn: KeyerFn<u64, Out>,
{
    /// Compute the index of the replica which this message should be forwarded to.
    pub fn index(&self, message: &Out) -> usize {
        match self {
            NextStrategy::OnlyOne | NextStrategy::All => 0,
            NextStrategy::Random => tls_rng().generate(),
            NextStrategy::GroupBy(keyer, _) => keyer(message) as usize,
        }
    }
}
