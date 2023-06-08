use std::any::TypeId;
use std::collections::VecDeque;
use std::fmt::Display;
use std::time::Duration;

use crate::block::{BlockStructure, OperatorStructure, Replication};
use crate::network::{Coord, NetworkSender, NetworkTopology, ReceiverEndpoint};
use crate::operator::source::Source;
use crate::operator::{Data, ExchangeData, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::CoordUInt;
use crate::{BatchMode, EnvironmentConfig};

/// A fake operator that can be used to unit-test the operators.
#[derive(Debug, Clone)]
pub struct FakeOperator<Out: Data> {
    /// The data to return from `next()`.
    buffer: VecDeque<StreamElement<Out>>,
}

impl<Out: Data> Display for FakeOperator<Out> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FakeOperator<{}>", std::any::type_name::<Out>())
    }
}

impl<Out: Data> FakeOperator<Out> {
    /// Create an empty `FakeOperator`.
    pub fn empty() -> Self {
        Self {
            buffer: Default::default(),
        }
    }

    /// Create a `FakeOperator` with the specified data.
    pub fn new<I: Iterator<Item = Out>>(data: I) -> Self {
        Self {
            buffer: data.map(StreamElement::Item).collect(),
        }
    }

    /// Add an element to the end of the list of elements to return from `next`.
    pub fn push(&mut self, item: StreamElement<Out>) {
        self.buffer.push_back(item);
    }
}

impl<Out: Data> Operator<Out> for FakeOperator<Out> {
    fn setup(&mut self, _metadata: &mut ExecutionMetadata) {}

    fn next(&mut self) -> StreamElement<Out> {
        if let Some(item) = self.buffer.pop_front() {
            item
        } else {
            StreamElement::Terminate
        }
    }

    fn structure(&self) -> BlockStructure {
        BlockStructure::default().add_operator(OperatorStructure::new::<Out, _>("FakeOperator"))
    }
}

impl<Out: Data> Source<Out> for FakeOperator<Out> {
    fn replication(&self) -> Replication {
        Replication::Unlimited
    }
}

pub(crate) struct FakeNetworkTopology<T: ExchangeData> {
    topology: NetworkTopology,
    senders: Vec<Vec<(Coord, NetworkSender<T>)>>,
    prev: Vec<(Coord, TypeId)>,
}

impl<T: ExchangeData> FakeNetworkTopology<T> {
    /// Build a fake network topology for a single replica (with coord b0 h0 r0), that receives data
    /// of type `T` from `num_prev_blocks`, each with `instances_per_block` replicas.
    pub fn new(num_prev_blocks: CoordUInt, instances_per_block: CoordUInt) -> Self {
        let config = EnvironmentConfig::local(1);
        let mut topology = NetworkTopology::new(config);

        let dest = Coord::new(0, 0, 0);
        let typ = TypeId::of::<i32>();

        let mut senders = vec![];
        let mut prev = vec![];
        for block_id in 1..num_prev_blocks + 1 {
            let mut block_senders = vec![];
            for replica_id in 0..instances_per_block {
                let coord = Coord::new(block_id, 0, replica_id);
                topology.connect(coord, dest, typ, false);
                let sender = topology.get_sender(ReceiverEndpoint::new(dest, block_id));
                block_senders.push((coord, sender));
                prev.push((coord, typ));
            }
            senders.push(block_senders);
        }

        Self {
            topology,
            senders,
            prev,
        }
    }

    pub fn metadata(&mut self) -> ExecutionMetadata<'_> {
        let dest = Coord::new(0, 0, 0);
        ExecutionMetadata {
            coord: dest,
            replicas: vec![dest],
            global_id: 0,
            prev: self.prev.clone(),
            network: &mut self.topology,
            batch_mode: BatchMode::adaptive(100, Duration::from_millis(100)),
        }
    }

    /// Get a mutable reference to the fake network topology's senders.
    #[must_use]
    pub(crate) fn senders_mut(&mut self) -> &mut Vec<Vec<(Coord, NetworkSender<T>)>> {
        &mut self.senders
    }
}
