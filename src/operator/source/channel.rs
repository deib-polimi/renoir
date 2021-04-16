use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::channel::BoundedChannelReceiver;
use crate::operator::source::Source;
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

pub struct ChannelSource<Out: Data> {
    receiver: BoundedChannelReceiver<StreamElement<Out>>,
}

impl<Out: Data> ChannelSource<Out> {
    pub fn new(receiver: BoundedChannelReceiver<StreamElement<Out>>) -> Self {
        Self { receiver }
    }
}

impl<Out: Data> Source<Out> for ChannelSource<Out> {
    fn get_max_parallelism(&self) -> Option<usize> {
        Some(1)
    }
}

impl<Out: Data> Operator<Out> for ChannelSource<Out> {
    fn setup(&mut self, _metadata: ExecutionMetadata) {}

    fn next(&mut self) -> StreamElement<Out> {
        if let Ok(elem) = self.receiver.recv() {
            elem
        } else {
            StreamElement::End
        }
    }

    fn to_string(&self) -> String {
        format!("ChannelSource<{}>", std::any::type_name::<Out>())
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("ChannelSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl<Out: Data> Clone for ChannelSource<Out> {
    fn clone(&self) -> Self {
        // Since this is a non-parallel source, we don't want the other replicas to emit any value
        panic!("ChannelSource cannot be cloned, max_parallelism should be 1");
    }
}
