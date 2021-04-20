use crate::operator::window::generic_operator::GenericWindowOperator;
use crate::operator::{Data, DataKey, Operator, WindowDescription};
use crate::stream::{KeyValue, KeyedStream, WindowedStream};
use std::iter::Sum;

impl<Key: DataKey, Out: Data, WindowDescr, OperatorChain>
    WindowedStream<Key, Out, OperatorChain, WindowDescr>
where
    WindowDescr: WindowDescription<Key, Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
    for<'a> Out: Sum<&'a Out>,
{
    pub fn sum(self) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>> {
        let stream = self.inner;
        let descr = self.descr;

        stream.add_operator(|prev| {
            GenericWindowOperator::new("Sum", prev, descr, |window| window.items().sum())
        })
    }
}
