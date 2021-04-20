use crate::operator::window::generic_operator::GenericWindowOperator;
use crate::operator::{Data, DataKey, Operator, WindowDescription};
use crate::stream::{KeyValue, KeyedStream, WindowedStream};

impl<Key: DataKey, Out: Data, WindowDescr, OperatorChain>
    WindowedStream<Key, Out, OperatorChain, WindowDescr>
where
    WindowDescr: WindowDescription<Key, Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn fold<NewOut: Data, F>(
        self,
        init: NewOut,
        fold: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        F: Fn(NewOut, &Out) -> NewOut + Clone + 'static,
    {
        let stream = self.inner;
        let descr = self.descr;

        stream.add_operator(|prev| {
            GenericWindowOperator::new("Fold", prev, descr, move |window| {
                let mut res = init.clone();
                for value in window.items() {
                    res = (fold)(res, value);
                }
                res
            })
        })
    }
}
