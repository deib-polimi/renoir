use crate::operator::window::generic_operator::GenericWindowOperator;
use crate::operator::{Data, DataKey, Operator, WindowDescription};
use crate::stream::{KeyValue, KeyedStream, KeyedWindowedStream, Stream, WindowedStream};

impl<Key: DataKey, Out: Data, WindowDescr, OperatorChain>
    KeyedWindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
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
        F: Fn(&mut NewOut, &Out) + Clone + Send + 'static,
    {
        let stream = self.inner;
        let descr = self.descr;

        stream.add_operator(|prev| {
            GenericWindowOperator::new("Fold", prev, descr, move |window| {
                let mut res = init.clone();
                for value in window.items() {
                    (fold)(&mut res, value);
                }
                res
            })
        })
    }
}

impl<Out: Data, WindowDescr, OperatorChain> WindowedStream<Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<(), Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<(), Out>> + Send + 'static,
{
    pub fn fold<NewOut: Data, F>(
        self,
        init: NewOut,
        fold: F,
    ) -> Stream<NewOut, impl Operator<NewOut>>
    where
        F: Fn(&mut NewOut, &Out) + Clone + Send + 'static,
    {
        self.inner.fold(init, fold).unkey().map(|(_, x)| x)
    }
}
