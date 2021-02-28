use std::marker::PhantomData;

use async_trait::async_trait;

use crate::operator::{Operator, StreamElement};
use crate::stream::Stream;

pub struct Map<Out, NewOut, PreviousOperators, F>
where
    PreviousOperators: Operator<Out>,
    F: Fn(Out) -> NewOut,
{
    prev: PreviousOperators,
    f: F,
    _out_type: PhantomData<Out>,
    _new_out_type: PhantomData<NewOut>,
}

#[async_trait]
impl<Out, NewOut, PreviousOperators, F> Operator<NewOut> for Map<Out, NewOut, PreviousOperators, F>
where
    Out: Send,
    NewOut: Send,
    PreviousOperators: Operator<Out> + Send,
    F: Fn(Out) -> NewOut + Send,
{
    async fn next(&mut self) -> StreamElement<NewOut> {
        match self.prev.next().await {
            StreamElement::Item(t) => StreamElement::Item((self.f)(t)),
            StreamElement::Timestamped(t, ts) => StreamElement::Timestamped((self.f)(t), ts),
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::End => StreamElement::End,
        }
    }
}

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn map<NewOut, F>(self, f: F) -> Stream<In, NewOut, Map<Out, NewOut, OperatorChain, F>>
    where
        In: Send + 'static,
        Out: Send + 'static,
        NewOut: Send + 'static,
        F: Fn(Out) -> NewOut + Send + 'static,
    {
        self.add_operator(|prev| Map {
            prev,
            f,
            _out_type: Default::default(),
            _new_out_type: Default::default(),
        })
    }
}
