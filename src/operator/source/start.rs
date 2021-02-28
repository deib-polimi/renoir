use async_trait::async_trait;

use crate::operator::{Operator, StreamElement};
use std::marker::PhantomData;

pub struct StartBlock<Out> {
    _out_type: PhantomData<Out>,
}

impl<Out> Default for StartBlock<Out> {
    fn default() -> Self {
        StartBlock {
            _out_type: Default::default(),
        }
    }
}

#[async_trait]
impl<Out> Operator<Out> for StartBlock<Out>
where
    Out: Send,
{
    async fn next(&mut self) -> StreamElement<Out> {
        unimplemented!()
    }
}
