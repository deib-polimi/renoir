use std::marker::PhantomData;

use async_trait::async_trait;

use crate::block::ExecutionMetadataRef;
use crate::operator::{Operator, StreamElement};

pub struct StartBlock<Out> {
    metadata: ExecutionMetadataRef,
    _out_type: PhantomData<Out>,
}

impl<Out> StartBlock<Out> {
    pub fn new(metadata: ExecutionMetadataRef) -> Self {
        StartBlock {
            metadata,
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

    fn to_string(&self) -> String {
        format!("[{}]", std::any::type_name::<Out>())
    }
}
