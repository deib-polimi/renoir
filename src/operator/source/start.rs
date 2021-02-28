use std::marker::PhantomData;

use async_trait::async_trait;

use crate::block::ExecutionMetadataRef;
use crate::operator::{Operator, StreamElement};

pub struct StartBlock<Out> {
    metadata: Option<ExecutionMetadataRef>,
    _out_type: PhantomData<Out>,
}

impl<Out> StartBlock<Out> {
    pub fn new() -> Self {
        StartBlock {
            metadata: None,
            _out_type: Default::default(),
        }
    }
}

#[async_trait]
impl<Out> Operator<Out> for StartBlock<Out>
where
    Out: Send,
{
    fn init(&mut self, metadata: ExecutionMetadataRef) {
        self.metadata = Some(metadata.clone());
    }

    async fn next(&mut self) -> StreamElement<Out> {
        unimplemented!()
    }

    fn to_string(&self) -> String {
        format!("[{}]", std::any::type_name::<Out>())
    }
}

impl<Out> Clone for StartBlock<Out>
where
    Out: Send,
{
    fn clone(&self) -> Self {
        if self.metadata.is_some() {
            panic!("Cannot clone once initialized");
        }
        Self {
            metadata: None,
            _out_type: Default::default(),
        }
    }
}
