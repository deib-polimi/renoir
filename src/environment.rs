use std::cell::RefCell;
use std::rc::Rc;

use crate::block::InnerBlock;
use crate::operator::source::Source;
use crate::stream::{BlockId, Stream};
use async_std::channel::Sender;
use async_std::task::JoinHandle;
use std::collections::HashMap;

pub struct StartHandle {
    pub starter: Sender<()>,
    pub join_handle: JoinHandle<()>,
}

pub struct StreamEnvironmentInner {
    pub block_count: BlockId,
    pub next_blocks: HashMap<BlockId, Vec<BlockId>>,
    pub start_handles: Vec<StartHandle>,
}

pub struct StreamEnvironment {
    inner: Rc<RefCell<StreamEnvironmentInner>>,
}

impl StreamEnvironment {
    pub fn new() -> Self {
        StreamEnvironment {
            inner: Rc::new(RefCell::new(StreamEnvironmentInner {
                block_count: 0,
                next_blocks: Default::default(),
                start_handles: Vec::new(),
            })),
        }
    }

    pub fn stream<Out, S>(&mut self, source: S) -> Stream<Out, Out, S>
    where
        S: Source<Out> + 'static,
    {
        let block_id = self.inner.borrow().block_count;
        self.inner.borrow_mut().block_count += 1;
        Stream {
            block_id,
            block: InnerBlock::new(source),
            env: self.inner.clone(),
        }
    }

    pub async fn execute(self) {
        let mut inner = self.inner.borrow_mut();
        let mut join = Vec::new();
        for handle in inner.start_handles.drain(..) {
            handle.starter.send(()).await.unwrap();
            join.push(handle.join_handle);
        }
        for join_handle in join {
            join_handle.await;
        }
    }
}
