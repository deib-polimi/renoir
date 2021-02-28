use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use async_std::channel::Sender;
use async_std::task::JoinHandle;

use crate::block::InnerBlock;
use crate::operator::source::Source;
use crate::scheduler;
use crate::stream::{BlockId, Stream};
use crate::worker::ExecutionMetadata;
use std::ops::DerefMut;

pub struct StartHandle {
    pub starter: Sender<ExecutionMetadata>,
    pub join_handle: JoinHandle<()>,
}

pub struct StreamEnvironmentInner {
    pub block_count: BlockId,
    pub next_blocks: HashMap<BlockId, Vec<BlockId>>,
    pub start_handles: HashMap<BlockId, StartHandle>,
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
                start_handles: Default::default(),
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
        let join = scheduler::start(self.inner.borrow_mut().deref_mut()).await;
        // wait till the computation ends
        for join_handle in join {
            join_handle.await;
        }
    }
}
