use std::collections::HashMap;
use std::marker::PhantomData;
use std::{fmt::Display, sync::Arc};

use parking_lot::Mutex;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure, Replication};
use crate::network::Coord;
use crate::{CoordUInt, RuntimeConfig};

use crate::operator::{source::Source, Data, Operator, StreamElement};

pub mod bincode;
mod stream_cache;
pub mod vec;
pub use bincode::{BincodeCacheConfig, BincodeCacher};
pub use stream_cache::*;
pub use vec::VecCacher;

pub trait Cacher<T>: Send {
    type Config: Send + Sync;
    type Handle: Send;
    type Replayer: CacheReplayer<T, Handle = Self::Handle> + 'static;

    fn init(config: &Self::Config, global_coord: Coord) -> Self;
    fn append(&mut self, item: T);
    fn finalize(self) -> Self::Handle;
}

pub trait CacheReplayer<T>: Send {
    type Handle: Send;

    fn new(handle: Self::Handle) -> Self;
    fn next(&mut self) -> Option<T>;
}

#[derive(Clone)]
pub(crate) struct CacheRegistry<T, H> {
    setup: bool,
    missing: usize,
    data: HashMap<CoordUInt, H>,
    _t: PhantomData<T>,
}

impl<T, H> CacheRegistry<T, H> {
    pub fn new() -> CacheRegistryRef<T, H> {
        Default::default()
    }

    pub fn register(&mut self) {
        self.setup = true;
        self.missing += 1;
        tracing::warn!("registered {}", self.missing);
    }

    pub fn insert(&mut self, global_id: CoordUInt, data: H) -> Option<H> {
        self.missing = self
            .missing
            .checked_sub(1)
            .expect("inserting more cache entries than expected");
        self.data.insert(global_id, data)
    }

    pub fn remove(&mut self, global_id: CoordUInt) -> Option<H> {
        self.data.remove(&global_id)
    }

    pub fn is_complete(&self) -> bool {
        self.setup && self.missing == 0
    }
}

impl<T, H> Default for CacheRegistry<T, H> {
    fn default() -> Self {
        Self {
            setup: false,
            missing: 0,
            data: Default::default(),
            _t: PhantomData,
        }
    }
}

pub(crate) type CacheRegistryRef<T, H> = Arc<Mutex<CacheRegistry<T, H>>>;

// pub(crate) type StreamCacheInner<I> = Arc<Mutex<Option<HashMap<u64, Vec<I>>>>>;

pub struct CacheSource<T, S: CacheReplayer<T>> {
    replication: Replication,
    cache: CacheRegistryRef<T, S::Handle>,
    source: Option<S>,
    flushed: bool,
}

impl<T: Clone, S: CacheReplayer<T>> Clone for CacheSource<T, S> {
    fn clone(&self) -> Self {
        Self {
            replication: self.replication,
            cache: self.cache.clone(),
            source: Default::default(),
            flushed: self.flushed,
        }
    }
}

impl<T, S: CacheReplayer<T>> CacheSource<T, S> {
    pub(crate) fn new(replication: Replication, cache: CacheRegistryRef<T, S::Handle>) -> Self {
        Self {
            replication,
            cache,
            source: Default::default(),
            flushed: false,
        }
    }
}

pub struct CacheSink<T, A: Cacher<T>, Op>
where
    Op: Operator<Out = T> + 'static,
{
    prev: Op,

    rt_config: Arc<RuntimeConfig>,
    global_id: Option<CoordUInt>,

    cacher_config: Arc<A::Config>,
    accumulator: Option<A>,
    cache: CacheRegistryRef<T, A::Handle>,
    terminated: bool,
}

impl<I: Data + Clone, A: Cacher<I>, Op: Clone> Clone for CacheSink<I, A, Op>
where
    Op: Operator<Out = I> + 'static,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            rt_config: self.rt_config.clone(),
            cacher_config: self.cacher_config.clone(),
            global_id: None,
            accumulator: Default::default(),
            cache: self.cache.clone(),
            terminated: false,
        }
    }
}

impl<T: Data, A: Cacher<T>, Op> CacheSink<T, A, Op>
where
    Op: Operator<Out = T> + 'static,
{
    pub(crate) fn new(
        prev: Op,
        config: Arc<RuntimeConfig>,
        cacher_config: A::Config,
        output: CacheRegistryRef<T, A::Handle>,
    ) -> Self {
        Self {
            rt_config: config,
            cacher_config: Arc::new(cacher_config),
            accumulator: None,
            global_id: None,
            prev,
            cache: output,
            terminated: false,
        }
    }
}

impl<T, A: Cacher<T>, Op> Display for CacheSink<T, A, Op>
where
    Op: Operator<Out = T> + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Cache<{},{}>",
            self.prev,
            std::any::type_name::<T>(),
            std::any::type_name::<A>()
        )
    }
}

impl<T: Data, A: Cacher<T> + Send, Op> Operator for CacheSink<T, A, Op>
where
    Op: Operator<Out = T> + 'static,
{
    type Out = ();

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.prev.setup(metadata);
        self.global_id = Some(metadata.global_id);
        self.accumulator = Some(A::init(self.cacher_config.as_ref(), metadata.coord));
        self.cache.lock().register();
    }

    fn next(&mut self) -> super::StreamElement<()> {
        match self.prev.next() {
            StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                self.accumulator.as_mut().unwrap().append(t);
                StreamElement::Item(())
            }
            StreamElement::Terminate => {
                if self.terminated {
                    return StreamElement::Terminate;
                }
                let mut output = self.cache.lock();

                let acc = self.accumulator.take().unwrap();
                let ret = output.insert(self.global_id.unwrap(), acc.finalize());
                if ret.is_some() {
                    panic!("duplicated insert in cache");
                }

                self.terminated = true;
                StreamElement::Terminate
            }
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::FlushBatch => StreamElement::FlushBatch,
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
        }
    }

    fn structure(&self) -> crate::block::BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<T, _>("Cache"))
    }
}

impl<T, S: CacheReplayer<T>> Display for CacheSource<T, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CacheSource<{}, {}>",
            std::any::type_name::<T>(),
            std::any::type_name::<S>()
        )
    }
}

impl<T: Data, S: CacheReplayer<T> + Send> Operator for CacheSource<T, S> {
    type Out = T;

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        let global_id = metadata.global_id;
        let mut cache = self.cache.lock();

        assert!(cache.is_complete(), "Reading cache before it was complete. execution from a cached stream must start after the previous StreamContext has completed!");
        match cache.remove(global_id) {
            Some(handle) => self.source = Some(S::new(handle)),
            None => {
                panic!("configuration mismatch for cache, no data for id {global_id}");
            }
        }
    }

    fn next(&mut self) -> StreamElement<T> {
        match self.source.as_mut().unwrap().next() {
            Some(item) => StreamElement::Item(item),
            None if !self.flushed => {
                self.flushed = true;
                StreamElement::FlushAndRestart
            }
            None => StreamElement::Terminate,
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<T, _>("CacheSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl<T: Clone + Send + 'static, S: CacheReplayer<T>> Source for CacheSource<T, S> {
    fn replication(&self) -> Replication {
        self.replication
    }
}
