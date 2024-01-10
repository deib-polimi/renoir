use std::collections::HashMap;
use std::{fmt::Display, sync::Arc};

use parking_lot::Mutex;

use crate::{
    block::{BlockStructure, OperatorKind, OperatorStructure},
    Replication, Stream,
};
use crate::{CoordUInt, RuntimeConfig, StreamContext};

use super::{source::Source, Data, Operator, StreamElement};

#[derive(Clone)]
pub(crate) struct CacheInner<I> {
    setup: bool,
    missing: usize,
    data: HashMap<CoordUInt, Vec<I>>,
}

impl<I> CacheInner<I> {
    pub fn register(&mut self) {
        self.setup = true;
        self.missing += 1;
    }

    pub fn insert(&mut self, global_id: CoordUInt, data: Vec<I>) -> Option<Vec<I>> {
        self.missing = self
            .missing
            .checked_sub(1)
            .expect("inserting more cache entries than expected");
        self.data.insert(global_id, data)
    }

    pub fn remove(&mut self, global_id: CoordUInt) -> Option<Vec<I>> {
        self.data.remove(&global_id)
    }

    pub fn is_complete(&self) -> bool {
        self.setup && self.missing == 0
    }
}

impl<I> Default for CacheInner<I> {
    fn default() -> Self {
        Self {
            setup: false,
            missing: 0,
            data: Default::default(),
        }
    }
}

pub(crate) type CacheInnerRef<I> = Arc<Mutex<CacheInner<I>>>;

// pub(crate) type StreamCacheInner<I> = Arc<Mutex<Option<HashMap<u64, Vec<I>>>>>;

pub struct StreamCache<I> {
    replication: Replication,
    config: Arc<RuntimeConfig>,
    data: CacheInnerRef<I>,
}

#[derive(Clone)]
pub struct CacheSource<I> {
    replication: Replication,
    cache: CacheInnerRef<I>,
    items: std::vec::IntoIter<I>,
    flushed: bool,
}

impl<I> CacheSource<I> {
    pub(crate) fn new(replication: Replication, cache: CacheInnerRef<I>) -> Self {
        Self {
            replication,
            cache,
            items: Default::default(),
            flushed: false,
        }
    }
}

pub struct CacheSink<I: Data, PreviousOperator>
where
    PreviousOperator: Operator<Out = I> + 'static,
{
    prev: PreviousOperator,

    config: Arc<RuntimeConfig>,
    global_id: Option<CoordUInt>,

    buffer: Vec<I>,
    cache: CacheInnerRef<I>,
    terminated: bool,
}

impl<I: Data + Clone, PreviousOperator: Clone> Clone for CacheSink<I, PreviousOperator>
where
    PreviousOperator: Operator<Out = I> + 'static,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            config: self.config.clone(),
            global_id: None,
            buffer: Default::default(),
            cache: self.cache.clone(),
            terminated: false,
        }
    }
}

impl<I: Data + Send> StreamCache<I> {
    pub(crate) fn new(
        config: Arc<RuntimeConfig>,
        replication: Replication,
        cache: CacheInnerRef<I>,
    ) -> Self {
        Self {
            replication,
            config,
            data: cache,
        }
    }

    pub fn config(&self) -> Arc<RuntimeConfig> {
        self.config.clone()
    }

    pub fn inner_cloned(&self) -> HashMap<CoordUInt, Vec<I>> {
        let cache = self.data.lock();
        assert!(cache.is_complete(), "Reading cache before it was complete. execution from a cached stream must start after the previous StreamContext has completed!");
        cache.data.clone()
    }

    // // for debuggin purposes
    // pub fn read(&self) -> Vec<I> {
    //     let cache_lock = self.cache_reference.lock();
    //     assert!(
    //         cache_lock.is_some(),
    //         "Can't read the cache before the execution"
    //     );
    //     cache_lock
    //         .clone()
    //         .unwrap()
    //         .into_iter()
    //         .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
    //         .flat_map(|(_, values)| values.into_iter())
    //         .fold(Vec::new(), |mut acc, values| {
    //             acc.push(values);
    //             acc
    //         })
    // }

    pub fn stream_in(self, ctx: &StreamContext) -> Stream<CacheSource<I>> {
        assert_eq!(
            self.config,
            ctx.config(),
            "Cache must be used in a StreamContext with the same RuntimeConfig"
        );

        let source = CacheSource::new(self.replication, self.data);
        ctx.stream(source)
    }
}

impl<I: Data + Send> Clone for StreamCache<I> {
    fn clone(&self) -> Self {
        // TODO: Check
        StreamCache {
            replication: self.replication,
            config: self.config.clone(),
            data: Arc::new(Mutex::new(self.data.lock().clone())),
        }
    }
}

impl<I: Data, PreviousOperator> CacheSink<I, PreviousOperator>
where
    PreviousOperator: Operator<Out = I> + 'static,
{
    pub(crate) fn new(
        prev: PreviousOperator,
        config: Arc<RuntimeConfig>,
        output: CacheInnerRef<I>,
    ) -> Self {
        Self {
            config,
            buffer: Vec::new(),
            global_id: None,
            prev,
            cache: output,
            terminated: false,
        }
    }
}

impl<I: Data, PreviousOperator> Display for CacheSink<I, PreviousOperator>
where
    PreviousOperator: Operator<Out = I> + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> Cache<{}>", self.prev, std::any::type_name::<I>())
    }
}

impl<I: Data, PreviousOperator> Operator for CacheSink<I, PreviousOperator>
where
    PreviousOperator: Operator<Out = I> + 'static,
{
    type Out = ();

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.prev.setup(metadata);
        self.global_id = Some(metadata.global_id);
        self.cache.lock().register();
    }

    fn next(&mut self) -> super::StreamElement<()> {
        match self.prev.next() {
            StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                self.buffer.push(t);
                StreamElement::Item(())
            }
            StreamElement::Terminate => {
                if self.terminated {
                    return StreamElement::Terminate;
                }
                let mut output = self.cache.lock();

                let buffer = std::mem::take(&mut self.buffer);
                let ret = output.insert(self.global_id.unwrap(), buffer);
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
            .add_operator(OperatorStructure::new::<I, _>("Cache"))
    }
}

impl<I: Data> Display for CacheSource<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CacheSource<{}>", std::any::type_name::<I>())
    }
}

impl<I: Data> Source for CacheSource<I> {
    fn replication(&self) -> Replication {
        self.replication
    }
}

impl<I: Data> Operator for CacheSource<I> {
    type Out = I;

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        let global_id = metadata.global_id;
        let mut cache = self.cache.lock();

        assert!(cache.is_complete(), "Reading cache before it was complete. execution from a cached stream must start after the previous StreamContext has completed!");
        match cache.remove(global_id) {
            Some(items) => self.items = items.into_iter(),
            None => {
                panic!("configuration mismatch for cache, no data for id {global_id}");
            }
        }
    }

    fn next(&mut self) -> StreamElement<I> {
        match self.items.next() {
            Some(item) => StreamElement::Item(item),
            None if !self.flushed => {
                self.flushed = true;
                StreamElement::FlushAndRestart
            }
            None => StreamElement::Terminate,
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<I, _>("CacheSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        operator::source::{IteratorSource, ParallelIteratorSource},
        StreamContext,
    };

    #[test]
    #[should_panic(
        expected = "Reading cache before it was complete. execution from a cached stream must start after the previous StreamContext has completed!"
    )]
    fn read_before_execution() {
        let env = StreamContext::new_local();
        let source = ParallelIteratorSource::new(0..10);

        let (cache, _s) = env.stream(source).map(|x| x + 1).cache();

        cache.inner_cloned();
    }

    #[test]
    fn one_to_one() {
        tracing_subscriber::fmt::SubscriberBuilder::default()
            .with_max_level(tracing::Level::DEBUG)
            .init();
        let ctx = StreamContext::new_local();
        let n = 20;
        let source = IteratorSource::new(0..n);

        let cache = ctx.stream(source).map(|x| x + 1).collect_cache();

        ctx.execute_blocking();

        let expected: HashMap<_, _> = [(0, (0..n).map(|x| x + 1).collect::<Vec<_>>())]
            .into_iter()
            .collect();

        assert_eq!(cache.inner_cloned(), expected);

        // Restart from cache

        let ctx = StreamContext::new_local();
        let result = cache.stream_in(&ctx).map(|x| x * 3).collect_vec();

        ctx.execute_blocking();
        assert_eq!(
            result.get().unwrap(),
            (0..n).map(|x| (x + 1) * 3).collect::<Vec<_>>()
        );
    }

    #[test]
    fn many_to_many() {
        tracing_subscriber::fmt::SubscriberBuilder::default()
            .with_max_level(tracing::Level::DEBUG)
            .init();
        let ctx = StreamContext::new_local();
        let n = 128;
        let cache = ctx.stream_par_iter(0..n).map(|x| x + 1).collect_cache();

        ctx.execute_blocking();

        let expected = (0..n).map(|x| x + 1).collect::<Vec<_>>();
        let mut actual: Vec<_> = cache.inner_cloned().into_values().flatten().collect();
        actual.sort_unstable();
        assert_eq!(actual, expected);

        // Pass through cache
        let ctx = StreamContext::new(cache.config());
        let cache2 = cache.stream_in(&ctx).collect_cache();
        ctx.execute_blocking();

        // Restart from cache

        let ctx = StreamContext::new(cache2.config());
        let result = cache2
            .stream_in(&ctx)
            .shuffle()
            .map(|x| x * 3)
            .collect_vec();

        ctx.execute_blocking();
        let mut result = result.get().unwrap();
        result.sort_unstable();
        assert_eq!(result, (0..n).map(|x| (x + 1) * 3).collect::<Vec<_>>());
    }
}
