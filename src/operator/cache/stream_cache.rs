use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::block::Replication;
use crate::operator::Data;
use crate::Stream;
use crate::{CoordUInt, RuntimeConfig, StreamContext};

use super::{CacheRegistryRef, CacheSource, Cacher};

pub struct CachedStream<T, C: Cacher<T>> {
    replication: Replication,
    config: Arc<RuntimeConfig>,
    data: CacheRegistryRef<T, C::Handle>,
}

impl<T: Data, C: Cacher<T>> CachedStream<T, C> {
    pub(crate) fn new(
        config: Arc<RuntimeConfig>,
        replication: Replication,
        cache: CacheRegistryRef<T, C::Handle>,
    ) -> Self {
        Self {
            replication,
            config,
            data: cache,
        }
    }

    /// Returns a copy of the [RuntimeConfig] of the [StreamContext] this
    /// cache was created in.
    pub fn config(&self) -> Arc<RuntimeConfig> {
        self.config.clone()
    }

    /// Returns the data cached on this node by cloning the inner cache.
    pub fn inner_cloned(&self) -> HashMap<CoordUInt, C::Handle>
    where
        C::Handle: Clone,
    {
        let cache = self.data.lock();
        assert!(cache.is_complete(), "Reading cache before it was complete. execution from a cached stream must start after the previous StreamContext has completed!");
        cache.data.clone()
    }

    pub fn into_inner(self) -> HashMap<CoordUInt, C::Handle> {
        assert!(self.data.lock().is_complete(), "Reading cache before it was complete. execution from a cached stream must start after the previous StreamContext has completed!");
        Arc::into_inner(self.data)
            .expect("multiple Arc ref in StreamCache")
            .into_inner()
            .data
    }

    /// Consume the cache creating a new [Stream] in a [StreamContext].
    ///
    /// The [StreamCache] will behave as a source with the same parallelism (and distribution of data)
    /// as the original [Stream] it was cached from.
    ///
    /// + **ATTENTION** ⚠️: The new [StreamContext] must have the same [RuntimeConfig] as the
    ///   one in which the cache was created.
    /// + **ATTENTION** ⚠️: The cache can be resumed **only after** the execution of its origin
    ///   `StreamContext` has terminated.
    pub fn stream_in(self, ctx: &StreamContext) -> Stream<CacheSource<T, C::Replayer>> {
        assert_eq!(
            self.config,
            ctx.config(),
            "Cache must be used in a StreamContext with the same RuntimeConfig"
        );

        let source = CacheSource::<T, C::Replayer>::new(self.replication, self.data);
        ctx.stream(source)
    }

    /// Consume the cache creating a new [Stream] in a [StreamContext] with the same [RuntimeConfig].
    ///
    /// The [StreamCache] will behave as a source with the same parallelism (and distribution of data)
    /// as the original [Stream] it was cached from.
    ///
    /// **Returns**: a tuple containing the new [StreamContext] and the [Stream] with the [CacheSource].
    ///
    /// + **ATTENTION** ⚠️: The cache can be resumed **only after** the execution of its origin
    ///   `StreamContext` has terminated.
    pub fn stream(self) -> (StreamContext, Stream<CacheSource<T, C::Replayer>>) {
        let ctx = StreamContext::new(self.config.clone());
        let stream = self.stream_in(&ctx);
        (ctx, stream)
    }
}

impl<T, C> Clone for CachedStream<T, C>
where
    T: Clone,
    C: Cacher<T>,
    C::Handle: Clone,
{
    fn clone(&self) -> Self {
        // TODO: Check
        CachedStream {
            replication: self.replication,
            config: self.config.clone(),
            data: Arc::new(Mutex::new(self.data.lock().clone())),
        }
    }
}
