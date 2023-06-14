use std::fmt::Display;
use std::hash::BuildHasher;
use std::marker::PhantomData;
use std::sync::Arc;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

use crate::block::{BlockStructure, GroupHasherBuilder, OperatorStructure};
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

use super::DataKey;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct MemoMap<I, O, K, F, Fk, Op, H: BuildHasher + Clone = GroupHasherBuilder>
where
    F: Fn(I) -> O + Send + Clone,
    Fk: Fn(&I) -> K + Send + Clone,
    Op: Operator<I>,
    I: Data,
    O: Data + Sync,
    K: DataKey + Sync,
{
    prev: Op,
    #[derivative(Debug = "ignore")]
    f: F,
    #[derivative(Debug = "ignore")]
    fk: Fk,
    cache: Arc<DashMap<K, O, H>>,
    _i: PhantomData<I>,
}

impl<I, O, K, F, Fk, Op> Display for MemoMap<I, O, K, F, Fk, Op>
where
    F: Fn(I) -> O + Send + Clone,
    Fk: Fn(&I) -> K + Send + Clone,
    Op: Operator<I>,
    I: Data,
    O: Data + Sync,
    K: DataKey + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> MapMemo<{} -> {}>",
            self.prev,
            std::any::type_name::<I>(),
            std::any::type_name::<O>()
        )
    }
}

impl<I, O, K, F, Fk, Op> MemoMap<I, O, K, F, Fk, Op>
where
    F: Fn(I) -> O + Send + Clone,
    Fk: Fn(&I) -> K + Send + Clone,
    Op: Operator<I>,
    I: Data,
    O: Data + Sync,
    K: DataKey + Sync,
{
    pub(super) fn new(prev: Op, f: F, fk: Fk) -> Self {
        Self {
            prev,
            f,
            fk,
            _i: Default::default(),
            cache: Default::default(),
        }
    }
}

impl<I, O, K, F, Fk, Op> Operator<O> for MemoMap<I, O, K, F, Fk, Op>
where
    F: Fn(I) -> O + Send + Clone,
    Fk: Fn(&I) -> K + Send + Clone,
    Op: Operator<I>,
    I: Data,
    O: Data + Sync,
    K: DataKey + Sync,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<O> {
        self.prev.next().map(|v| {
            let k = (self.fk)(&v);
            match self.cache.entry(k) {
                Entry::Occupied(e) => {
                    log::info!("cache hit, loading");
                    e.get().clone()
                }
                Entry::Vacant(e) => {
                    log::info!("cache miss, computing");
                    let o = (self.f)(v);
                    e.insert(o.clone());
                    o
                }
            }
        })
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<O, _>("MapMemo"))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::operator::map::Map;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    #[cfg(feature = "timestamp")]
    fn map_stream() {
        let mut fake_operator = FakeOperator::new(0..10u8);
        for i in 0..10 {
            fake_operator.push(StreamElement::Timestamped(i, i as i64));
        }
        fake_operator.push(StreamElement::Watermark(100));

        let map = Map::new(fake_operator, |x| x.to_string());
        let map = Map::new(map, |x| x + "000");
        let mut map = Map::new(map, |x| u32::from_str(&x).unwrap());

        for i in 0..10 {
            let elem = map.next();
            assert_eq!(elem, StreamElement::Item(i * 1000));
        }
        for i in 0..10 {
            let elem = map.next();
            assert_eq!(elem, StreamElement::Timestamped(i * 1000, i as i64));
        }
        assert_eq!(map.next(), StreamElement::Watermark(100));
        assert_eq!(map.next(), StreamElement::Terminate);
    }
}
