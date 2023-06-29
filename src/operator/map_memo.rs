use std::fmt::Display;
use std::hash::BuildHasher;
use std::marker::PhantomData;
use std::sync::Arc;

use quick_cache::sync::Cache;
use quick_cache::UnitWeighter;

use crate::block::{BlockStructure, GroupHasherBuilder, OperatorStructure};
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

use super::DataKey;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct MapMemo<I, O, K, F, Fk, Op, H: BuildHasher + Clone = GroupHasherBuilder>
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
    cache: Arc<Cache<K, O, UnitWeighter, H>>,
    _i: PhantomData<I>,
}

impl<I, O, K, F, Fk, Op> Display for MapMemo<I, O, K, F, Fk, Op>
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

impl<I, O, K, F, Fk, Op> MapMemo<I, O, K, F, Fk, Op>
where
    F: Fn(I) -> O + Send + Clone,
    Fk: Fn(&I) -> K + Send + Clone,
    Op: Operator<I>,
    I: Data,
    O: Data + Sync,
    K: DataKey + Sync,
{
    pub(super) fn new(prev: Op, f: F, fk: Fk, capacity: usize) -> Self {
        Self {
            prev,
            f,
            fk,
            _i: Default::default(),
            cache: Arc::new(Cache::with(
                capacity,
                capacity as u64,
                UnitWeighter,
                Default::default(),
            )),
        }
    }
}

impl<I, O, K, F, Fk, Op> Operator<O> for MapMemo<I, O, K, F, Fk, Op>
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
            match self.cache.get_value_or_guard(&k, None) {
                quick_cache::GuardResult::Value(o) => o,
                quick_cache::GuardResult::Guard(g) => {
                    log::debug!("cache miss, computing");
                    let o = (self.f)(v);
                    g.insert(o.clone());
                    o
                }
                quick_cache::GuardResult::Timeout => unreachable!(),
            }
        })
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<O, _>("MapMemo"))
    }
}

// #[cfg(test)]
// mod tests {
//     use std::str::FromStr;

//     use crate::operator::map::Map;
//     use crate::operator::{Operator, StreamElement};
//     use crate::test::FakeOperator;

//     #[test]
//     #[cfg(feature = "timestamp")]
//     fn map_stream() {
//         let mut fake_operator = FakeOperator::new(0..10u8);
//         for i in 0..10 {
//             fake_operator.push(StreamElement::Timestamped(i, i as i64));
//         }
//         fake_operator.push(StreamElement::Watermark(100));

//         let map = Map::new(fake_operator, |x| x.to_string());
//         let map = Map::new(map, |x| x + "000");
//         let mut map = Map::new(map, |x| u32::from_str(&x).unwrap());

//         for i in 0..10 {
//             let elem = map.next();
//             assert_eq!(elem, StreamElement::Item(i * 1000));
//         }
//         for i in 0..10 {
//             let elem = map.next();
//             assert_eq!(elem, StreamElement::Timestamped(i * 1000, i as i64));
//         }
//         assert_eq!(map.next(), StreamElement::Watermark(100));
//         assert_eq!(map.next(), StreamElement::Terminate);
//     }
// }
