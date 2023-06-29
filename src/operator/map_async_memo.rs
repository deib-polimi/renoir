use std::fmt::Display;
use std::hash::BuildHasher;
use std::sync::Arc;

use flume::{Receiver, Sender};
use futures::Future;

use quick_cache::sync::Cache;
use quick_cache::UnitWeighter;

use crate::block::{BlockStructure, GroupHasherBuilder, OperatorStructure};
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

use super::DataKey;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct MapAsyncMemo<I, O, K, Fk, PreviousOperators, H: BuildHasher + Clone = GroupHasherBuilder>
where
    PreviousOperators: Operator<I>,
    I: Data,
{
    prev: PreviousOperators,
    #[derivative(Debug = "ignore")]
    cache: Arc<Cache<K, O, UnitWeighter, H>>,
    #[derivative(Debug = "ignore")]
    fk: Fk,
    i_tx: Sender<I>,
    o_rx: Receiver<O>,
}

impl<I, O, K, Fk, PreviousOperators> Display for MapAsyncMemo<I, O, K, Fk, PreviousOperators>
where
    I: Data,
    PreviousOperators: Operator<I>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> MapAsyncMemo<{} -> {}>",
            self.prev,
            std::any::type_name::<I>(),
            std::any::type_name::<O>()
        )
    }
}

impl<I, O, K, Fk, PreviousOperators> MapAsyncMemo<I, O, K, Fk, PreviousOperators>
where
    I: Data,
    K: DataKey + Sync,
    O: Data + Sync,
    PreviousOperators: Operator<I>,
{
    pub(super) fn new<F, Fut>(prev: PreviousOperators, f: F, fk: Fk, capacity: usize) -> Self
    where
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = O> + Send,
    {
        let (i_tx, i_rx) = flume::bounded(32);
        let (o_tx, o_rx) = flume::bounded(32);

        let f = Arc::new(f);

        tokio::spawn(async move {
            while let Ok(v) = i_rx.recv_async().await {
                let o_tx = o_tx.clone();
                let f = f.clone();
                tokio::spawn(async move {
                    let result = (f)(v).await;
                    o_tx.send_async(result).await.unwrap();
                });
            }
        });

        Self {
            prev,
            i_tx,
            o_rx,
            fk,
            cache: Arc::new(Cache::with(
                capacity,
                capacity as u64,
                UnitWeighter,
                Default::default(),
            )),
        }
    }
}

impl<I, O, K, Fk, PreviousOperators> Operator<O> for MapAsyncMemo<I, O, K, Fk, PreviousOperators>
where
    I: Data,
    O: Data + Sync,
    K: DataKey + Sync,
    Fk: Fn(&I) -> K + Send + Clone,
    PreviousOperators: Operator<I>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<O> {
        let r = self.prev.next().map(|v| {
            let k = (self.fk)(&v);
            match self.cache.get_value_or_guard(&k, None) {
                quick_cache::GuardResult::Value(o) => o,
                quick_cache::GuardResult::Guard(g) => {
                    log::debug!("cache miss, computing");

                    self.i_tx.send(v).unwrap();
                    let o = self.o_rx.recv().unwrap();

                    g.insert(o.clone());
                    o
                }
                quick_cache::GuardResult::Timeout => unreachable!(),
            }
        });

        
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<O, _>("Map"))
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
