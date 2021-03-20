use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;

use crate::block::NextStrategy;
use crate::operator::EndBlock;
use crate::operator::{KeyBy, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};

pub type Keyer<Key, Out> = Arc<dyn Fn(&Out) -> Key + Send + Sync>;

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    In: Clone + Serialize + DeserializeOwned + Send + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn group_by<Key, Keyer>(
        self,
        keyer: Keyer,
    ) -> KeyedStream<Out, Key, Out, impl Operator<KeyValue<Key, Out>>>
    where
        Key: Clone + Serialize + DeserializeOwned + Send + Hash + Eq + 'static,
        Keyer: Fn(&Out) -> Key + Send + Sync + 'static,
    {
        let keyer = Arc::new(keyer);
        let new_stream = self
            .add_block(|prev, _, batch_mode| {
                let keyer = keyer.clone();
                EndBlock::new(
                    prev,
                    NextStrategy::GroupBy(Arc::new(move |out| {
                        let mut s = DefaultHasher::new();
                        keyer(out).hash(&mut s);
                        s.finish() as usize
                    })),
                    batch_mode,
                )
            })
            .add_operator(|prev| KeyBy::new(prev, keyer));
        KeyedStream(new_stream)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[test]
    fn group_by_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(0..100u8);
        let res = env
            .stream(source)
            .group_by(|&n| n.to_string().chars().next().unwrap())
            .unkey()
            .collect_vec();
        env.execute();
        let res = res.get().unwrap().into_iter().sorted().collect_vec();
        let expected = (0..100u8)
            .map(|n| (n.to_string().chars().next().unwrap(), n))
            .sorted()
            .collect_vec();
        assert_eq!(res, expected);
    }
}
