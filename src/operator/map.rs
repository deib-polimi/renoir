use std::hash::Hash;

use async_std::sync::Arc;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Map<Out, NewOut, PreviousOperators>
where
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    #[derivative(Debug = "ignore")]
    f: Arc<dyn Fn(Out) -> NewOut + Send + Sync>,
}

#[async_trait]
impl<Out, NewOut, PreviousOperators> Operator<NewOut> for Map<Out, NewOut, PreviousOperators>
where
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    NewOut: Clone + Serialize + DeserializeOwned + Send + 'static,
    PreviousOperators: Operator<Out> + Send,
{
    async fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata).await;
    }

    async fn next(&mut self) -> StreamElement<NewOut> {
        self.prev.next().await.map(&*self.f)
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> Map<{} -> {}>",
            self.prev.to_string(),
            std::any::type_name::<Out>(),
            std::any::type_name::<NewOut>()
        )
    }
}

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    In: Clone + Serialize + DeserializeOwned + Send + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn map<NewOut, F>(self, f: F) -> Stream<In, NewOut, impl Operator<NewOut>>
    where
        NewOut: Clone + Serialize + DeserializeOwned + Send + 'static,
        F: Fn(Out) -> NewOut + Send + Sync + 'static,
    {
        self.add_operator(|prev| Map {
            prev,
            f: Arc::new(f),
        })
    }
}

impl<In, Key, Out, OperatorChain> KeyedStream<In, Key, Out, OperatorChain>
where
    Key: Clone + Serialize + DeserializeOwned + Send + Hash + Eq + 'static,
    In: Clone + Serialize + DeserializeOwned + Send + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn map<NewOut, F>(
        self,
        f: F,
    ) -> KeyedStream<In, Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        NewOut: Clone + Serialize + DeserializeOwned + Send + 'static,
        F: Fn(KeyValue<Key, Out>) -> NewOut + Send + Sync + 'static,
    {
        self.add_operator(|prev| Map {
            prev,
            f: Arc::new(move |(k, v)| (k.clone(), f((k, v)))),
        })
    }
}

#[cfg(test)]
mod tests {
    use async_std::stream::from_iter;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;
    use itertools::Itertools;
    use std::str::FromStr;

    #[async_std::test]
    async fn map_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(from_iter(0..10u8));
        let res = env
            .stream(source)
            .map(|n| n.to_string())
            .map(|n| n + "000")
            .map(|n| u32::from_str(&n).unwrap())
            .collect_vec();
        env.execute().await;
        let res = res.get().unwrap();
        let expected = (0..10u32).map(|n| 1000 * n).collect_vec();
        assert_eq!(res, expected);
    }

    #[async_std::test]
    async fn map_keyed_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(from_iter(0..10u8));
        let res = env
            .stream(source)
            .group_by(|n| n % 2)
            .map(|(k, v)| 100 * k + v)
            .unkey()
            .map(|(_k, v)| v)
            .collect_vec();
        env.execute().await;
        let res = res.get().unwrap().into_iter().sorted().collect_vec();
        let expected = (0..10u8).map(|n| (n % 2) * 100 + n).sorted().collect_vec();
        assert_eq!(res, expected);
    }
}
