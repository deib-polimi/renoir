use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::block::NextStrategy;
use crate::operator::{EndBlock, Operator};
use crate::stream::Stream;

impl<In, Out, OperatorChain> Stream<In, Out, OperatorChain>
where
    In: Clone + Serialize + DeserializeOwned + Send + 'static,
    Out: Clone + Serialize + DeserializeOwned + Send + 'static,
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn shuffle(mut self) -> Stream<Out, Out, impl Operator<Out>> {
        self.block.next_strategy = NextStrategy::Random;
        self.add_block(EndBlock::new)
    }
}

#[cfg(test)]
mod tests {
    use async_std::stream::from_iter;
    use itertools::Itertools;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[async_std::test]
    async fn shuffle_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(from_iter(0..1000u16));
        let res = env
            .stream(source)
            .shuffle()
            .shuffle()
            .shuffle()
            .shuffle()
            .shuffle()
            .collect_vec();
        env.execute().await;
        let res = res.get().unwrap();
        let res_sorted = res.clone().into_iter().sorted().collect_vec();
        let expected = (0..1000u16).collect_vec();
        assert_eq!(res_sorted, expected);
        assert_ne!(
            res, expected,
            "It's very improbable that going to the shuffles the result is sorted"
        );
    }
}
