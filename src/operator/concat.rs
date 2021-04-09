use crate::operator::{Data, Operator, StartBlock};
use crate::stream::Stream;

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn concat<OperatorChain2>(
        self,
        oth: Stream<Out, OperatorChain2>,
    ) -> Stream<Out, impl Operator<Out>>
    where
        OperatorChain2: Operator<Out> + Send + 'static,
    {
        self.add_y_connection(oth, |id1, id2| StartBlock::concat(vec![id1, id2]))
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[test]
    fn concat_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source1 = source::IteratorSource::new(0..10000u16);
        let source2 = source::IteratorSource::new(10000..20000u16);

        let stream1 = env.stream(source1);
        let stream2 = env.stream(source2);

        let res = stream1.concat(stream2).collect_vec();
        env.execute();
        let res = res.get().unwrap();
        let res_sorted = res.into_iter().sorted().collect_vec();
        let expected = (0..20000u16).collect_vec();
        assert_eq!(res_sorted, expected);
    }

    #[test]
    fn concat_stream_with_empty() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source1 = source::IteratorSource::new(0..10000u16);
        let source2 = source::IteratorSource::new(0..0u16);

        let stream1 = env.stream(source1);
        let stream2 = env.stream(source2);

        let res = stream1.concat(stream2).collect_vec();
        env.execute();
        let res = res.get().unwrap();
        let res_sorted = res.into_iter().sorted().collect_vec();
        let expected = (0..10000u16).collect_vec();
        assert_eq!(res_sorted, expected);
    }

    #[test]
    fn concat_stream_with_empty_other_way() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source1 = source::IteratorSource::new(0..0u16);
        let source2 = source::IteratorSource::new(0..10000u16);

        let stream1 = env.stream(source1);
        let stream2 = env.stream(source2);

        let res = stream1.concat(stream2).collect_vec();
        env.execute();
        let res = res.get().unwrap();
        let res_sorted = res.into_iter().sorted().collect_vec();
        let expected = (0..10000u16).collect_vec();
        assert_eq!(res_sorted, expected);
    }

    #[test]
    fn concat_empty_with_empty() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source1 = source::IteratorSource::new(0..0u16);
        let source2 = source::IteratorSource::new(0..0u16);

        let stream1 = env.stream(source1);
        let stream2 = env.stream(source2);

        let res = stream1.concat(stream2).collect_vec();
        env.execute();
        let res = res.get().unwrap();
        assert!(res.is_empty());
    }
}
