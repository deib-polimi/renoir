use crate::operator::{Data, DataKey, Operator, StartBlock};
use crate::stream::{KeyValue, KeyedStream, Stream};

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

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn concat<OperatorChain2>(
        self,
        oth: KeyedStream<Key, Out, OperatorChain2>,
    ) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>>
    where
        OperatorChain2: Operator<KeyValue<Key, Out>> + Send + 'static,
    {
        KeyedStream(
            self.0
                .add_y_connection(oth.0, |id1, id2| StartBlock::concat(vec![id1, id2])),
        )
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::block::BlockStructure;
    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::StreamElement::Timestamped;
    use crate::operator::{source, Data, Operator, StreamElement, Timestamp};
    use crate::scheduler::ExecutionMetadata;
    use std::marker::PhantomData;

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

    #[test]
    fn concat_with_timestamps() {
        #[derive(Clone)]
        struct WatermarkController<Out: Data, PreviousOperator>
        where
            PreviousOperator: Operator<Out>,
        {
            last_watermark: Option<Timestamp>,
            prev: PreviousOperator,
            missing_watermarks: usize,
            _out: PhantomData<Out>,
        }

        impl<Out: Data, PreviousOperator: Operator<Out>> WatermarkController<Out, PreviousOperator> {
            fn new(prev: PreviousOperator, expected_watermarks: usize) -> Self {
                Self {
                    last_watermark: None,
                    prev,
                    missing_watermarks: expected_watermarks,
                    _out: Default::default(),
                }
            }
        }

        impl<Out: Data, PreviousOperator: Operator<Out>> Operator<Out>
            for WatermarkController<Out, PreviousOperator>
        {
            fn setup(&mut self, metadata: ExecutionMetadata) {
                self.prev.setup(metadata);
            }

            fn next(&mut self) -> StreamElement<Out> {
                let item = self.prev.next();
                match &item {
                    Timestamped(_, ts) => {
                        if let Some(w) = &self.last_watermark {
                            assert!(ts > w);
                        }
                    }
                    StreamElement::Watermark(ts) => {
                        if let Some(w) = &self.last_watermark {
                            assert!(ts > w);
                        }
                        self.last_watermark = Some(*ts);
                        assert_ne!(self.missing_watermarks, 0);
                        self.missing_watermarks -= 1;
                    }
                    StreamElement::End => {
                        assert_eq!(self.missing_watermarks, 0);
                    }
                    _ => {}
                }
                item
            }

            fn to_string(&self) -> String {
                String::from("WatermarkController")
            }

            fn structure(&self) -> BlockStructure {
                Default::default()
            }
        }

        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source1 = source::EventTimeIteratorSource::new(
            (0..10u64).map(|x| (x, Timestamp::from_secs(x))),
            |x, ts| if x % 2 == 1 { Some(*ts) } else { None },
        );
        let source2 = source::EventTimeIteratorSource::new(
            (100..110u64).map(|x| (x, Timestamp::from_secs(x % 10))),
            |x, ts| if x % 2 == 1 { Some(*ts) } else { None },
        );

        let stream1 = env.stream(source1).shuffle();
        let stream2 = env.stream(source2).shuffle();

        let mut stream = stream1
            .concat(stream2)
            .shuffle()
            .add_operator(|prev| WatermarkController::new(prev, 5));

        stream.block.scheduler_requirements.max_parallelism = Some(1);
        let res = stream.collect_vec();

        env.execute();
        let res = res.get().unwrap();
        assert_eq!(res.len(), 20);
    }

    #[test]
    fn concat_keyed_stream() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source1 = source::IteratorSource::new(0..100u64);
        let source2 = source::IteratorSource::new(100..200u64);

        let stream1 = env.stream(source1).group_by(|x| x % 3);
        let stream2 = env.stream(source2).group_by(|x| x % 3);

        let res = stream1
            .concat(stream2)
            .reduce(|x, y| x + y)
            .unkey()
            .collect_vec();
        env.execute();

        let mut res = res.get().unwrap();
        res.sort_unstable();

        let expected = (0..3)
            .map(|k| (k, (0..200).filter(|x| x % 3 == k).sum()))
            .collect_vec();
        assert_eq!(res, expected);
    }
}
