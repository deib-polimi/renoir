use std::collections::VecDeque;

use crate::operator::{Data, Operator, StartBlock, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{BlockId, Stream};

#[derive(Debug, Clone)]
pub struct Zip<Out1: Data, Out2: Data> {
    prev1: StartBlock<Out1>,
    prev2: StartBlock<Out2>,
    watermarks1: VecDeque<Timestamp>,
    watermarks2: VecDeque<Timestamp>,
    item1_stash: Option<StreamElement<Out1>>,
}

impl<Out1: Data, Out2: Data> Zip<Out1, Out2> {
    fn new(prev_block_id1: BlockId, prev_block_id2: BlockId) -> Self {
        Self {
            prev1: StartBlock::new(prev_block_id1),
            prev2: StartBlock::new(prev_block_id2),
            watermarks1: Default::default(),
            watermarks2: Default::default(),
            item1_stash: None,
        }
    }
}

impl<Out1: Data, Out2: Data> Operator<(Out1, Out2)> for Zip<Out1, Out2> {
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev1.setup(metadata.clone());
        self.prev2.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<(Out1, Out2)> {
        // both the streams have received a watermark...
        if !self.watermarks1.is_empty() && !self.watermarks2.is_empty() {
            // ...for sure no message earlier than the smaller watermark will be emitted
            return if self.watermarks1[0] < self.watermarks2[0] {
                StreamElement::Watermark(self.watermarks1.pop_front().unwrap())
            } else {
                StreamElement::Watermark(self.watermarks2.pop_front().unwrap())
            };
        }
        // if an item was stored from the previous call of `next` because it couldn't be returned
        // immediately
        let item1 = if let Some(item1) = self.item1_stash.take() {
            item1
        } else {
            loop {
                match self.prev1.next() {
                    StreamElement::Watermark(ts) => self.watermarks1.push_back(ts),
                    StreamElement::FlushBatch => return StreamElement::FlushBatch,
                    StreamElement::End => return StreamElement::End,
                    item => break item,
                }
            }
        };
        let item2 = loop {
            match self.prev2.next() {
                StreamElement::Watermark(ts) => self.watermarks2.push_back(ts),
                StreamElement::FlushBatch => {
                    // make sure not to lose item1
                    self.item1_stash = Some(item1);
                    return StreamElement::FlushBatch;
                }
                StreamElement::End => return StreamElement::End,
                item => break item,
            }
        };
        match (item1, item2) {
            (StreamElement::Item(item1), StreamElement::Item(item2)) => {
                StreamElement::Item((item1, item2))
            }
            (StreamElement::Timestamped(item1, ts1), StreamElement::Timestamped(item2, ts2)) => {
                StreamElement::Timestamped((item1, item2), ts1.max(ts2))
            }
            _ => panic!("Unsupported mixing of timestamped and non-timestamped items"),
        }
    }

    fn to_string(&self) -> String {
        format!(
            "Zip[{}, {}]",
            std::any::type_name::<Out1>(),
            std::any::type_name::<Out2>()
        )
    }
}

impl<Out1: Data, OperatorChain1> Stream<Out1, OperatorChain1>
where
    OperatorChain1: Operator<Out1> + Send + 'static,
{
    pub fn zip<Out2: Data, OperatorChain2>(
        self,
        oth: Stream<Out2, OperatorChain2>,
    ) -> Stream<(Out1, Out2), impl Operator<(Out1, Out2)>>
    where
        OperatorChain2: Operator<Out2> + Send + 'static,
    {
        self.add_y_connection(oth, Zip::new)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;
    use itertools::Itertools;

    #[test]
    fn test_zip() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let items1 = 0..10u8;
        let items2 = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        let source1 = source::StreamSource::new(items1.clone());
        let source2 = source::StreamSource::new(items2.clone().into_iter());
        let stream1 = env.stream(source1);
        let stream2 = env.stream(source2);
        let res = stream1.zip(stream2).collect_vec();
        env.execute();
        let res = res.get().unwrap();
        let expected = items1.zip(items2.into_iter()).collect_vec();
        assert_eq!(res, expected);
    }
}
