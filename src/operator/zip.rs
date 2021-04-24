use std::collections::VecDeque;
use std::sync::Arc;

use crate::block::{BlockStructure, OperatorReceiver, OperatorStructure};
use crate::operator::{Data, IterationStateLock, Operator, StartBlock, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{BlockId, Stream};

#[derive(Debug, Clone)]
pub struct Zip<Out1: Data, Out2: Data> {
    prev_block_id1: BlockId,
    prev_block_id2: BlockId,
    prev1: StartBlock<Out1>,
    prev2: StartBlock<Out2>,
    watermarks1: VecDeque<Timestamp>,
    watermarks2: VecDeque<Timestamp>,
    item1_stash: Option<StreamElement<Out1>>,
}

impl<Out1: Data, Out2: Data> Zip<Out1, Out2> {
    fn new(
        prev_block_id1: BlockId,
        prev_block_id2: BlockId,
        state_lock: Option<Arc<IterationStateLock>>,
    ) -> Self {
        Self {
            prev_block_id1,
            prev_block_id2,
            prev1: StartBlock::new(prev_block_id1, state_lock.clone()),
            prev2: StartBlock::new(prev_block_id2, state_lock),
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
                    StreamElement::FlushAndRestart => {
                        // consume the other stream
                        while !matches!(self.prev2.next(), StreamElement::FlushAndRestart) {}
                        return StreamElement::FlushAndRestart;
                    }
                    StreamElement::Terminate => {
                        // consume the other stream
                        while !matches!(self.prev2.next(), StreamElement::Terminate) {}
                        return StreamElement::Terminate;
                    }
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
                StreamElement::FlushAndRestart => {
                    // consume the other stream
                    while !matches!(self.prev1.next(), StreamElement::FlushAndRestart) {}
                    return StreamElement::FlushAndRestart;
                }
                StreamElement::Terminate => {
                    // consume the other stream
                    while !matches!(self.prev1.next(), StreamElement::Terminate) {}
                    return StreamElement::Terminate;
                }
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

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<(Out1, Out2), _>("Zip");
        operator
            .receivers
            .push(OperatorReceiver::new::<Out1>(self.prev_block_id1));
        operator
            .receivers
            .push(OperatorReceiver::new::<Out2>(self.prev_block_id2));
        BlockStructure::default().add_operator(operator)
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
        let mut new_stream = self.add_y_connection(oth, Zip::new);
        // if the zip operator is partitioned there could be some loss of data
        new_stream.block.scheduler_requirements.max_parallelism(1);
        new_stream
    }
}
