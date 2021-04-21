use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::operator::source::Source;
use crate::operator::{Data, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct EventTimeIteratorSource<Out: Data, It, WatermarkGen>
where
    It: Iterator<Item = (Out, Timestamp)> + Send + 'static,
    WatermarkGen: Fn(&Out, &Timestamp) -> Option<Timestamp>,
{
    #[derivative(Debug = "ignore")]
    inner: It,
    #[derivative(Debug = "ignore")]
    watermark_gen: WatermarkGen,
    pending_watermark: Option<Timestamp>,
}

impl<Out: Data, It, WatermarkGen> EventTimeIteratorSource<Out, It, WatermarkGen>
where
    It: Iterator<Item = (Out, Timestamp)> + Send + 'static,
    WatermarkGen: Fn(&Out, &Timestamp) -> Option<Timestamp>,
{
    pub fn new(inner: It, watermark_gen: WatermarkGen) -> Self {
        Self {
            inner,
            watermark_gen,
            pending_watermark: None,
        }
    }
}

impl<Out: Data, It, WatermarkGen> Source<Out> for EventTimeIteratorSource<Out, It, WatermarkGen>
where
    It: Iterator<Item = (Out, Timestamp)> + Send + 'static,
    WatermarkGen: Fn(&Out, &Timestamp) -> Option<Timestamp>,
{
    fn get_max_parallelism(&self) -> Option<usize> {
        Some(1)
    }
}

impl<Out: Data, It, WatermarkGen> Operator<Out> for EventTimeIteratorSource<Out, It, WatermarkGen>
where
    It: Iterator<Item = (Out, Timestamp)> + Send + 'static,
    WatermarkGen: Fn(&Out, &Timestamp) -> Option<Timestamp>,
{
    fn setup(&mut self, _metadata: ExecutionMetadata) {}

    fn next(&mut self) -> StreamElement<Out> {
        if let Some(ts) = self.pending_watermark.take() {
            return StreamElement::Watermark(ts);
        }

        // TODO: with adaptive batching this does not work since it never emits FlushBatch messages
        match self.inner.next() {
            Some((item, ts)) => {
                self.pending_watermark = (self.watermark_gen)(&item, &ts);
                StreamElement::Timestamped(item, ts)
            }
            None => StreamElement::Terminate,
        }
    }

    fn to_string(&self) -> String {
        format!("EventTimeStreamSource<{}>", std::any::type_name::<Out>())
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("EventTimeIteratorSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl<Out: Data, It, WatermarkGen> Clone for EventTimeIteratorSource<Out, It, WatermarkGen>
where
    It: Iterator<Item = (Out, Timestamp)> + Send + 'static,
    WatermarkGen: Fn(&Out, &Timestamp) -> Option<Timestamp>,
{
    fn clone(&self) -> Self {
        // Since this is a non-parallel source, we don't want the other replicas to emit any value
        panic!("EventTimeStreamSource cannot be cloned, max_parallelism should be 1");
    }
}

#[cfg(test)]
mod tests {
    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::{source, Timestamp};

    #[test]
    fn event_time_source() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::EventTimeIteratorSource::new(
            (0..10u64).map(|x| (x, Timestamp::new(x, 0))),
            |item, ts| {
                if item % 2 == 1 {
                    Some(*ts)
                } else {
                    None
                }
            },
        );
        let res = env.stream(source).collect_vec();
        env.execute();
        let res = res.get().unwrap();
        assert_eq!(res, (0..10u64).collect::<Vec<_>>());
    }
}
