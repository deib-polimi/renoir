use crate::operator::source::Source;
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct StreamSource<Out: Data, It>
where
    It: Iterator<Item = Out> + Send + 'static,
{
    #[derivative(Debug = "ignore")]
    inner: It,
}

impl<Out: Data, It> StreamSource<Out, It>
where
    It: Iterator<Item = Out> + Send + 'static,
{
    pub fn new(inner: It) -> Self {
        Self { inner }
    }
}

impl<Out: Data, It> Source<Out> for StreamSource<Out, It>
where
    It: Iterator<Item = Out> + Send + 'static,
{
    fn get_max_parallelism(&self) -> Option<usize> {
        Some(1)
    }
}

impl<Out: Data, It> Operator<Out> for StreamSource<Out, It>
where
    It: Iterator<Item = Out> + Send + 'static,
{
    fn setup(&mut self, _metadata: ExecutionMetadata) {}

    fn next(&mut self) -> StreamElement<Out> {
        // TODO: with adaptive batching this does not work since it never emits FlushBatch messages
        match self.inner.next() {
            Some(t) => StreamElement::Item(t),
            None => StreamElement::End,
        }
    }

    fn to_string(&self) -> String {
        format!("StreamSource<{}>", std::any::type_name::<Out>())
    }
}

impl<Out: Data, It> Clone for StreamSource<Out, It>
where
    It: Iterator<Item = Out> + Send + 'static,
{
    fn clone(&self) -> Self {
        // Since this is a non-parallel source, we don't want the other replicas to emit any value
        panic!("StreamSource cannot be cloned, max_parallelism should be 1");
    }
}
