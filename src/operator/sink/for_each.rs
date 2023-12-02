use std::fmt::Display;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};

use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct ForEach<F, Op>
where
    F: FnMut(Op::Out) + Send + Clone,
    Op: Operator,
{
    prev: Op,
    #[derivative(Debug = "ignore")]
    f: F,
}

impl<F, Op> ForEach<F, Op>
where
    F: FnMut(Op::Out) + Send + Clone,
    Op: Operator,
{
    pub(crate) fn new(prev: Op, f: F) -> Self {
        Self { prev, f }
    }
}

impl<F, Op> Display for ForEach<F, Op>
where
    F: FnMut(Op::Out) + Send + Clone,
    Op: Operator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> ForEach", self.prev)
    }
}

impl<F, Op> Operator for ForEach<F, Op>
where
    F: FnMut(Op::Out) + Send + Clone,
    Op: Operator,
{
    type Out = ();

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<()> {
        loop {
            match self.prev.next() {
                StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                    (self.f)(t);
                }
                StreamElement::Watermark(w) => return StreamElement::Watermark(w),
                StreamElement::Terminate => return StreamElement::Terminate,
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::FlushAndRestart => return StreamElement::FlushAndRestart,
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Op::Out, _>("ForEachSink");
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::sync::Arc;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[test]
    fn for_each() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let sum = Arc::new(AtomicU8::new(0));
        let sum2 = sum.clone();
        env.stream(source).for_each(move |x| {
            sum.fetch_add(x, Ordering::Release);
        });
        env.execute_blocking();
        assert_eq!(sum2.load(Ordering::Acquire), (0..10).sum::<u8>());
    }

    #[test]
    fn for_each_keyed() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let sum = Arc::new(AtomicU8::new(0));
        let sum2 = sum.clone();
        env.stream(source)
            .group_by(|x| x % 2)
            .for_each(move |(p, x)| {
                sum.fetch_add(x * (p + 1), Ordering::Release);
            });
        env.execute_blocking();
        assert_eq!(
            sum2.load(Ordering::Acquire),
            (0..10).map(|x| x * (x % 2 + 1)).sum::<u8>()
        );
    }
}
