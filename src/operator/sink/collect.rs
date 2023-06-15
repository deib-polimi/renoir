use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::operator::sink::{Sink, StreamOutputRef};
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

#[derive(Debug)]
pub struct Collect<Out: ExchangeData, C: FromIterator<Out> + Send, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    output: StreamOutputRef<C>,
    _out: PhantomData<Out>,
}

impl<Out: ExchangeData, C: FromIterator<Out> + Send, PreviousOperators>
    Collect<Out, C, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    pub fn new(prev: PreviousOperators, output: StreamOutputRef<C>) -> Self {
        Self {
            prev,
            output,
            _out: PhantomData,
        }
    }
}

impl<Out: ExchangeData, C: FromIterator<Out> + Send, PreviousOperators> Display
    for Collect<Out, C, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Collect<{}>",
            self.prev,
            std::any::type_name::<C>()
        )
    }
}

impl<Out: ExchangeData, C: FromIterator<Out> + Send, PreviousOperators> Operator<()>
    for Collect<Out, C, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<()> {
        let iter = std::iter::from_fn(|| loop {
            match self.prev.next() {
                StreamElement::Item(t) | StreamElement::Timestamped(t, _) => return Some(t),
                StreamElement::Terminate => return None,
                _ => continue,
            }
        });
        let c = C::from_iter(iter);
        *self.output.lock().unwrap() = Some(c);

        StreamElement::Terminate
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("Collect");
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }
}

impl<Out: ExchangeData, C: FromIterator<Out> + Send, PreviousOperators> Sink
    for Collect<Out, C, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
}

impl<Out: ExchangeData, C: FromIterator<Out> + Send, PreviousOperators> Clone
    for Collect<Out, C, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn clone(&self) -> Self {
        panic!("Collect cannot be cloned, replication should be 1");
    }
}

#[cfg(test)]
mod qtests {
    use std::collections::HashSet;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[test]
    fn collect_vec() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let res = env.stream(source).collect::<Vec<_>>();
        env.execute_blocking();
        assert_eq!(res.get().unwrap(), (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn collect_set() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let res = env.stream(source).collect::<HashSet<_>>();
        env.execute_blocking();
        assert_eq!(res.get().unwrap(), (0..10).collect::<HashSet<_>>());
    }
}
