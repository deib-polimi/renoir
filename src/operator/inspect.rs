use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, DataKey, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Inspect<Out: Data, F, PreviousOperators>
where
    F: FnMut(&Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    #[derivative(Debug = "ignore")]
    f: F,
    _out: PhantomData<Out>,
}

impl<Out: Data, F, PreviousOperators> Display for Inspect<Out, F, PreviousOperators>
where
    F: FnMut(&Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> Inspect", self.prev)
    }
}

impl<Out: Data, F, PreviousOperators> Operator<Out> for Inspect<Out, F, PreviousOperators>
where
    F: FnMut(&Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Out> {
        let el = self.prev.next();
        match &el {
            StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                (self.f)(t);
            }
            _ => {}
        }
        el
    }

    fn structure(&self) -> BlockStructure {
        let operator = OperatorStructure::new::<Out, _>("Inspect");
        self.prev.structure().add_operator(operator)
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Apply the given function to all the elements of the stream, consuming the stream.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// s.inspect(|n| println!("Item: {}", n)).for_each(std::mem::drop);
    ///
    /// env.execute();
    /// ```
    pub fn inspect<F>(self, f: F) -> Stream<Out, impl Operator<Out>>
    where
        F: FnMut(&Out) + Send + Clone + 'static,
    {
        self.add_operator(|prev| Inspect {
            prev,
            f,
            _out: Default::default(),
        })
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Apply the given function to all the elements of the stream, consuming the stream.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5))).group_by(|&n| n % 2);
    /// s.inspect(|(key, n)| println!("Item: {} has key {}", n, key)).for_each(std::mem::drop);
    ///
    /// env.execute();
    /// ```
    pub fn inspect<F>(self, f: F) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>>
    where
        F: FnMut(&(Key, Out)) + Send + Clone + 'static,
    {
        self.add_operator(|prev| Inspect {
            prev,
            f,
            _out: Default::default(),
        })
    }
}
