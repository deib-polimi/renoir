use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::Stream;
use crate::{KeyValue, KeyedStream};

use super::DataKey;

pub struct ElementGenerator<'a, Out, Op> {
    inner: &'a mut Op,
    _out: PhantomData<Out>,
}

impl<'a, Out: Data, Op: Operator<Out>> ElementGenerator<'a, Out, Op> {
    pub fn new(inner: &'a mut Op) -> Self {
        Self {inner,_out: PhantomData,}
    }

    pub fn next(&mut self) -> StreamElement<Out> {
        self.inner.next()
    }
}

#[derive(Clone, Debug)]
struct RichMapCustom<Out: Data, NewOut: Data, F, OperatorChain>
where
    F: FnMut(ElementGenerator<Out, OperatorChain>) -> StreamElement<NewOut> + Clone + Send,
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    map_fn: F,
    _out: PhantomData<Out>,
    _new_out: PhantomData<NewOut>,
}

impl<Out: Data, NewOut: Data, F, OperatorChain> Display
    for RichMapCustom<Out, NewOut, F, OperatorChain>
where
    F: FnMut(ElementGenerator<Out, OperatorChain>) -> StreamElement<NewOut> + Clone + Send,
    OperatorChain: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> RichMapCustom<{} -> {}>",
            self.prev,
            std::any::type_name::<Out>(),
            std::any::type_name::<NewOut>()
        )
    }
}

impl<Out: Data, NewOut: Data, F, OperatorChain> RichMapCustom<Out, NewOut, F, OperatorChain>
where
    F: FnMut(ElementGenerator<Out, OperatorChain>) -> StreamElement<NewOut> + Clone + Send,
    OperatorChain: Operator<Out>,
{
    fn new(prev: OperatorChain, f: F) -> Self {
        Self {
            prev,
            map_fn: f,
            _out: Default::default(),
            _new_out: Default::default(),
        }
    }
}

impl<Out: Data, NewOut: Data, F, OperatorChain> Operator<NewOut>
    for RichMapCustom<Out, NewOut, F, OperatorChain>
where
    F: FnMut(ElementGenerator<Out, OperatorChain>) -> StreamElement<NewOut> + Clone + Send,
    OperatorChain: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<NewOut> {
        let eg = ElementGenerator::new(&mut self.prev);
        return (self.map_fn)(eg);
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<NewOut, _>("RichMapCustom"))
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Map the elements of the stream into new elements. The mapping function can be stateful.
    ///
    /// This version of `rich_flat_map` is a lower level primitive that gives full control over the
    /// inner types used in streams. It can be used to define custom unary operators.
    ///
    /// The closure must follow these rules to ensure the correct behaviour of noir:
    /// + `Watermark` messages must be sent when no more items with lower timestamp will ever be produced
    /// + `FlushBatch` messages must be forwarded if received
    /// + For each `FlushAndRestart` and `Terminate` message received, the operator must generate
    ///     one and only one message of the same kind. No other messages of this kind should be created
    ///
    /// The mapping function is _cloned_ inside each replica, and they will not share state between
    /// each other. If you want that only a single replica handles all the items you may want to
    /// change the parallelism of this operator with [`Stream::max_parallelism`].
    ///
    /// ## Examples
    ///
    /// TODO
    pub fn rich_map_custom<NewOut: Data, F>(self, f: F) -> Stream<NewOut, impl Operator<NewOut>>
    where
        F: FnMut(ElementGenerator<Out, OperatorChain>) -> StreamElement<NewOut>
            + Clone
            + Send
            + 'static,
    {
        self.add_operator(|prev| RichMapCustom::new(prev, f))
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Map the elements of the stream into new elements. The mapping function can be stateful.
    ///
    /// This is exactly like [`Stream::rich_map`], but the function is cloned for each key. This
    /// means that each key will have a unique mapping function (and therefore a unique state).
    pub fn rich_map_custom<NewOut: Data, F>(self, f: F) -> Stream<NewOut, impl Operator<NewOut>>
    where
        F: FnMut(ElementGenerator<KeyValue<Key, Out>, OperatorChain>) -> StreamElement<NewOut>
            + Clone
            + Send
            + 'static,
    {
        self.0.add_operator(|prev| RichMapCustom::new(prev, f))
    }
}
