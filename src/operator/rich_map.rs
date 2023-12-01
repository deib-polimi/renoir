use std::collections::HashMap;
use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, DataKey, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

#[derive(Debug)]
pub struct RichMap<Key: DataKey, Out: Data, NewOut: Data, F, OperatorChain>
where
    F: FnMut((&Key, Out)) -> NewOut + Clone + Send,
    OperatorChain: Operator<Out = (Key, Out)>,
{
    prev: OperatorChain,
    maps_fn: HashMap<Key, F, crate::block::GroupHasherBuilder>,
    init_map: F,
    _out: PhantomData<Out>,
    _new_out: PhantomData<NewOut>,
}

impl<Key: DataKey, Out: Data, NewOut: Data, F: Clone, OperatorChain: Clone> Clone
    for RichMap<Key, Out, NewOut, F, OperatorChain>
where
    F: FnMut((&Key, Out)) -> NewOut + Clone + Send,
    OperatorChain: Operator<Out = (Key, Out)>,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            maps_fn: self.maps_fn.clone(),
            init_map: self.init_map.clone(),
            _out: self._out,
            _new_out: self._new_out,
        }
    }
}

impl<Key: DataKey, Out: Data, NewOut: Data, F, OperatorChain> Display
    for RichMap<Key, Out, NewOut, F, OperatorChain>
where
    F: FnMut((&Key, Out)) -> NewOut + Clone + Send,
    OperatorChain: Operator<Out = (Key, Out)>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> RichMap<{} -> {}>",
            self.prev,
            std::any::type_name::<Out>(),
            std::any::type_name::<NewOut>()
        )
    }
}

impl<Key: DataKey, Out: Data, NewOut: Data, F, OperatorChain>
    RichMap<Key, Out, NewOut, F, OperatorChain>
where
    F: FnMut((&Key, Out)) -> NewOut + Clone + Send,
    OperatorChain: Operator<Out = (Key, Out)>,
{
    pub(super) fn new(prev: OperatorChain, f: F) -> Self {
        Self {
            prev,
            maps_fn: Default::default(),
            init_map: f,
            _out: Default::default(),
            _new_out: Default::default(),
        }
    }
}

impl<Key: DataKey, Out: Data, NewOut: Data, F, OperatorChain> Operator
    for RichMap<Key, Out, NewOut, F, OperatorChain>
where
    F: FnMut((&Key, Out)) -> NewOut + Clone + Send,
    OperatorChain: Operator<Out = (Key, Out)>,
{
    type Out = (Key, NewOut);

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<(Key, NewOut)> {
        let element = self.prev.next();
        if matches!(element, StreamElement::FlushAndRestart) {
            // self.maps_fn.clear();
        }
        element.map(|(key, value)| {
            let map_fn = if let Some(map_fn) = self.maps_fn.get_mut(&key) {
                map_fn
            } else {
                // the key is not present in the hashmap, so this always inserts a new map function
                let map_fn = self.init_map.clone();
                self.maps_fn.entry(key.clone()).or_insert(map_fn)
            };

            let new_value = (map_fn)((&key, value));
            (key, new_value)
        })
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<NewOut, _>("RichMap"))
    }
}
