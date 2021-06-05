use std::collections::HashMap;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, DataKey, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

#[derive(Clone, Debug)]
struct RichMap<Key: DataKey, Out: Data, NewOut: Data, F, OperatorChain>
where
    F: FnMut(KeyValue<&Key, Out>) -> NewOut + Clone + Send,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    prev: OperatorChain,
    maps_fn: HashMap<Key, F>,
    init_map: F,
    _out: PhantomData<Out>,
    _new_out: PhantomData<NewOut>,
}

impl<Key: DataKey, Out: Data, NewOut: Data, F, OperatorChain>
    RichMap<Key, Out, NewOut, F, OperatorChain>
where
    F: FnMut(KeyValue<&Key, Out>) -> NewOut + Clone + Send,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    fn new(prev: OperatorChain, f: F) -> Self {
        Self {
            prev,
            maps_fn: Default::default(),
            init_map: f,
            _out: Default::default(),
            _new_out: Default::default(),
        }
    }
}

impl<Key: DataKey, Out: Data, NewOut: Data, F, OperatorChain> Operator<KeyValue<Key, NewOut>>
    for RichMap<Key, Out, NewOut, F, OperatorChain>
where
    F: FnMut(KeyValue<&Key, Out>) -> NewOut + Clone + Send,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<(Key, NewOut)> {
        let element = self.prev.next();
        if matches!(element, StreamElement::FlushAndRestart) {
            self.maps_fn.clear();
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

    fn to_string(&self) -> String {
        format!(
            "{} -> RichMap<{} -> {}>",
            self.prev.to_string(),
            std::any::type_name::<Out>(),
            std::any::type_name::<NewOut>()
        )
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<NewOut, _>("RichMap"))
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn rich_map<NewOut: Data, F>(self, mut f: F) -> Stream<NewOut, impl Operator<NewOut>>
    where
        F: FnMut(Out) -> NewOut + Send + Clone + 'static,
    {
        self.key_by(|_| ())
            .add_operator(|prev| RichMap::new(prev, move |(_, value)| f(value)))
            .drop_key()
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    pub fn rich_map<NewOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        F: FnMut(KeyValue<&Key, Out>) -> NewOut + Clone + Send + 'static,
    {
        self.add_operator(|prev| RichMap::new(prev, f))
    }
}
