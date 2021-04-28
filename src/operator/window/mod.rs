use std::collections::VecDeque;
use std::fmt::{Display, Formatter};

use hashbrown::hash_map::IterMut;
use hashbrown::HashMap;

pub use aggregator::*;
pub use description::*;

use crate::operator::{Data, DataKey, Operator, StreamElement, Timestamp};
use crate::stream::{KeyValue, KeyedStream, KeyedWindowedStream, Stream, WindowedStream};

mod aggregator;
mod description;
mod generic_operator;
mod join;
mod time_window;

/// A WindowDescription describes how a window behaves.
pub trait WindowDescription<Key: DataKey, Out: Data>: Send {
    /// The type of the window generator of this window.
    type Generator: WindowGenerator<Key, Out> + Clone + 'static;

    /// Construct a new window generator, ready to handle elements.
    fn new_generator(&self) -> Self::Generator;

    fn to_string(&self) -> String;
}

/// A WindowGenerator handles the generation of windows for a given key.
pub trait WindowGenerator<Key: DataKey, Out: Data>: Send {
    /// Handle a new element of the stream.
    fn add(&mut self, item: StreamElement<KeyValue<Key, Out>>);
    /// If a window is ready, return it so that it can be processed.
    fn next_window(&mut self) -> Option<Window<Key, Out>>;
    /// Close the current open window.
    /// This method is called when a `Window` is dropped after being processed.
    fn advance(&mut self);
    /// Return the buffer from which `Window` will get the elements of the window.
    fn buffer(&self) -> &VecDeque<Out>;
}

/// A window is a collection of elements and may be associated with a timestamp.
pub struct Window<'a, Key: DataKey, Out: Data> {
    /// A reference to the generator that produced this window.
    ///
    /// This will be used for fetching the window elements and for advancing the window when this
    /// is dropped.
    gen: &'a mut dyn WindowGenerator<Key, Out>,
    /// The number of elements of this window.
    size: usize,
    /// If this window contains elements with a timestamp, a timestamp for this window is built.
    timestamp: Option<Timestamp>,
}

impl<'a, Key: DataKey, Out: Data> Window<'a, Key, Out> {
    /// An iterator to the elements of the window.
    fn items(&self) -> impl Iterator<Item = &Out> {
        self.gen.buffer().iter().take(self.size)
    }
}

impl<'a, Key: DataKey, Out: Data> Drop for Window<'a, Key, Out> {
    fn drop(&mut self) {
        self.gen.advance();
    }
}

/// Wrapper used to return either a single or multiple `WindowGenerator`.
///
/// This is the type returned by `KeyedWindowManager::add`. Adding an item into the window may
/// produce different outcomes based on the item's type.
enum ManagerAddIterator<'a, Key: DataKey, Out: Data, WindowDescr: WindowDescription<Key, Out>> {
    /// The item triggered just one key, so only the generator of that key is returned.
    ///
    /// This is the typical case for `Item` and `Timestamped`.
    Single(Option<(&'a Key, &'a mut WindowDescr::Generator)>),
    /// The item involed all the keys, so all the generators are returned.
    ///
    /// This happens for watermarks or stream ending, when all the keys may advance the window.
    All(IterMut<'a, Key, WindowDescr::Generator>),
}

impl<'a, Key: DataKey, Out: Data, WindowDescr: WindowDescription<Key, Out>> Iterator
    for ManagerAddIterator<'a, Key, Out, WindowDescr>
{
    type Item = (&'a Key, &'a mut WindowDescr::Generator);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ManagerAddIterator::Single(single) => single.take(),
            ManagerAddIterator::All(iter) => iter.next(),
        }
    }
}

/// Manager of multiple `WindowGenerator`, one for each key of the stream.
#[derive(Clone)]
struct KeyedWindowManager<Key: DataKey, Out: Data, WindowDescr: WindowDescription<Key, Out>> {
    /// The description that is used for generating the windows.
    descr: WindowDescr,
    /// The set of all the generators, indexed by key.
    generators: HashMap<Key, WindowDescr::Generator>,
    /// The list of additional items to return downstream.
    ///
    /// This list will include the `Watermark`s, `Terminate`, and other non-data items.
    extra_items: VecDeque<StreamElement<KeyValue<Key, Out>>>,
    /// Whether the content of `generators` should be dropped.
    should_reset: bool,
}

impl<Key: DataKey, Out: Data, WindowDescr: WindowDescription<Key, Out>>
    KeyedWindowManager<Key, Out, WindowDescr>
{
    fn new(descr: WindowDescr) -> Self {
        Self {
            descr,
            generators: Default::default(),
            extra_items: Default::default(),
            should_reset: false,
        }
    }

    /// Add a `StreamElement` to this window manager.
    ///
    /// The returned value contains the set of all the involved window generators. The caller should
    /// process all the windows returned from each generator.
    fn add(
        &mut self,
        el: StreamElement<KeyValue<Key, Out>>,
    ) -> impl Iterator<Item = (&Key, &mut WindowDescr::Generator)> {
        if self.should_reset {
            assert!(
                self.extra_items.is_empty(),
                "resetting would lose extra items"
            );
            self.generators.clear();
        }
        match &el {
            StreamElement::Item((k, _)) | StreamElement::Timestamped((k, _), _) => {
                let key = k.clone();
                if let Some(gen) = self.generators.get_mut(k) {
                    gen.add(el);
                } else {
                    let mut gen = self.descr.new_generator();
                    gen.add(el);
                    self.generators.insert(key.clone(), gen);
                }

                // Only return this window generator, since the others are not affected by this element
                return ManagerAddIterator::<Key, Out, WindowDescr>::Single(
                    self.generators.get_key_value_mut(&key),
                );
            }

            StreamElement::FlushAndRestart | StreamElement::Watermark(_) => {
                // This is a flush, so all the generators should be dropped.
                self.should_reset = matches!(el, StreamElement::FlushAndRestart);
                // Pass the element to every window generator
                for (_key, gen) in self.generators.iter_mut() {
                    gen.add(el.clone());
                }
                // Save this element so that it can be forwarded downstream
                self.extra_items.push_back(el);
                // Return all the generators
                return ManagerAddIterator::<Key, Out, WindowDescr>::All(
                    self.generators.iter_mut(),
                );
            }
            StreamElement::FlushBatch => {
                unreachable!("KeyedWindowManager does not handle FlushBatch")
            }
            StreamElement::Terminate => {
                unreachable!("KeyedWindowManager does not handle Terminate")
            }
        }
    }

    /// Return extra elements that should be forwarded downstream.
    fn next_extra_elements(&mut self) -> Option<StreamElement<KeyValue<Key, Out>>> {
        self.extra_items.pop_front()
    }
}

impl<Key: DataKey, Out: Data, WindowDescr: WindowDescription<Key, Out>> Display
    for KeyedWindowManager<Key, Out, WindowDescr>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "KeyedWindowManager<({}, {}), {}>",
            std::any::type_name::<Key>(),
            std::any::type_name::<Out>(),
            self.descr.to_string()
        )
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn window<WinOut: Data, WinDescr: WindowDescription<Key, WinOut>>(
        self,
        descr: WinDescr,
    ) -> KeyedWindowedStream<Key, Out, impl Operator<KeyValue<Key, Out>>, WinOut, WinDescr> {
        KeyedWindowedStream {
            inner: self,
            descr,
            _win_out: Default::default(),
        }
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn window_all<WinOut: Data, WinDescr: WindowDescription<(), WinOut>>(
        self,
        descr: WinDescr,
    ) -> WindowedStream<Out, impl Operator<KeyValue<(), Out>>, WinOut, WinDescr> {
        // max_parallelism and key_by are used instead of group_by so that there is exactly one
        // replica, since window_all cannot be parallelized
        let stream = self.max_parallelism(1).key_by(|_| ()).window(descr);
        WindowedStream { inner: stream }
    }
}
