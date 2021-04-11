use std::collections::VecDeque;

pub use count_window::*;
pub use event_time_window::*;
pub use processing_time_window::*;

use crate::operator::{Data, DataKey, Operator, Reorder, StreamElement, Timestamp};
use crate::stream::{KeyValue, KeyedStream, WindowedStream};
use hashbrown::hash_map::IterMut;
use hashbrown::HashMap;

mod count_window;
mod event_time_window;
mod first;
mod fold;
mod generic_operator;
mod max;
mod min;
mod processing_time_window;
mod sum;
mod time_window;

/// A WindowDescription describes how a window behaves.
pub trait WindowDescription<Key: DataKey, Out: Data> {
    /// The type of the window generator of this window.
    type Generator: WindowGenerator<Key, Out> + Clone + 'static;
    /// Construct a new window generator, ready to handle elements.
    fn new_generator(&self) -> Self::Generator;
    fn to_string(&self) -> String;
}

/// A WindowGenerator handles the generation of windows for a given key.
pub trait WindowGenerator<Key: DataKey, Out: Data> {
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

pub struct Window<'a, Key: DataKey, Out: Data> {
    gen: &'a mut dyn WindowGenerator<Key, Out>,
    size: usize,
    timestamp: Option<Timestamp>,
}

impl<'a, Key: DataKey, Out: Data> Window<'a, Key, Out> {
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
enum ManagerAddIterator<'a, Key: DataKey, Out: Data, WindowDescr: WindowDescription<Key, Out>> {
    Single(Option<(&'a Key, &'a mut WindowDescr::Generator)>),
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
    descr: WindowDescr,
    generators: HashMap<Key, WindowDescr::Generator>,
    extra_items: VecDeque<StreamElement<KeyValue<Key, Out>>>,
}

impl<Key: DataKey, Out: Data, WindowDescr: WindowDescription<Key, Out>>
    KeyedWindowManager<Key, Out, WindowDescr>
{
    fn new(descr: WindowDescr) -> Self {
        Self {
            descr,
            generators: Default::default(),
            extra_items: Default::default(),
        }
    }

    fn add(
        &mut self,
        el: StreamElement<KeyValue<Key, Out>>,
    ) -> impl Iterator<Item = (&Key, &mut WindowDescr::Generator)> {
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

            StreamElement::End | StreamElement::Watermark(_) => {
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
        }
    }

    /// Return extra elements that should be forwarded downstream
    fn next_extra_elements(&mut self) -> Option<StreamElement<KeyValue<Key, Out>>> {
        self.extra_items.pop_front()
    }

    // TODO: maybe switch to Display
    #[allow(clippy::inherent_to_string)]
    fn to_string(&self) -> String {
        format!(
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
    pub fn window<WinDescr: WindowDescription<Key, Out>>(
        self,
        descr: WinDescr,
    ) -> WindowedStream<Key, Out, impl Operator<KeyValue<Key, Out>>, WinDescr> {
        WindowedStream {
            inner: self.add_operator(Reorder::new),
            descr,
        }
    }
}
