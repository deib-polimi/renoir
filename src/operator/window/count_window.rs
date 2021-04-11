use std::collections::VecDeque;
use std::num::NonZeroUsize;

use crate::operator::window::{Window, WindowDescription, WindowGenerator};
use crate::operator::{Data, DataKey, StreamElement, Timestamp};
use crate::stream::KeyValue;
use std::marker::PhantomData;

#[derive(Clone, Debug)]
pub struct CountWindow {
    size: NonZeroUsize,
    step: NonZeroUsize,
}

impl CountWindow {
    pub fn new(size: usize, step: usize) -> Self {
        Self {
            size: NonZeroUsize::new(size).expect("CountWindow size must be positive"),
            step: NonZeroUsize::new(step).expect("CountWindow size must be positive"),
        }
    }
}

impl<Key: DataKey, Out: Data> WindowDescription<Key, Out> for CountWindow {
    type Generator = CountWindowGenerator<Key, Out>;

    fn new_generator(&self) -> Self::Generator {
        CountWindowGenerator::new(self.clone())
    }

    fn to_string(&self) -> String {
        format!(
            "CountWindow[size={}, step={}]",
            self.size.get(),
            self.step.get()
        )
    }
}

#[derive(Clone, Debug)]
pub struct CountWindowGenerator<Key: DataKey, Out: Data> {
    descr: CountWindow,
    buffer: VecDeque<Out>,
    timestamp_buffer: VecDeque<Timestamp>,
    received_end: bool,
    _key: PhantomData<Key>,
}

impl<Key: DataKey, Out: Data> CountWindowGenerator<Key, Out> {
    fn new(descr: CountWindow) -> Self {
        Self {
            descr,
            buffer: Default::default(),
            timestamp_buffer: Default::default(),
            received_end: false,
            _key: Default::default(),
        }
    }
}

impl<Key: DataKey, Out: Data> WindowGenerator<Key, Out> for CountWindowGenerator<Key, Out> {
    fn add(&mut self, item: StreamElement<KeyValue<Key, Out>>) {
        match item {
            StreamElement::Item((_, item)) => self.buffer.push_back(item),
            StreamElement::Timestamped((_, item), ts) => {
                self.buffer.push_back(item);
                self.timestamp_buffer.push_back(ts);
            }
            StreamElement::Watermark(_) => {
                // Watermarks are not used by count windows
            }
            StreamElement::FlushBatch => unreachable!("Windows do not handle FlushBatch"),
            StreamElement::End => {
                self.received_end = true;
            }
            StreamElement::IterEnd => {
                todo!("Count windows inside an iteration are not supported yet");
            }
        }
    }

    fn next_window(&mut self) -> Option<Window<Key, Out>> {
        if self.buffer.len() >= self.descr.size.get()
            || (self.received_end && !self.buffer.is_empty())
        {
            Some(Window {
                size: self.descr.size.get().min(self.buffer.len()),
                gen: self,
                // TODO: handle timestamp
                // TODO: make sure the last incomplete windows do not have timestamps bigger than old watermarks
                timestamp: None,
            })
        } else {
            None
        }
    }

    fn advance(&mut self) {
        for _ in 0..self.descr.step.get() {
            self.buffer.pop_front();
            self.timestamp_buffer.pop_front();
        }
    }

    fn buffer(&self) -> &VecDeque<Out> {
        &self.buffer
    }
}
