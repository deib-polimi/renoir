use std::collections::VecDeque;
use std::num::NonZeroUsize;

use crate::operator::window::{Window, WindowDescription, WindowGenerator};
use crate::operator::{Data, StreamElement, Timestamp};

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

impl<Out: Data> WindowDescription<Out> for CountWindow {
    type GeneratorType = CountWindowGenerator<Out>;

    fn into_generator(self) -> Self::GeneratorType {
        CountWindowGenerator::new(self)
    }
}

#[derive(Clone, Debug)]
pub struct CountWindowGenerator<Out: Data> {
    descr: CountWindow,
    buffer: VecDeque<Out>,
    timestamp_buffer: VecDeque<Timestamp>,
    extra_items: Vec<StreamElement<Out>>,
    received_end: bool,
}

impl<Out: Data> CountWindowGenerator<Out> {
    fn new(descr: CountWindow) -> Self {
        Self {
            descr,
            buffer: Default::default(),
            timestamp_buffer: Default::default(),
            extra_items: Default::default(),
            received_end: false,
        }
    }
}

impl<Out: Data> WindowGenerator<Out> for CountWindowGenerator<Out> {
    fn add(&mut self, item: StreamElement<Out>) {
        match item {
            StreamElement::Item(item) => self.buffer.push_back(item),
            StreamElement::Timestamped(item, ts) => {
                self.buffer.push_back(item);
                self.timestamp_buffer.push_back(ts);
            }
            StreamElement::Watermark(_) => {
                todo!()
            }
            StreamElement::FlushBatch => {
                // TODO: is this ok?
                self.extra_items.push(item);
            }
            StreamElement::End => {
                self.received_end = true;
            }
        }
    }

    fn next_window(&mut self) -> Option<Window<Out>> {
        if self.buffer.len() >= self.descr.size.get() || self.received_end {
            // the stream has ended, make sure to send End only when also the last window is closed
            if self.received_end && self.buffer.is_empty() && self.extra_items.is_empty() {
                self.extra_items.push(StreamElement::End);
            }
            Some(Window {
                size: self.descr.size.get().min(self.buffer.len()),
                extra_items: std::mem::take(&mut self.extra_items),
                gen: self,
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

    fn to_string(&self) -> String {
        format!(
            "CountWindow[size={}, step={}]",
            self.descr.size, self.descr.step
        )
    }
}
