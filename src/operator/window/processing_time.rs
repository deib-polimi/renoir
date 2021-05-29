use std::collections::VecDeque;
use std::marker::PhantomData;
use std::time::UNIX_EPOCH;

use crate::operator::{Data, DataKey, StreamElement, Timestamp, Window, WindowGenerator};

/// Wrapper of a `WindowGenerator` that converts every `StreamElement::Item` into
/// a `StreamElement::Timestamped`.
#[derive(Clone)]
pub struct ProcessingTimeWindowGenerator<
    Key: DataKey,
    Out: Data,
    WindowGen: WindowGenerator<Key, Out>,
> {
    last_timestamp: Option<Timestamp>,
    generator: WindowGen,
    _key: PhantomData<Key>,
    _out: PhantomData<Out>,
}

impl<Key: DataKey, Out: Data, WindowGen: WindowGenerator<Key, Out>>
    ProcessingTimeWindowGenerator<Key, Out, WindowGen>
{
    pub(crate) fn new(generator: WindowGen) -> Self {
        Self {
            last_timestamp: None,
            generator,
            _key: Default::default(),
            _out: Default::default(),
        }
    }
}

impl<Key: DataKey, Out: Data, WindowGen: WindowGenerator<Key, Out>> WindowGenerator<Key, Out>
    for ProcessingTimeWindowGenerator<Key, Out, WindowGen>
{
    fn add(&mut self, element: StreamElement<Out>) {
        match element {
            StreamElement::Item(item) => {
                // TODO: consider not using `SystemTime`
                let elapsed = UNIX_EPOCH.elapsed().unwrap();
                // Make sure timestamps are monotonic
                let timestamp = match self.last_timestamp {
                    None => elapsed,
                    Some(last_ts) => last_ts.max(elapsed),
                };
                self.last_timestamp = Some(timestamp);

                self.generator
                    .add(StreamElement::Timestamped(item, timestamp))
            }
            StreamElement::Timestamped(_, _) | StreamElement::Watermark(_) => {
                panic!("Processing time windows don't handle timestamps")
            }
            _ => self.generator.add(element),
        }
    }

    fn next_window(&mut self) -> Option<Window<Key, Out>> {
        let mut window = self.generator.next_window();
        if let Some(w) = &mut window {
            // TODO: should we keep the timestamp?
            w.timestamp = None;
        }
        window
    }

    fn advance(&mut self) {
        self.generator.advance()
    }

    fn buffer(&self) -> &VecDeque<Out> {
        self.generator.buffer()
    }
}
