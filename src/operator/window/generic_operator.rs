use std::collections::VecDeque;

use crate::operator::window::KeyedWindowManager;
use crate::operator::{
    Data, DataKey, Operator, StreamElement, Window, WindowDescription, WindowGenerator,
};
use crate::scheduler::ExecutionMetadata;
use crate::stream::KeyValue;

#[derive(Clone)]
pub(crate) struct GenericWindowOperator<
    Key: DataKey,
    Out: Data,
    NewOut: Data,
    ProcessFunc,
    WindowDescr,
    OperatorChain,
> where
    ProcessFunc: Fn(&Window<Key, Out>) -> NewOut + Clone,
    WindowDescr: WindowDescription<Key, Out> + Clone,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    name: String,
    manager: KeyedWindowManager<Key, Out, WindowDescr>,
    process_func: ProcessFunc,
    prev: OperatorChain,
    buffer: VecDeque<StreamElement<KeyValue<Key, NewOut>>>,
}

impl<Key: DataKey, Out: Data, NewOut: Data, F, WindowDescr, OperatorChain>
    GenericWindowOperator<Key, Out, NewOut, F, WindowDescr, OperatorChain>
where
    F: Fn(&Window<Key, Out>) -> NewOut + Clone,
    WindowDescr: WindowDescription<Key, Out> + Clone,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    pub(crate) fn new<S: Into<String>>(
        name: S,
        prev: OperatorChain,
        descr: WindowDescr,
        process_func: F,
    ) -> Self {
        Self {
            name: name.into(),
            manager: KeyedWindowManager::new(descr),
            process_func,
            prev,
            buffer: Default::default(),
        }
    }
}

impl<Key: DataKey, Out: Data, NewOut: Data, F, WindowDescr, OperatorChain>
    Operator<KeyValue<Key, NewOut>>
    for GenericWindowOperator<Key, Out, NewOut, F, WindowDescr, OperatorChain>
where
    F: Fn(&Window<Key, Out>) -> NewOut + Clone,
    WindowDescr: WindowDescription<Key, Out> + Clone,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<KeyValue<Key, NewOut>> {
        loop {
            if let Some(item) = self.buffer.pop_front() {
                return item;
            }

            let item = self.prev.next();
            // the manager is not interested in FlushBatch
            if matches!(item, StreamElement::FlushBatch) {
                return StreamElement::FlushBatch;
            }

            for (key, window_gen) in self.manager.add(item) {
                while let Some(window) = window_gen.next_window() {
                    let value = (self.process_func)(&window);
                    self.buffer.push_back(if let Some(ts) = window.timestamp {
                        StreamElement::Timestamped((key.clone(), value), ts)
                    } else {
                        StreamElement::Item((key.clone(), value))
                    });
                }
            }

            while let Some(el) = self.manager.next_extra_elements() {
                self.buffer.push_back(match el {
                    StreamElement::Item(_) => {
                        unreachable!("Items cannot be returned as extra elements")
                    }
                    StreamElement::Timestamped(_, _) => {
                        unreachable!("Timestamped items cannot be returned as extra elements")
                    }
                    StreamElement::Watermark(ts) => StreamElement::Watermark(ts),
                    StreamElement::FlushBatch => StreamElement::FlushBatch,
                    StreamElement::End => StreamElement::End,
                });
            }
        }
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> {} -> GenericOperator[{}]<{}>",
            self.prev.to_string(),
            self.manager.to_string(),
            self.name,
            std::any::type_name::<NewOut>(),
        )
    }
}
