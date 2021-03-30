use std::collections::VecDeque;

use crate::operator::{Data, Operator, StreamElement, WindowDescription, WindowGenerator};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{Stream, WindowedStream};

#[derive(Clone)]
pub(crate) struct First<Out: Data, WinGen, OperatorChain>
where
    WinGen: WindowGenerator<Out> + Clone,
    OperatorChain: Operator<Out>,
{
    window_generator: WinGen,
    prev: OperatorChain,
    extra_items: VecDeque<StreamElement<Out>>,
}

impl<Out: Data, WinGen, OperatorChain> First<Out, WinGen, OperatorChain>
where
    WinGen: WindowGenerator<Out> + Clone,
    OperatorChain: Operator<Out>,
{
    pub(crate) fn new(prev: OperatorChain, window_generator: WinGen) -> Self {
        Self {
            window_generator,
            prev,
            extra_items: Default::default(),
        }
    }
}

impl<Out: Data, WinGen, OperatorChain> Operator<Out> for First<Out, WinGen, OperatorChain>
where
    WinGen: WindowGenerator<Out> + Clone,
    OperatorChain: Operator<Out>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Out> {
        loop {
            if let Some(item) = self.extra_items.pop_front() {
                return item;
            }
            if let Some(mut window) = self.window_generator.next_window() {
                self.extra_items.extend(window.extra_items.drain(..));
                if let Some(first) = window.items().next().cloned() {
                    return if let Some(ts) = window.timestamp {
                        StreamElement::Timestamped(first, ts)
                    } else {
                        StreamElement::Item(first)
                    };
                }
            }
            self.window_generator.add(self.prev.next());
        }
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> {} -> First",
            self.prev.to_string(),
            self.window_generator.to_string()
        )
    }
}

impl<Out: Data, OperatorChain, WinDescr> WindowedStream<Out, OperatorChain, WinDescr>
where
    OperatorChain: Operator<Out> + Send + 'static,
    WinDescr: WindowDescription<Out> + Clone,
{
    pub fn first(self) -> Stream<Out, impl Operator<Out>> {
        let generator = self.window.into_generator();
        self.inner.add_operator(|prev| First::new(prev, generator))
    }
}

#[cfg(test)]
mod tests {
    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::{source, CountWindow};

    #[test]
    fn test_first() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::StreamSource::new(0..10u8);
        let res = env
            .stream(source)
            .window(CountWindow::new(3, 2))
            .first()
            .collect_vec();
        env.execute();
        let res = res.get().unwrap();
        assert_eq!(res, vec![0, 2, 4, 6, 8]);
    }
}
