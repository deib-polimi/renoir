use crate::operator::window::time_window::TimeWindowGenerator;
use crate::operator::{
    Data, DataKey, StreamElement, Timestamp, Window, WindowDescription, WindowGenerator,
};
use std::collections::VecDeque;
use std::time::{Duration, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct SlidingProcessingTimeWindow {
    size: Duration,
    step: Duration,
}

impl SlidingProcessingTimeWindow {
    pub fn new(size: Duration, step: Duration) -> Self {
        assert!(step <= size);
        assert_ne!(size, Duration::new(0, 0));
        assert_ne!(step, Duration::new(0, 0));
        Self { size, step }
    }
}

impl<Key: DataKey, Out: Data> WindowDescription<Key, Out> for SlidingProcessingTimeWindow {
    type Generator = ProcessingTimeWindowGenerator<Key, Out>;

    fn new_generator(&self) -> Self::Generator {
        Self::Generator::new(self.size, self.step)
    }

    fn to_string(&self) -> String {
        format!(
            "SlidingProcessingTimeWindow[size={}, step={}]",
            self.size.as_secs_f64(),
            self.step.as_secs_f64()
        )
    }
}
#[derive(Clone, Debug)]
pub struct TumblingProcessingTimeWindow {
    size: Duration,
}

impl TumblingProcessingTimeWindow {
    pub fn new(size: Duration) -> Self {
        assert_ne!(size, Duration::new(0, 0));
        Self { size }
    }
}

impl<Key: DataKey, Out: Data> WindowDescription<Key, Out> for TumblingProcessingTimeWindow {
    type Generator = ProcessingTimeWindowGenerator<Key, Out>;

    fn new_generator(&self) -> Self::Generator {
        Self::Generator::new(self.size, self.size)
    }

    fn to_string(&self) -> String {
        format!(
            "TumblingProcessingTimeWindow[size={}]",
            self.size.as_secs_f64(),
        )
    }
}

#[derive(Clone)]
/// Wrapper of `TimeWindowGenerator` that converts every `StreamElement::Item` into
/// a `StreamElement::Timestamped`.
pub struct ProcessingTimeWindowGenerator<Key: DataKey, Out: Data> {
    last_timestamp: Option<Timestamp>,
    generator: TimeWindowGenerator<Key, Out>,
}

impl<Key: DataKey, Out: Data> ProcessingTimeWindowGenerator<Key, Out> {
    fn new(size: Duration, step: Duration) -> Self {
        Self {
            last_timestamp: None,
            generator: TimeWindowGenerator::new(size, step),
        }
    }
}

impl<Key: DataKey, Out: Data> WindowGenerator<Key, Out>
    for ProcessingTimeWindowGenerator<Key, Out>
{
    fn add(&mut self, element: StreamElement<(Key, Out)>) {
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

#[cfg(test)]
mod tests {
    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::{source, TumblingProcessingTimeWindow};
    use std::time::Duration;

    #[test]
    fn tumbling_processing_time() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(1..=1000);

        let res = env
            .stream(source)
            .group_by(|x| x % 2)
            .window(TumblingProcessingTimeWindow::new(Duration::from_micros(
                200,
            )))
            .fold(0, |acc, x| acc + x)
            .unkey()
            .map(|(_, x)| x)
            .collect_vec();
        env.execute();

        let res = res.get().unwrap();
        let sum: i32 = res.into_iter().sum();
        assert_eq!(sum, (1..=1000).sum::<i32>());
    }
}
