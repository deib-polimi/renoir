use std::time::Duration;

use crate::operator::window::time_window::TimeWindowGenerator;
use crate::operator::{Data, DataKey, WindowDescription};

#[derive(Clone, Debug)]
pub struct EventTimeWindow {
    size: Duration,
    step: Duration,
}

impl EventTimeWindow {
    pub fn sliding(size: Duration, step: Duration) -> Self {
        assert!(step <= size);
        assert_ne!(size, Duration::new(0, 0));
        assert_ne!(step, Duration::new(0, 0));
        Self { size, step }
    }

    pub fn tumbling(size: Duration) -> Self {
        Self::sliding(size, size)
    }
}

impl<Key: DataKey, Out: Data> WindowDescription<Key, Out> for EventTimeWindow {
    type Generator = EventTimeWindowGenerator<Key, Out>;

    fn new_generator(&self) -> Self::Generator {
        Self::Generator::new(self.size, self.step)
    }

    fn to_string(&self) -> String {
        format!(
            "SlidingEventTimeWindow[size={}, step={}]",
            self.size.as_secs_f64(),
            self.step.as_secs_f64()
        )
    }
}

pub type EventTimeWindowGenerator<Key, Out> = TimeWindowGenerator<Key, Out>;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::window::EventTimeWindow;
    use crate::operator::{source, Timestamp};

    #[test]
    fn sliding_event_time() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::EventTimeIteratorSource::new(
            (0..10).map(|x| (x, Timestamp::from_secs(x))),
            |x, ts| {
                if x % 2 == 1 {
                    Some(*ts)
                } else {
                    None
                }
            },
        );

        let res = env
            .stream(source)
            .group_by(|x| x % 2)
            .window(EventTimeWindow::sliding(
                Duration::from_secs(3),
                Duration::from_millis(2500),
            ))
            .first()
            .unkey()
            .map(|(_, x)| x)
            .collect_vec();
        env.execute();

        let mut res = res.get().unwrap();
        // Windows and elements
        // 0.0 -> 3.0  [0, 2] and [1]
        // 2.5 -> 5.5  [4] and [3, 5]
        // 5.0 -> 8.0  [6] and [5, 7]
        // 7.5 -> 10.5 [8] and [9]
        res.sort_unstable();
        assert_eq!(res, vec![0, 1, 3, 4, 5, 6, 8, 9]);
    }

    #[test]
    fn tumbling_event_time() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::EventTimeIteratorSource::new(
            (0..10).map(|x| (x, Timestamp::from_secs(x))),
            |x, ts| {
                if x % 2 == 1 {
                    Some(*ts)
                } else {
                    None
                }
            },
        );

        let res = env
            .stream(source)
            .group_by(|x| x % 2)
            .window(EventTimeWindow::tumbling(Duration::from_secs(3)))
            .first()
            .unkey()
            .map(|(_, x)| x)
            .collect_vec();
        env.execute();

        let mut res = res.get().unwrap();
        // Windows and elements
        // 0.0 -> 3.0  [0, 2] and [1]
        // 3.0 -> 6.0  [4] and [3, 5]
        // 6.0 -> 9.0  [6, 8] and [7]
        // 9.0 -> 12.0 [] and [9]
        res.sort_unstable();
        assert_eq!(res, vec![0, 1, 3, 4, 6, 7, 9]);
    }
}
