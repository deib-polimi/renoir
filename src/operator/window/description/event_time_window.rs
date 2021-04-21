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

    use itertools::Itertools;

    use crate::operator::window::EventTimeWindow;
    use crate::operator::{StreamElement, Timestamp, WindowDescription, WindowGenerator};

    #[test]
    fn sliding_event_time() {
        let descr = EventTimeWindow::sliding(Duration::from_secs(3), Duration::from_millis(2500));
        let mut generator = descr.new_generator();

        // current window [0, 3)
        generator.add(StreamElement::Timestamped((0, 0), Timestamp::from_secs(0)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Timestamped((0, 1), Timestamp::from_secs(1)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Timestamped((0, 2), Timestamp::from_secs(2)));
        assert!(generator.next_window().is_none());
        // this closes the window
        generator.add(StreamElement::Timestamped((0, 3), Timestamp::from_secs(3)));

        let window = generator.next_window().unwrap();
        assert_eq!(window.timestamp, Some(Timestamp::from_secs(3)));
        let items = window.items().copied().collect_vec();
        assert_eq!(items, vec![0, 1, 2]);
        drop(window);

        // current window [2.5, 5.5)
        generator.add(StreamElement::Watermark(Timestamp::from_secs(5)));
        assert!(generator.next_window().is_none());
        // this closes the window
        generator.add(StreamElement::Watermark(Timestamp::from_secs(6)));

        let window = generator.next_window().unwrap();
        assert_eq!(window.timestamp, Some(Timestamp::from_millis(5500)));
        let items = window.items().copied().collect_vec();
        assert_eq!(items, vec![3]);
        drop(window);

        // current window [7.5, 10.5)
        generator.add(StreamElement::Timestamped(
            (0, 10),
            Timestamp::from_secs(10),
        ));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Terminate);

        let window = generator.next_window().unwrap();
        assert_eq!(window.timestamp, Some(Timestamp::from_millis(10500)));
        let items = window.items().copied().collect_vec();
        assert_eq!(items, vec![10]);
        drop(window);

        // current window [10, 13)
        let window = generator.next_window().unwrap();
        assert_eq!(window.timestamp, Some(Timestamp::from_secs(13)));
        let items = window.items().copied().collect_vec();
        assert_eq!(items, vec![10]);
        drop(window);

        assert!(generator.next_window().is_none());
    }
}
