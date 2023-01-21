#[cfg(feature = "timestamp")]
use std::time::Duration;

pub use count_window::CountWindow;

#[cfg(feature = "timestamp")]
pub use crate::operator::window::description::session_window::{
    SessionEventTimeWindowDescr, SessionProcessingTimeWindowDescr,
};
#[cfg(feature = "timestamp")]
pub use crate::operator::window::description::sliding_window::{
    SlidingEventTimeWindowDescr, SlidingProcessingTimeWindowDescr,
};

mod count_window;
#[cfg(feature = "timestamp")]
mod session_window;
#[cfg(feature = "timestamp")]
mod sliding_window;

/// Processing-time windows divide elements into windows based on their arrival time.
/// The elements' timestamps are assigned automatically using the system time of the machine.
/// For this reason, processing-time windows only support non-timestamped elements and cannot be
/// used on streams of elements with timestamps and watermarks.
///
/// There are three different kinds of processing-time windows:
///  - [`ProcessingTimeWindow::sliding`]
///  - [`ProcessingTimeWindow::tumbling`]
///  - [`ProcessingTimeWindow::session`]
///
/// Windows have an inclusive start timestamp and an exclusive end timestamp.
#[cfg(feature = "timestamp")]
pub struct ProcessingTimeWindow {}

#[cfg(feature = "timestamp")]
impl ProcessingTimeWindow {
    /// Processing-time sliding windows have a fixed size and they may overlap.
    /// The first window starts at timestamp zero, the second one at timestamp `step`, the third
    /// one at timestamp `2 * step` and so on.
    ///
    /// This is equivalent to say that each window has `size` duration and starts `step` amount of
    /// time after the beginning of the previous one.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::operator::window::ProcessingTimeWindow;
    /// # use std::time::Duration;
    /// # use itertools::Itertools;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .window_all(ProcessingTimeWindow::sliding(Duration::from_secs(2), Duration::from_secs(1)))
    ///     .map(|window| window.cloned().collect_vec())
    ///     .collect_vec();
    ///
    /// env.execute();
    /// let mut res = res.get().unwrap();
    /// ```
    pub fn sliding(size: Duration, step: Duration) -> SlidingProcessingTimeWindowDescr {
        SlidingProcessingTimeWindowDescr::new(size, step)
    }

    /// Processing-time tumbling windows have a fixed size and do not overlap.
    /// They are equivalent to processing-time sliding windows with `step` equal to the size of the
    /// window.
    /// Because of this, each element is assigned to exactly one window.
    ///
    /// ## Example
    /// In this example, tumbling windows with size of two seconds are used.
    ///
    /// The first window starts at timestamp zero (included) and ends at timestamp `2s` (excluded).
    /// The second windows starts at timestamp `2s` and ends at timestamp `4s`, and so on.
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::operator::window::ProcessingTimeWindow;
    /// # use std::time::Duration;
    /// # use itertools::Itertools;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..7)));
    /// let res = s
    ///     .window_all(ProcessingTimeWindow::tumbling(Duration::from_secs(2)))
    ///     .map(|window| window.cloned().collect_vec())
    ///     .collect_vec();
    ///
    /// env.execute();
    /// let mut res = res.get().unwrap();
    /// ```
    pub fn tumbling(size: Duration) -> SlidingProcessingTimeWindowDescr {
        SlidingProcessingTimeWindowDescr::new(size, size)
    }

    /// Processing-time session windows do not have a fixed size and do not overlap.
    ///
    /// If there is no window open and a new element arrives, then a new window is created.
    /// Otherwise, the element is added to the currently open window.
    ///
    /// When new elements do not arrive for `gap` amount of time, the currently open window is closed.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::operator::window::ProcessingTimeWindow;
    /// # use std::time::Duration;
    /// # use itertools::Itertools;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new(vec![0, 1, 4, 5, 6, 9].into_iter()));
    /// let res = s
    ///     .window_all(ProcessingTimeWindow::session(Duration::from_secs(2)))
    ///     .map(|window| window.cloned().collect_vec())
    ///     .collect_vec();
    ///
    /// env.execute();
    /// let mut res = res.get().unwrap();
    /// ```
    pub fn session(gap: Duration) -> SessionProcessingTimeWindowDescr {
        SessionProcessingTimeWindowDescr::new(gap)
    }
}

/// Event-time windows divide elements into windows based on their timestamp.
/// For this reason, event-time windows only support timestamped elements and cannot be used on
/// streams of elements without a timestamp.
///
/// There are three different kinds of event-time windows:
///  - [`EventTimeWindow::sliding`]
///  - [`EventTimeWindow::tumbling`]
///  - [`EventTimeWindow::session`]
///
/// Windows have an inclusive start timestamp and an exclusive end timestamp.
#[cfg(feature = "timestamp")]
pub struct EventTimeWindow {}

#[cfg(feature = "timestamp")]
impl EventTimeWindow {
    /// Event-time sliding windows have a fixed size and they may overlap.
    /// The first window starts at timestamp zero, the second one at timestamp `step`, the third
    /// one at timestamp `2 * step` and so on.
    ///
    /// This is equivalent to say that each window has `size` duration and starts `step` amount of
    /// time after the beginning of the previous one.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::operator::window::{CountWindow, EventTimeWindow};
    /// # use noir::operator::Timestamp;
    /// # use std::time::Duration;
    /// # use itertools::Itertools;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .add_timestamps(
    ///         |&n| Timestamp::from_secs(n),
    ///         |&n, &ts| if n % 2 == 0 { Some(ts) } else { None }
    ///     )
    ///     .window_all(EventTimeWindow::sliding(Duration::from_secs(2), Duration::from_secs(1)))
    ///     .map(|window| window.cloned().collect_vec())
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// assert_eq!(res, vec![vec![0, 1], vec![1, 2], vec![2, 3], vec![3, 4], vec![4]]);
    /// ```
    pub fn sliding(size: Duration, step: Duration) -> SlidingEventTimeWindowDescr {
        SlidingEventTimeWindowDescr::new(size, step)
    }

    /// Event-time tumbling windows have a fixed size and do not overlap.
    /// They are equivalent to event-time sliding windows with `step` equal to the size of the window.
    /// Because of this, each element is assigned to exactly one window.
    ///
    /// ## Example
    /// In this example, tumbling windows with size of two seconds are used.
    ///
    /// The first window starts at timestamp zero (included) and ends at timestamp `2s` (excluded).
    /// The second windows starts at timestamp `2s` and ends at timestamp `4s`, and so on.
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::operator::window::{CountWindow, EventTimeWindow};
    /// # use noir::operator::Timestamp;
    /// # use std::time::Duration;
    /// # use itertools::Itertools;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..7)));
    /// let res = s
    ///     .add_timestamps(
    ///         |&n| Timestamp::from_secs(n),
    ///         |&n, &ts| if n % 2 == 0 { Some(ts) } else { None }
    ///     )
    ///     .window_all(EventTimeWindow::tumbling(Duration::from_secs(2)))
    ///     .map(|window| window.cloned().collect_vec())
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// assert_eq!(res, vec![vec![0, 1], vec![2, 3], vec![4, 5], vec![6]]);
    /// ```
    pub fn tumbling(size: Duration) -> SlidingEventTimeWindowDescr {
        Self::sliding(size, size)
    }

    /// Event-time session windows do not have a fixed size and do not overlap.
    ///
    /// If there is no window open and a new element arrives, then a new window is created.
    /// Otherwise, the element is added to the currently open window if and only if the time
    /// difference between its timestamp and the timestamp of the last element added to the window
    /// is less than `gap`.
    ///
    /// This is equivalent to say that two consecutive elements belong to the same window if and
    /// only if they are at most `gap` amount of time apart.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::operator::window::{CountWindow, EventTimeWindow};
    /// # use noir::operator::Timestamp;
    /// # use std::time::Duration;
    /// # use itertools::Itertools;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new(vec![0, 1, 4, 5, 6, 9].into_iter()));
    /// let res = s
    ///     .add_timestamps(
    ///         |&n| Timestamp::from_secs(n),
    ///         |&n, &ts| if n % 2 == 0 { Some(ts) } else { None }
    ///     )
    ///     .window_all(EventTimeWindow::session(Duration::from_secs(2)))
    ///     .map(|window| window.cloned().collect_vec())
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// assert_eq!(res, vec![vec![0, 1], vec![4, 5, 6], vec![9]]);
    /// ```
    pub fn session(gap: Duration) -> SessionEventTimeWindowDescr {
        SessionEventTimeWindowDescr::new(gap)
    }
}
