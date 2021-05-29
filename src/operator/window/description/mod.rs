use std::time::Duration;

pub use count_window::*;

use crate::operator::window::description::session_window::{
    SessionEventTimeWindowDescr, SessionProcessingTimeWindowDescr,
};
use crate::operator::window::description::sliding_window::{
    SlidingEventTimeWindowDescr, SlidingProcessingTimeWindowDescr,
};

mod count_window;
mod session_window;
mod sliding_window;

pub struct ProcessingTimeWindow {}

impl ProcessingTimeWindow {
    pub fn sliding(size: Duration, step: Duration) -> SlidingProcessingTimeWindowDescr {
        SlidingProcessingTimeWindowDescr::new(size, step)
    }

    pub fn tumbling(size: Duration) -> SlidingProcessingTimeWindowDescr {
        SlidingProcessingTimeWindowDescr::new(size, size)
    }

    pub fn session(gap: Duration) -> SessionProcessingTimeWindowDescr {
        SessionProcessingTimeWindowDescr::new(gap)
    }
}

pub struct EventTimeWindow {}

impl EventTimeWindow {
    pub fn sliding(size: Duration, step: Duration) -> SlidingEventTimeWindowDescr {
        SlidingEventTimeWindowDescr::new(size, step)
    }

    pub fn tumbling(size: Duration) -> SlidingEventTimeWindowDescr {
        Self::sliding(size, size)
    }

    pub fn session(gap: Duration) -> SessionEventTimeWindowDescr {
        SessionEventTimeWindowDescr::new(gap)
    }
}
