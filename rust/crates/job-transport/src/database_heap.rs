//! Bounded heap event identity/order from the actual Celery event tuple.
//! Original Celery/Dispatcharr authors retain credit/licenses.
use std::{cmp::Ordering, sync::Arc};
pub(super) struct Event {
    pub when: f64,
    pub priority: u8,
    pub name: String,
    identity: Arc<()>,
}
impl Event {
    pub fn new(when: f64, name: String) -> Self {
        Self {
            when,
            priority: 5,
            name,
            identity: Arc::new(()),
        }
    }
    pub fn identity(&self) -> usize {
        Arc::as_ptr(&self.identity) as usize
    }
    fn compare(&self, other: &Self) -> Ordering {
        self.when
            .total_cmp(&other.when)
            .then(self.priority.cmp(&other.priority))
            .then(self.identity().cmp(&other.identity()))
    }
}
pub(super) fn first(events: &[Event]) -> Option<usize> {
    events
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| a.compare(b))
        .map(|(index, _)| index)
}
