//! Identity/lifecycle predicates only; never task, SQL, or restore authority.
//! Original Dispatcharr and dependency authors retain all credit and licenses.
use std::sync::{Arc, Weak};

pub(super) fn same_generation<T>(bound: &Weak<T>, actual: &Arc<T>) -> bool {
    bound
        .upgrade()
        .is_some_and(|owner| Arc::ptr_eq(&owner, actual))
}

pub(super) fn live_receiver(started: bool, stopping: bool, stopped: bool, core: bool) -> bool {
    started && !stopping && !stopped && core
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_same_live_generation_matches() {
        let actor = Arc::new(7usize);
        let bound = Arc::downgrade(&actor);
        assert!(same_generation(&bound, &actor.clone()));
        assert!(!same_generation(&bound, &Arc::new(7usize)));
        drop(actor);
        assert!(!same_generation(&bound, &Arc::new(7usize)));
    }

    #[test]
    fn lifecycle_and_owner_mode_cannot_be_replaced_by_matching_metadata() {
        for started in [false, true] {
            for stopping in [false, true] {
                for stopped in [false, true] {
                    for core in [false, true] {
                        assert_eq!(
                            live_receiver(started, stopping, stopped, core),
                            (started, stopping, stopped, core) == (true, false, false, true)
                        );
                    }
                }
            }
        }
    }
}
