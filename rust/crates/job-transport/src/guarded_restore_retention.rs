//! A pause token can retain the actual later-acquired reservation on cancellation.
//! Original Dispatcharr and dependency authors retain their credit and licenses.
use std::sync::{Arc, Mutex};

enum Slot<T> {
    Empty,
    Held(Arc<T>),
    Released,
}

pub(super) struct Retained<T> {
    reservation: Mutex<Slot<T>>,
    _caller: Arc<dyn Send + Sync>,
}

impl<T> Retained<T> {
    pub(super) fn new(caller: Arc<dyn Send + Sync>) -> Self {
        Self {
            reservation: Mutex::new(Slot::Empty),
            _caller: caller,
        }
    }

    pub(super) fn install(&self, reservation: Arc<T>) -> Result<(), Arc<T>> {
        let Ok(mut slot) = self.reservation.lock() else {
            return Err(reservation);
        };
        if !matches!(*slot, Slot::Empty) {
            return Err(reservation);
        }
        *slot = Slot::Held(reservation);
        Ok(())
    }

    // Only the adapter calls this after actual SQL close. Pointer identity must
    // match; a later/foreign reservation can never clear the retained owner.
    pub(super) fn release_closed(&self, expected: &Arc<T>) -> Result<(), ()> {
        let mut slot = self.reservation.lock().map_err(|_| ())?;
        if !matches!(&*slot, Slot::Held(actual) if Arc::ptr_eq(actual, expected)) {
            return Err(());
        }
        *slot = Slot::Released;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caller_and_actual_reservation_follow_retained_pause_owner() {
        let caller = Arc::new(());
        let caller_weak = Arc::downgrade(&caller);
        let reservation = Arc::new(());
        let reservation_weak = Arc::downgrade(&reservation);
        let held = Arc::new(Retained::new(caller.clone()));
        assert!(held.install(reservation.clone()).is_ok());
        let retained_by_pause = held.clone();
        drop(caller);
        drop(reservation);
        drop(held);
        assert!(caller_weak.upgrade().is_some());
        assert!(reservation_weak.upgrade().is_some());
        drop(retained_by_pause);
        assert!(caller_weak.upgrade().is_none());
        assert!(reservation_weak.upgrade().is_none());
    }

    #[test]
    fn verified_closed_slot_releases_before_pause_and_cannot_be_reused() {
        let held = Arc::new(Retained::new(Arc::new(())));
        let reservation = Arc::new(1usize);
        let observed = Arc::downgrade(&reservation);
        assert!(held.install(reservation.clone()).is_ok());
        assert!(held.release_closed(&Arc::new(1usize)).is_err());
        assert!(observed.upgrade().is_some());
        assert!(held.release_closed(&reservation).is_ok());
        assert!(held.install(Arc::new(2usize)).is_err());
        drop(reservation);
        assert!(observed.upgrade().is_none());
        // The pause may still own this caller holder after resources are gone.
        assert_eq!(Arc::strong_count(&held), 1);
    }

    #[test]
    fn duplicate_install_cannot_replace_the_actual_reservation() {
        let held = Retained::new(Arc::new(()));
        let first = Arc::new(1usize);
        let later = Arc::new(2usize);
        assert!(held.install(first.clone()).is_ok());
        let returned = held.install(later.clone()).unwrap_err();
        assert!(Arc::ptr_eq(&returned, &later));
        assert!(
            matches!(&*held.reservation.lock().unwrap(), Slot::Held(actual) if Arc::ptr_eq(actual, &first))
        );
    }
}
