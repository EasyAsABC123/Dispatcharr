//! Split the existing one-MiB response/scratch quote; never add an allowance.
use crate::StoreError;
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
const LIMIT: usize = 1024 * 1024;
#[derive(Clone)]
pub(crate) struct Scratch(Arc<AtomicUsize>);
pub(crate) struct Charge {
    used: Arc<AtomicUsize>,
    bytes: usize,
}
impl Scratch {
    pub(crate) fn new() -> Self {
        Self(Arc::new(AtomicUsize::new(0)))
    }
    pub(crate) fn used(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
    pub(crate) fn charge(&self, bytes: usize) -> Result<Arc<Charge>, StoreError> {
        let mut old = self.used();
        loop {
            let Some(next) = old.checked_add(bytes).filter(|n| *n <= LIMIT) else {
                return Err(StoreError::StatusBody(
                    503,
                    json!({"detail":"CoreSettings resource capacity is unavailable."}),
                ));
            };
            match self
                .0
                .compare_exchange_weak(old, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return Ok(Arc::new(Charge {
                        used: self.0.clone(),
                        bytes,
                    }))
                }
                Err(current) => old = current,
            }
        }
    }
}
impl Drop for Charge {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn clones_hold_original_quote_until_last_physical_owner() {
        let quote = Scratch::new();
        let first = quote.charge(LIMIT - 128).unwrap();
        let clone = first.clone();
        assert!(quote.charge(129).is_err());
        let frame = quote.charge(128).unwrap();
        drop(first);
        assert_eq!(quote.used(), LIMIT);
        drop(clone);
        assert_eq!(quote.used(), 128);
        drop(frame);
        assert_eq!(quote.used(), 0);
        assert!(quote.charge(usize::MAX).is_err());
    }
}
