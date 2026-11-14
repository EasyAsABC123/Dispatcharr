//! Normal named/UUID Celery revoked LimitedSet, shared by actual control receiver
//! and admitted consumers. Original Celery authors retain algorithm credit.
//! Membership does not age entries; add-at-capacity performs Source expiry purge.
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyError {
    Invalid,
    Capacity,
    Poisoned,
}
struct Entries {
    values: HashMap<String, Instant>,
    order: BTreeMap<(Instant, String), ()>,
}
impl Entries {
    fn add_at(&mut self, key: String, now: Instant, purge_now: Instant) {
        if let Some(old) = self.values.insert(key.clone(), now) {
            self.order.remove(&(old, key.clone()));
        }
        self.order.insert((now, key), ());
        if self.values.len() >= 50_000 {
            while self.values.len() > 50_000 {
                self.pop();
            }
            while self.order.first_key_value().is_some_and(|((at, _), _)| {
                purge_now.saturating_duration_since(*at) >= Duration::from_secs(10_800)
            }) {
                self.pop();
            }
        }
    }
    fn pop(&mut self) {
        if let Some(((at, key), ())) = self.order.pop_first() {
            if self.values.get(&key) == Some(&at) {
                self.values.remove(&key);
            }
        }
    }
}
/// One worker-local set, including actual named keys. It does not remove queues,
/// write results, stop active media or claim that terminate=True is implemented.
#[derive(Clone)]
pub struct Revoked {
    inner: Arc<Mutex<Entries>>,
}
impl Default for Revoked {
    fn default() -> Self {
        Self::new()
    }
}
impl Revoked {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Entries {
                values: HashMap::new(),
                order: BTreeMap::new(),
            })),
        }
    }
    pub fn apply(
        &self,
        task_keys: &[String],
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), ApplyError> {
        let _owner = owner;
        if task_keys.len() > 50_000 {
            return Err(ApplyError::Capacity);
        }
        // Validate the entire command before changing any member. Named periodic task
        // identities are legal Celery keys too, even when no corresponding queue task exists.
        if task_keys.iter().any(|key| !valid(key)) {
            return Err(ApplyError::Invalid);
        }
        let mut entries = self.inner.lock().map_err(|_| ApplyError::Poisoned)?;
        let now = Instant::now();
        for key in task_keys {
            entries.add_at(key.clone(), now, Instant::now());
        }
        Ok(())
    }
    pub fn contains(&self, task_key: &str) -> Result<bool, ApplyError> {
        if !valid(task_key) {
            return Err(ApplyError::Invalid);
        }
        Ok(self
            .inner
            .lock()
            .map_err(|_| ApplyError::Poisoned)?
            .values
            .contains_key(task_key))
    }
}
fn valid(key: &str) -> bool {
    !key.is_empty() && key.len() <= 255 && key.bytes().all(|v| v.is_ascii_graphic())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn named_identity_is_not_uuid_alias_and_invalid_batch_is_atomic() {
        let set = Revoked::new();
        let uuid = uuid::Uuid::new_v4().to_string();
        set.apply(
            &[
                uuid.clone(),
                "dvr-recover-37".into(),
                "dvr-recording-37".into(),
            ],
            Arc::new(()),
        )
        .unwrap();
        for key in [&uuid, "dvr-recover-37", "dvr-recording-37"] {
            assert!(set.contains(key).unwrap());
        }
        assert!(!set.contains("dvr-recover-38").unwrap());
        assert_eq!(
            set.apply(&["new-task".into(), "bad\nkey".into()], Arc::new(())),
            Err(ApplyError::Invalid)
        );
        assert!(!set.contains("new-task").unwrap());
    }
    #[test]
    fn membership_does_not_expire_and_add_at_capacity_purges_source_expiry() {
        let base = Instant::now();
        let mut entries = Entries {
            values: HashMap::new(),
            order: BTreeMap::new(),
        };
        entries.add_at(
            "dvr-recover-1".into(),
            base,
            base + Duration::from_secs(10_801),
        );
        assert!(entries.values.contains_key("dvr-recover-1"));
        for n in 2..=50_001 {
            entries.add_at(
                format!("dvr-recover-{n}"),
                base + Duration::from_millis(n),
                base + Duration::from_secs(10_801),
            );
        }
        assert!(!entries.values.contains_key("dvr-recover-1"));
        assert!(entries.values.len() <= 50_000);
        assert_eq!(entries.values.len(), entries.order.len());
    }
}
