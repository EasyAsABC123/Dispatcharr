//! Cleanup-only Celery expiry/revoked state. Original Celery authors retain credit.
//! Pinned LimitedSet defaults: 50000 members,10800s, membership does not purge;
//! add at capacity purges oldest and expired members. No persisted worker state.
use crate::protocol::ProtocolError;
use chrono::{DateTime, Datelike, Utc};
use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, Instant},
};
use uuid::Uuid;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CleanupExpiration {
    pub deadline: DateTime<Utc>,
    pub transport_millis: u64,
}
impl CleanupExpiration {
    pub fn new(deadline: DateTime<Utc>, transport_millis: u64) -> Result<Self, ProtocolError> {
        if !(1970..=9999).contains(&deadline.year())
            || !deadline.timestamp_subsec_nanos().is_multiple_of(1000)
            || transport_millis > 43_200_000
        {
            return Err(ProtocolError::InvalidArgument);
        }
        Ok(Self {
            deadline,
            transport_millis,
        })
    }
    pub(crate) fn expired(&self, now: DateTime<Utc>) -> bool {
        now > self.deadline
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Revocation {
    Expired,
    Revoked,
}
impl Revocation {
    pub fn reason(self) -> &'static str {
        match self {
            Self::Expired => "expired",
            Self::Revoked => "revoked",
        }
    }
}
pub(crate) struct RevokedSet {
    entries: HashMap<Uuid, Instant>,
    order: BTreeMap<(Instant, Uuid), ()>,
}
impl RevokedSet {
    pub(crate) fn new() -> Self {
        Self {
            entries: HashMap::new(),
            order: BTreeMap::new(),
        }
    }
    pub(crate) fn contains(&self, id: Uuid) -> bool {
        self.entries.contains_key(&id)
    }
    pub(crate) fn add(&mut self, id: Uuid) {
        self.add_at(id, Instant::now(), Instant::now());
    }
    fn add_at(&mut self, id: Uuid, now: Instant, purge_now: Instant) {
        if let Some(old) = self.entries.insert(id, now) {
            self.order.remove(&(old, id));
        }
        self.order.insert((now, id), ());
        if self.entries.len() >= 50_000 {
            while self.entries.len() > 50_000 {
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
        if let Some(((at, id), ())) = self.order.pop_first() {
            if self.entries.get(&id) == Some(&at) {
                self.entries.remove(&id);
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_expiry_boundary_and_bounded_input() {
        let at = DateTime::parse_from_rfc3339("2026-11-11T04:00:00+00:00")
            .unwrap()
            .to_utc();
        let value = CleanupExpiration::new(at, 43_200_000).unwrap();
        assert!(!value.expired(at));
        assert!(value.expired(at + chrono::Duration::microseconds(1)));
        assert!(CleanupExpiration::new(at, 43_200_001).is_err());
        assert!(CleanupExpiration::new(at + chrono::Duration::nanoseconds(1), 0).is_err());
    }
    #[test]
    fn source_limitedset_no_membership_expiry_and_capacity_purge() {
        let base = Instant::now();
        let mut set = RevokedSet::new();
        let first = Uuid::from_u128(1);
        set.add_at(first, base, base);
        assert!(set.contains(first));
        for n in 2..50_001u128 {
            set.add_at(
                Uuid::from_u128(n),
                base + Duration::from_millis(n as u64),
                base + Duration::from_secs(10_801),
            );
        }
        assert!(!set.contains(first));
        assert!(set.entries.len() <= 50_000);
        assert_eq!(set.entries.len(), set.order.len());
    }
}
