//! Bounded actual-publication provenance, never parent-ID/fixture authority.
//! Pending intent is NOT a writer/claim grant. Only a settled native PUBLISH of
//! these exact bytes enables a causal writer handoff to a distinct pool lease.
//! Delayed jobs retain weak writers/parents, so they cannot pin Auth4 forever.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::plugin_execution_host::Invocation;
use crate::task_sql_session::{TaskSqlPublicationWitness, TaskSqlWriterWitness};
use crate::{AuthService, StoreError, TaskSqlSession};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};
use uuid::Uuid;
type Owner = Arc<dyn Send + Sync>;
type Key = (Uuid, [u8; 32]);
#[derive(Clone)]
struct Record {
    nonce: Uuid,
    settled: bool,
    // Hash of the complete actual published JSON value, for a broker-stored
    // tuple which loses only JSON whitespace/key order. Never matches by ID alone.
    stored_identity: Key,
    parent: Weak<dyn Send + Sync>,
    writer: TaskSqlWriterWitness,
}
#[derive(Clone)]
pub struct PluginPublicationOwners {
    records: Arc<Mutex<BTreeMap<Key, Record>>>,
    maximum: usize,
}
pub(crate) struct PublicationIntent {
    key: Key,
    nonce: Uuid,
    records: Weak<Mutex<BTreeMap<Key, Record>>>,
    settled: bool,
}
fn unavailable() -> StoreError {
    StoreError::Unsupported("Actual plugin publication owner unavailable")
}
fn key(id: Uuid, raw: &[u8]) -> Key {
    (id, Sha256::digest(raw).into())
}
impl Drop for PublicationIntent {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        if let Some(records) = self.records.upgrade() {
            if let Ok(mut records) = records.lock() {
                if records
                    .get(&self.key)
                    .is_some_and(|r| r.nonce == self.nonce && !r.settled)
                {
                    records.remove(&self.key);
                }
            }
        }
    }
}
fn stored_identity(id: Uuid, raw: &[u8]) -> Result<Key, StoreError> {
    if raw.len() > 65536 {
        return Err(unavailable());
    }
    fn sorted(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => {
                let mut entries: Vec<_> = map.into_iter().collect();
                entries.sort_by(|a, b| a.0.cmp(&b.0));
                serde_json::Value::Object(
                    entries.into_iter().map(|(k, v)| (k, sorted(v))).collect(),
                )
            }
            serde_json::Value::Array(items) => {
                serde_json::Value::Array(items.into_iter().map(sorted).collect())
            }
            other => other,
        }
    }
    let frame: serde_json::Value = serde_json::from_slice(raw).map_err(|_| unavailable())?;
    let canonical = serde_json::to_vec(&sorted(frame)).map_err(|_| unavailable())?;
    Ok(key(id, &canonical))
}
impl PluginPublicationOwners {
    /// Main supplies its actual queue capacity plus EXISTING global6/ETA16/
    /// publisher-operation bounds, not an independent work/admission semaphore.
    pub fn prepare(maximum: usize) -> Result<Self, StoreError> {
        if maximum == 0 || maximum > 131072 {
            return Err(unavailable());
        }
        Ok(Self {
            records: Arc::new(Mutex::new(BTreeMap::new())),
            maximum,
        })
    }
    pub(crate) fn before_publish(
        &self,
        id: Uuid,
        raw: &[u8],
        scope: &Invocation,
    ) -> Result<Option<PublicationIntent>, StoreError> {
        let Some(session) = scope.sql.as_ref() else {
            return Ok(None);
        };
        let provenance = session.publication_provenance(&scope.owner);
        let key = key(id, raw);
        let stored_identity = stored_identity(id, raw)?;
        let mut records = self.records.lock().map_err(|_| unavailable())?;
        records.retain(|_, record| !record.settled || record.parent.strong_count() != 0);
        if records.len() >= self.maximum
            || records.contains_key(&key)
            || records
                .values()
                .any(|v| v.stored_identity == stored_identity)
        {
            return Err(StoreError::StatusBody(
                503,
                serde_json::json!({"detail":"Plugin publication ownership capacity unavailable."}),
            ));
        }
        let nonce = Uuid::new_v4();
        records.insert(
            key,
            Record {
                nonce,
                settled: false,
                stored_identity,
                parent: provenance.parent,
                writer: provenance.writer,
            },
        );
        Ok(Some(PublicationIntent {
            key,
            nonce,
            records: Arc::downgrade(&self.records),
            settled: false,
        }))
    }
    pub(crate) fn settled(
        &self,
        mut intent: PublicationIntent,
        actual_id: Uuid,
    ) -> Result<(), StoreError> {
        if intent.key.0 != actual_id {
            return Err(unavailable());
        }
        let mut records = self.records.lock().map_err(|_| unavailable())?;
        let record = records
            .get_mut(&intent.key)
            .filter(|r| r.nonce == intent.nonce && !r.settled)
            .ok_or_else(unavailable)?;
        record.settled = true;
        intent.settled = true;
        Ok(())
    }
    pub(crate) async fn admit(
        &self,
        id: Uuid,
        raw: &[u8],
        auth: AuthService,
        owner: Owner,
    ) -> Result<TaskSqlSession, StoreError> {
        let record = self
            .records
            .lock()
            .map_err(|_| unavailable())?
            .get(&key(id, raw))
            .cloned();
        if let Some(record) = record {
            if !record.settled {
                return Err(StoreError::StatusBody(
                    503,
                    serde_json::json!({"detail":"Plugin publication has not physically settled."}),
                ));
            }
            if let Some(parent) = record.parent.upgrade() {
                let provenance = TaskSqlPublicationWitness {
                    writer: record.writer.clone(),
                    parent: Arc::downgrade(&parent),
                };
                let session = match record
                    .writer
                    .try_borrow(auth.clone(), Arc::new((owner.clone(), parent)))
                    .await?
                {
                    Some(session) => session,
                    // Busy or non-idle root: remaining ORIGINAL Auth admission
                    // may execute independently, never another borrowed writer.
                    None => auth.borrow_task_sql_session(owner).await?,
                };
                session.inherit_publication_provenance(provenance)?;
                return Ok(session);
            }
        }
        auth.borrow_task_sql_session(owner).await
    }
    /// Transfer only after actual owner-checked requeue/warm restoration. A
    /// changed raw JSON supplied by Python cannot mint a causal writer witness.
    pub(crate) fn restored(&self, id: Uuid, raw: &[u8], restored: &[u8]) -> Result<(), StoreError> {
        let mut records = self.records.lock().map_err(|_| unavailable())?;
        let original = key(id, raw);
        let new = key(id, restored);
        if original == new {
            return Ok(());
        }
        if records.contains_key(&new) {
            return Err(unavailable());
        }
        if let Some(mut record) = records.remove(&original) {
            record.stored_identity = stored_identity(id, restored)?;
            records.insert(new, record);
        }
        Ok(())
    }
    /// Called only after an owner-checked visibility CAS actually restored a
    /// typed receipt. Match its COMPLETE stored frame against the previously
    /// settled publication digest; no matching by caller-supplied task ID alone.
    pub(crate) fn restored_stored(
        &self,
        id: Uuid,
        stored: &[u8],
        restored: &[u8],
    ) -> Result<(), StoreError> {
        let identity = stored_identity(id, stored)?;
        let next_identity = stored_identity(id, restored)?;
        let new = key(id, restored);
        let mut records = self.records.lock().map_err(|_| unavailable())?;
        let matching: Vec<_> = records
            .iter()
            .filter(|(_, r)| r.settled && r.stored_identity == identity)
            .map(|(k, _)| *k)
            .collect();
        match matching.as_slice() {
            [] => Ok(()),
            [old] => {
                if *old != new && records.contains_key(&new) {
                    return Err(unavailable());
                }
                let mut record = records.remove(old).ok_or_else(unavailable)?;
                record.stored_identity = next_identity;
                records.insert(new, record);
                Ok(())
            }
            _ => Err(unavailable()),
        }
    }
    /// Called only after this exact raw head was actually claimed/reserved, never
    /// on selection/preflight (which can race with a foreign head replacement).
    pub(crate) fn consumed(&self, id: Uuid, raw: &[u8]) -> Result<(), StoreError> {
        self.records
            .lock()
            .map_err(|_| unavailable())?
            .remove(&key(id, raw));
        Ok(())
    }
}

#[cfg(test)]
mod stored_frame_identity_tests {
    use super::*;
    #[test]
    fn broker_tuple_json_respelling_preserves_complete_frame_identity_only() {
        let id = Uuid::new_v4();
        let original=br#" {"headers":{"id":"actual-id","task":"exact-task"},"body":"W1tdLHt9LHt9XQ==","properties":{"delivery_tag":"actual-tag"}} "#;
        let reordered=br#"{"properties":{"delivery_tag":"actual-tag"},"body":"W1tdLHt9LHt9XQ==","headers":{"task":"exact-task","id":"actual-id"}}"#;
        assert_ne!(key(id, original), key(id, reordered));
        assert_eq!(
            stored_identity(id, original).unwrap(),
            stored_identity(id, reordered).unwrap()
        );
        for (field, value) in [
            ("body", serde_json::json!("different-actual-body")),
            (
                "headers",
                serde_json::json!({"id":"actual-id","task":"other-task"}),
            ),
            (
                "properties",
                serde_json::json!({"delivery_tag":"other-tag"}),
            ),
        ] {
            let mut changed: serde_json::Value = serde_json::from_slice(original).unwrap();
            changed[field] = value;
            assert_ne!(
                stored_identity(id, original).unwrap(),
                stored_identity(id, &serde_json::to_vec(&changed).unwrap()).unwrap()
            );
        }
        assert_ne!(
            stored_identity(id, original).unwrap(),
            stored_identity(Uuid::new_v4(), original).unwrap()
        );
    }
}
