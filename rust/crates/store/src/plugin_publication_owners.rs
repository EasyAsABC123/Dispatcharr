//! Bounded actual-publication provenance, never parent-ID/fixture authority.
//! Pending intent is NOT a writer/claim grant. Only a settled native PUBLISH of
//! these exact bytes enables a causal writer handoff to a distinct pool lease.
//! Delayed jobs retain weak writers/parents, so they cannot pin Auth4 forever.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::plugin_execution_host::Invocation;
use crate::task_sql_session::TaskSqlWriterWitness;
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
        let key = key(id, raw);
        let mut records = self.records.lock().map_err(|_| unavailable())?;
        records.retain(|_, record| !record.settled || record.parent.strong_count() != 0);
        if records.len() >= self.maximum || records.contains_key(&key) {
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
                parent: Arc::downgrade(&scope.owner),
                writer: session.publication_writer(),
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
                if let Some(session) = record
                    .writer
                    .try_borrow(auth.clone(), Arc::new((owner.clone(), parent)))
                    .await?
                {
                    return Ok(session);
                }
            }
        }
        auth.borrow_task_sql_session(owner).await
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
