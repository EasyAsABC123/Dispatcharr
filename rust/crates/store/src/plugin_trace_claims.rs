//! One-use native execution epochs bound to the actually selected task host.
//! Only Native runtime after its real broker ACK registers an epoch. Python
//! can validate one; it cannot mint one through any callback/HTTP method.
use super::{plugin_execution_host::Identity, plugin_python_registry::PluginTaskCapability};
use crate::StoreError;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};
use uuid::Uuid;
#[derive(Clone)]
pub(crate) struct PluginTraceClaims {
    inner: Arc<Mutex<BTreeMap<Uuid, Record>>>,
    maximum: usize,
}
struct Record {
    id: Uuid,
    identity: Identity,
    witness: Uuid,
    name: String,
    used: bool,
}
pub(crate) struct TraceEpoch {
    pub token: Uuid,
    inner: Weak<Mutex<BTreeMap<Uuid, Record>>>,
}
fn unavailable() -> StoreError {
    StoreError::Unsupported("Actual registered plugin trace epoch unavailable")
}
impl Drop for TraceEpoch {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            if let Ok(mut inner) = inner.lock() {
                inner.remove(&self.token);
            }
        }
    }
}
impl PluginTraceClaims {
    pub(crate) fn prepare(maximum: usize) -> Result<Self, StoreError> {
        if maximum == 0 || maximum > 6 {
            return Err(unavailable());
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(BTreeMap::new())),
            maximum,
        })
    }
    pub(crate) fn after_actual_ack(
        &self,
        id: Uuid,
        capability: &PluginTaskCapability,
    ) -> Result<TraceEpoch, StoreError> {
        if !capability.live() {
            return Err(unavailable());
        }
        let mut records = self.inner.lock().map_err(|_| unavailable())?;
        if records.len() >= self.maximum {
            return Err(unavailable());
        }
        let token = Uuid::new_v4();
        records.insert(
            token,
            Record {
                id,
                identity: capability.host_identity().clone(),
                witness: capability.witness(),
                name: capability.task_name().to_owned(),
                used: false,
            },
        );
        Ok(TraceEpoch {
            token,
            inner: Arc::downgrade(&self.inner),
        })
    }
    pub(crate) fn validate(
        &self,
        token: Uuid,
        id: Uuid,
        identity: &Identity,
        capability: &PluginTaskCapability,
    ) -> Result<bool, StoreError> {
        if !capability.live() {
            return Err(unavailable());
        }
        let mut records = self.inner.lock().map_err(|_| unavailable())?;
        let record = records.get_mut(&token).ok_or_else(unavailable)?;
        let matched = !record.used
            && record.id == id
            && record.identity.pid == identity.pid
            && record.identity.start_generation == identity.start_generation
            && record.identity.pid_namespace == identity.pid_namespace
            && record.witness == capability.witness()
            && record.name == capability.task_name();
        if matched {
            record.used = true;
        }
        Ok(matched)
    }
}
