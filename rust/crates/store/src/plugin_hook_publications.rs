//! Actual settled hook publications, separate from mutable Context metadata.
use super::{plugin_execution_host::Error, plugin_hook_context::PluginHookChildReceipt};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex, Weak};
use uuid::Uuid;
type Owner = Arc<dyn Send + Sync>;
struct Active {
    token: Uuid,
    parent: String,
    owner: Weak<dyn Send + Sync>,
    pending: usize,
    receipts: Vec<PluginHookChildReceipt>,
}
#[derive(Clone, Default)]
pub(crate) struct HookPublications {
    active: Arc<Mutex<Option<Active>>>,
}
pub(crate) struct HookRegistration {
    token: Uuid,
    tracker: HookPublications,
}
pub(crate) struct HookPublicationIntent {
    token: Uuid,
    receipt: Option<PluginHookChildReceipt>,
    tracker: HookPublications,
}
impl HookPublications {
    pub(crate) fn begin(&self, parent: String, owner: &Owner) -> Result<HookRegistration, Error> {
        let mut a = self.active.lock().map_err(|_| Error::Protocol)?;
        if a.is_some() {
            return Err(Error::Capacity);
        }
        let token = Uuid::new_v4();
        *a = Some(Active {
            token,
            parent,
            owner: Arc::downgrade(owner),
            pending: 0,
            receipts: Vec::new(),
        });
        Ok(HookRegistration {
            token,
            tracker: self.clone(),
        })
    }
    pub(crate) fn before(
        &self,
        raw: &[u8],
        owner: &Owner,
    ) -> Result<Option<HookPublicationIntent>, Error> {
        let mut a = self.active.lock().map_err(|_| Error::Protocol)?;
        let Some(a) = a.as_mut() else { return Ok(None) };
        if !a.owner.upgrade().is_some_and(|p| Arc::ptr_eq(&p, owner)) {
            return Ok(None);
        }
        if a.pending + a.receipts.len() >= 1024 {
            return Err(Error::Capacity);
        }
        let frame: Value = serde_json::from_slice(raw).map_err(|_| Error::Protocol)?;
        let id = frame["headers"]["id"]
            .as_str()
            .ok_or(Error::Protocol)?
            .to_owned();
        let name = frame["headers"]["task"]
            .as_str()
            .ok_or(Error::Protocol)?
            .to_owned();
        a.pending += 1;
        Ok(Some(HookPublicationIntent {
            token: a.token,
            receipt: Some(PluginHookChildReceipt {
                id,
                name,
                publisher: a.parent.clone(),
                raw_sha256: format!("{:x}", Sha256::digest(raw)),
            }),
            tracker: self.clone(),
        }))
    }
}
impl HookPublicationIntent {
    pub(crate) fn settled(mut self, id: &str) -> Result<(), Error> {
        let receipt = self.receipt.take().ok_or(Error::Protocol)?;
        if receipt.id != id {
            return Err(Error::Protocol);
        }
        let mut a = self.tracker.active.lock().map_err(|_| Error::Protocol)?;
        let a = a
            .as_mut()
            .filter(|a| a.token == self.token)
            .ok_or(Error::Protocol)?;
        a.pending = a.pending.checked_sub(1).ok_or(Error::Protocol)?;
        a.receipts.push(receipt);
        Ok(())
    }
}
impl Drop for HookPublicationIntent {
    fn drop(&mut self) {
        if self.receipt.is_some() {
            if let Ok(mut a) = self.tracker.active.lock() {
                if let Some(a) = a.as_mut().filter(|a| a.token == self.token) {
                    a.pending = a.pending.saturating_sub(1);
                }
            }
        }
    }
}
impl HookRegistration {
    pub(crate) fn receipts(&self) -> Result<Vec<PluginHookChildReceipt>, Error> {
        let a = self.tracker.active.lock().map_err(|_| Error::Protocol)?;
        let a = a
            .as_ref()
            .filter(|a| a.token == self.token)
            .ok_or(Error::Protocol)?;
        if a.pending != 0 {
            return Err(Error::Callback);
        }
        Ok(a.receipts.clone())
    }
}
impl Drop for HookRegistration {
    fn drop(&mut self) {
        if let Ok(mut a) = self.tracker.active.lock() {
            if a.as_ref().is_some_and(|a| a.token == self.token) {
                *a = None;
            }
        }
    }
}
