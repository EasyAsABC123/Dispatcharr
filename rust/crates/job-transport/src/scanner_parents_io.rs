//! Actual same-process Scanner report/backend/publication authority.
use super::*;
use crate::scanner_files_protocol::{Kind, Message};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
const READ_PARENT:&str="local t=redis.call('TYPE',KEYS[1]).ok;if t=='none'then return{0,''}end;if t~='string'then return{-1,''}end;if redis.call('STRLEN',KEYS[1])>65536 then return{-2,''}end;return{1,redis.call('GET',KEYS[1])}";
#[derive(Clone)]
pub(crate) struct ScannerPublication {
    pub message: Message,
    pub digest: [u8; 32],
}
struct Parent {
    backend: [u8; 32],
    children: BTreeMap<Uuid, ScannerPublication>,
}
struct RegistryInner {
    jobs: Jobs,
    parents: std::sync::Mutex<BTreeMap<Uuid, Parent>>,
}
#[derive(Clone)]
pub struct ScannerParentRegistry {
    inner: Arc<RegistryInner>,
}
#[derive(Clone, Debug)]
pub struct ScannerRefreshCapability {
    scanner: Uuid,
    child: Uuid,
    source: i64,
    digest: [u8; 32],
}
impl ScannerRefreshCapability {
    pub fn scanner_id(&self) -> Uuid {
        self.scanner
    }
    pub fn child_id(&self) -> Uuid {
        self.child
    }
    pub fn source_id(&self) -> i64 {
        self.source
    }
    pub(crate) fn digest(&self) -> [u8; 32] {
        self.digest
    }
}
impl ScannerParentRegistry {
    pub fn new(jobs: Jobs) -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                jobs,
                parents: std::sync::Mutex::new(BTreeMap::new()),
            }),
        }
    }
    pub(crate) fn preflight(&self) -> Result<(), Error> {
        if self.inner.parents.lock().map_err(|_| Error::Backend)?.len() >= 32 {
            Err(Error::Capacity)
        } else {
            Ok(())
        }
    }
    async fn backend(
        &self,
        id: Uuid,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let lease = self.inner.jobs.lease(owner)?;
        let keys = [format!("celery-tasks:celery-task-meta-{id}")];
        let (reply, _held) = self
            .inner
            .jobs
            .inner
            .result
            .command_classified::<(i64, Vec<u8>), _>(READ_PARENT, &keys, &[], lease)
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => Ok(Some(reply.1)),
            _ => Err(Error::Corrupt),
        }
    }
    pub async fn head(&self, owner: Arc<dyn Send + Sync>) -> Result<Option<Vec<u8>>, Error> {
        let lease = self.inner.jobs.lease(owner)?;
        let keys = ["celery".to_owned()];
        let (reply, _held) = self
            .inner
            .jobs
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(PEEK_CREATE, &keys, &[], lease)
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => Ok(Some(reply.1)),
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) async fn record(
        &self,
        parent: Uuid,
        children: &[Uuid],
        publications: Vec<ScannerPublication>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if parent.is_nil() || children.len() > 32 || publications.len() > 32 {
            return Err(Error::Protocol);
        }
        let raw = self.backend(parent, owner).await?.ok_or(Error::Protocol)?;
        let value: serde_json::Value = serde_json::from_slice(&raw).map_err(|_| Error::Protocol)?;
        let expected: Vec<serde_json::Value> = children
            .iter()
            .map(|id| serde_json::json!([[id.to_string(), null], null]))
            .collect();
        if value["task_id"] != parent.to_string()
            || value["status"] != "SUCCESS"
            || value["result"] != serde_json::Value::Null
            || value["traceback"] != serde_json::Value::Null
            || value.get("parent_id").is_some()
            || value["children"] != serde_json::json!(expected)
        {
            return Err(Error::Protocol);
        }
        let mut actual = BTreeMap::new();
        for publication in publications {
            let m = publication.message;
            if m.parent != parent
                || !children.contains(&m.id)
                || actual.insert(m.id, publication).is_some()
            {
                return Err(Error::Protocol);
            }
        }
        if actual.is_empty() {
            return Ok(());
        }
        let mut parents = self.inner.parents.lock().map_err(|_| Error::Backend)?;
        if parents.len() >= 32 || parents.contains_key(&parent) {
            return Err(Error::Capacity);
        }
        parents.insert(
            parent,
            Parent {
                backend: Sha256::digest(&raw).into(),
                children: actual,
            },
        );
        Ok(())
    }
    async fn publication(
        &self,
        raw: &[u8],
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<ScannerPublication>, Error> {
        let message = match crate::scanner_files_protocol::parse_native(raw) {
            Ok(value) => value,
            Err(_) => return Ok(None),
        };
        let expected = {
            let parents = self.inner.parents.lock().map_err(|_| Error::Backend)?;
            parents.get(&message.parent).and_then(|parent| {
                parent
                    .children
                    .get(&message.id)
                    .map(|child| (parent.backend, child.clone()))
            })
        };
        let Some((digest, publication)) = expected else {
            return Ok(None);
        };
        if publication.message != message
            || publication.digest != <[u8; 32]>::from(Sha256::digest(raw))
        {
            return Ok(None);
        }
        let Some(backend) = self.backend(message.parent, owner).await? else {
            return Ok(None);
        };
        if digest != <[u8; 32]>::from(Sha256::digest(&backend)) {
            return Ok(None);
        }
        Ok(Some(publication))
    }
    pub async fn refresh_scope(
        &self,
        raw: &[u8],
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<ScannerRefreshCapability>, Error> {
        Ok(self
            .publication(raw, owner)
            .await?
            .filter(|v| v.message.kind == Kind::EpgRefresh)
            .map(|v| ScannerRefreshCapability {
                scanner: v.message.parent,
                child: v.message.id,
                source: v.message.object,
                digest: v.digest,
            }))
    }
    /// Optional Native report/backend provenance. Normal Source intake must use
    /// broker_m3u_scope instead; a lost/crashed parent is not refusal authority.
    pub async fn m3u_scope(
        &self,
        raw: &[u8],
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<crate::scanner_m3u_protocol::Scope>, Error> {
        Ok(self
            .publication(raw, owner)
            .await?
            .filter(|v| matches!(v.message.kind, Kind::M3uGroups | Kind::M3uSingle))
            .map(|v| {
                crate::scanner_m3u_protocol::Scope::from_registry(
                    v.message.parent,
                    v.message.object,
                    v.message.id,
                    v.digest,
                )
            }))
    }
    /// Actual authenticated broker observation followed by the consumer's real
    /// bounded account SQL preflight and exact candidate CAS. No process-local
    /// producer record or parent SUCCESS is required for surviving Source tasks.
    pub(crate) async fn broker_m3u_scope(
        &self,
        expected: &[u8],
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<crate::scanner_m3u_protocol::Scope>, Error> {
        let Some(actual) = self.head(owner).await? else {
            return Err(Error::ClaimRace);
        };
        if expected.is_empty() || actual != expected {
            return Err(Error::ClaimRace);
        }
        let message = match crate::scanner_m3u_protocol::parse(&actual) {
            Ok(message) => message,
            Err(_) => return Ok(None),
        };
        Ok(Some(crate::scanner_m3u_protocol::Scope::from_broker(
            message.parent,
            message.account,
            message.id,
            Sha256::digest(&actual).into(),
        )))
    }
    /// Only actual terminal handlers call this after successful backend settlement.
    pub fn retire_child(&self, child: Uuid) -> Result<(), Error> {
        let mut parents = self.inner.parents.lock().map_err(|_| Error::Backend)?;
        let parent = parents
            .iter()
            .find_map(|(id, v)| v.children.contains_key(&child).then_some(*id));
        if let Some(parent) = parent {
            if let Some(value) = parents.get_mut(&parent) {
                value.children.remove(&child);
                if value.children.is_empty() {
                    parents.remove(&parent);
                }
            }
        }
        Ok(())
    }
}
