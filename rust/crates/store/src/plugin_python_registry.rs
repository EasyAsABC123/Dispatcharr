//! Actual loaded-plugin callable witnesses for the shared native Jobs bridge.
//! No public task-name list mint, fixture-ID authority or namespace-prefix gate.
//! Original Dispatcharr/plugin authors retain full code/algorithm credit.
use super::plugin_execution_host::Identity;
use crate::StoreError;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
};
use uuid::Uuid;
type Owner = Arc<dyn Send + Sync>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HostRole {
    Actions,
    Tasks,
}
struct Host {
    identity: Identity,
    role: HostRole,
    witness: Uuid,
    records: BTreeMap<String, Arc<Callable>>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PluginTaskPolicy {
    pub acks_late: bool,
    pub acks_on_failure_or_timeout: bool,
    pub reject_on_worker_lost: bool,
    pub ignore_result: bool,
    pub track_started: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Callable {
    name: String,
    key: String,
    root: PathBuf,
    source: PathBuf,
    sha256: String,
    policy: PluginTaskPolicy,
}
/// A capability only actual registered live callables can supply. Jobs receives
/// this typed value; JSON caller names/parent UUIDs cannot construct one.
#[derive(Clone)]
pub struct PluginTaskCapability {
    record: Arc<Callable>,
    identity: Identity,
    witness: Uuid,
    registry: Weak<Mutex<BTreeMap<u32, Host>>>,
}
impl PluginTaskCapability {
    pub fn policy(&self) -> &PluginTaskPolicy {
        &self.record.policy
    }
    pub fn task_name(&self) -> &str {
        &self.record.name
    }
    pub fn plugin_key(&self) -> &str {
        &self.record.key
    }
    pub fn source_path(&self) -> &Path {
        &self.record.source
    }
    pub fn source_sha256(&self) -> &str {
        &self.record.sha256
    }
    pub fn host_pid(&self) -> u32 {
        self.identity.pid
    }
    pub fn live(&self) -> bool {
        if !live(&self.identity) {
            return false;
        }
        let Some(registry) = self.registry.upgrade() else {
            return false;
        };
        let Ok(hosts) = registry.lock() else {
            return false;
        };
        hosts.get(&self.identity.pid).is_some_and(|host| {
            same(&host.identity, &self.identity)
                && host.witness == self.witness
                && host
                    .records
                    .get(&self.record.name)
                    .is_some_and(|record| Arc::ptr_eq(record, &self.record))
        })
    }
    pub(crate) fn host_identity(&self) -> &Identity {
        &self.identity
    }
    pub(crate) fn witness(&self) -> Uuid {
        self.witness
    }
}
#[derive(Clone)]
pub struct PluginTaskRegistry {
    root: PathBuf,
    hosts: Arc<Mutex<BTreeMap<u32, Host>>>,
}
fn unavailable() -> StoreError {
    StoreError::Unsupported("Actual loaded plugin task witness unavailable")
}
pub(super) fn same(a: &Identity, b: &Identity) -> bool {
    a.pid == b.pid && a.start_generation == b.start_generation && a.pid_namespace == b.pid_namespace
}
pub(super) fn live(identity: &Identity) -> bool {
    let Ok(raw) = std::fs::read_to_string(format!("/proc/{}/stat", identity.pid)) else {
        return false;
    };
    let Some((_, tail)) = raw.rsplit_once(')') else {
        return false;
    };
    let fields: Vec<_> = tail.split_whitespace().collect();
    fields
        .get(19)
        .is_some_and(|v| *v == identity.start_generation)
        && std::fs::read_link(format!("/proc/{}/ns/pid", identity.pid))
            .is_ok_and(|v| v == identity.pid_namespace)
}
impl PluginTaskRegistry {
    /// Only the native compatibility assembly binds the real configured data root.
    /// This starts no interpreter, client, SQL pool or background task.
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            root,
            hosts: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
    /// Called by actual native child_started, never by a Source/HTTP JSON method.
    pub(crate) fn bind_host(&self, identity: Identity, role: HostRole) -> Result<(), StoreError> {
        if !live(&identity) {
            return Err(unavailable());
        }
        let raw = std::fs::read_to_string(format!("/proc/{}/stat", identity.pid))
            .map_err(|_| unavailable())?;
        let fields: Vec<_> = raw
            .rsplit_once(')')
            .ok_or_else(unavailable)?
            .1
            .split_whitespace()
            .collect();
        if fields.get(1).and_then(|s| s.parse::<u32>().ok()) != Some(std::process::id())
            || fields.get(2).and_then(|s| s.parse::<u32>().ok()) != Some(identity.pid)
        {
            return Err(unavailable());
        }
        let mut hosts = self.hosts.lock().map_err(|_| unavailable())?;
        if hosts.contains_key(&identity.pid) || hosts.len() >= 8 {
            return Err(unavailable());
        }
        hosts.insert(
            identity.pid,
            Host {
                identity,
                role,
                witness: Uuid::new_v4(),
                records: BTreeMap::new(),
            },
        );
        Ok(())
    }
    /// The callback adapter supplies its BOUND actual child identity; actual_pid
    /// fields in JSON merely have to agree. File I/O retains the real caller in
    /// an uncancelled physical blocking task until the entire bounded hash closes.
    pub(crate) async fn register(
        &self,
        identity: Identity,
        rows: Value,
        owner: Owner,
    ) -> Result<Uuid, StoreError> {
        {
            let hosts = self.hosts.lock().map_err(|_| unavailable())?;
            let host = hosts.get(&identity.pid).ok_or_else(unavailable)?;
            if !same(&host.identity, &identity) {
                return Err(unavailable());
            }
        }
        let configured = self.root.clone();
        let observed = identity.clone();
        let records = tokio::task::spawn_blocking(move || {
            let _owner = owner;
            if !live(&observed) {
                return Err(unavailable());
            }
            let root = configured.canonicalize().map_err(|_| unavailable())?;
            let rows = rows
                .as_array()
                .filter(|v| v.len() <= 1024)
                .ok_or_else(unavailable)?;
            let mut records = BTreeMap::new();
            let mut hashes = BTreeMap::<PathBuf, String>::new();
            let mut bytes = 0usize;
            for row in rows {
                if row["actual_pid"].as_u64() != Some(u64::from(observed.pid)) {
                    return Err(unavailable());
                }
                let text = |key: &str, max: usize| {
                    row[key]
                        .as_str()
                        .filter(|v| !v.is_empty() && v.len() <= max && !v.contains('\0'))
                        .map(str::to_owned)
                        .ok_or_else(unavailable)
                };
                let boolean = |key: &str| row["policy"][key].as_bool().ok_or_else(unavailable);
                let policy = PluginTaskPolicy {
                    acks_late: boolean("acks_late")?,
                    acks_on_failure_or_timeout: boolean("acks_on_failure_or_timeout")?,
                    reject_on_worker_lost: boolean("reject_on_worker_lost")?,
                    ignore_result: boolean("ignore_result")?,
                    track_started: boolean("track_started")?,
                };
                let name = text("name", 512)?;
                let key = text("plugin_key", 128)?;
                let module_root = PathBuf::from(text("root", 4096)?)
                    .canonicalize()
                    .map_err(|_| unavailable())?;
                let source = PathBuf::from(text("source", 4096)?)
                    .canonicalize()
                    .map_err(|_| unavailable())?;
                if module_root == root
                    || !module_root.starts_with(&root)
                    || !source.starts_with(&module_root)
                    || !source.is_file()
                {
                    return Err(unavailable());
                }
                let expected = text("sha256", 64)?;
                if expected.len() != 64
                    || !expected
                        .bytes()
                        .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
                {
                    return Err(unavailable());
                }
                let digest = if let Some(hash) = hashes.get(&source) {
                    hash.clone()
                } else {
                    if hashes.len() >= 128 {
                        return Err(unavailable());
                    }
                    let mut file = std::fs::File::open(&source).map_err(|_| unavailable())?;
                    let mut digest = Sha256::new();
                    let mut buffer = [0; 65536];
                    let mut count = 0usize;
                    loop {
                        let n = file.read(&mut buffer).map_err(|_| unavailable())?;
                        if n == 0 {
                            break;
                        }
                        count += n;
                        bytes += n;
                        if count > 1048576 || bytes > 16777216 {
                            return Err(unavailable());
                        }
                        digest.update(&buffer[..n]);
                    }
                    let hash = format!("{:x}", digest.finalize());
                    hashes.insert(source.clone(), hash.clone());
                    hash
                };
                if digest != expected {
                    return Err(unavailable());
                }
                let record = Arc::new(Callable {
                    name: name.clone(),
                    key,
                    root: module_root,
                    source,
                    sha256: digest,
                    policy,
                });
                if records.insert(name, record).is_some() {
                    return Err(unavailable());
                }
            }
            Ok::<_, StoreError>(records)
        })
        .await
        .map_err(|_| unavailable())??;
        let mut hosts = self.hosts.lock().map_err(|_| unavailable())?;
        let host = hosts.get_mut(&identity.pid).ok_or_else(unavailable)?;
        if !same(&host.identity, &identity) || !live(&identity) {
            return Err(unavailable());
        }
        if host.records != records {
            host.witness = Uuid::new_v4();
            host.records = records;
        }
        Ok(host.witness)
    }
    pub(crate) fn capability(
        &self,
        identity: &Identity,
        witness: Uuid,
        name: &str,
    ) -> Result<PluginTaskCapability, StoreError> {
        let hosts = self.hosts.lock().map_err(|_| unavailable())?;
        let host = hosts.get(&identity.pid).ok_or_else(unavailable)?;
        if !same(&host.identity, identity) || host.witness != witness || !live(identity) {
            return Err(unavailable());
        }
        Ok(PluginTaskCapability {
            record: host.records.get(name).ok_or_else(unavailable)?.clone(),
            identity: identity.clone(),
            witness,
            registry: Arc::downgrade(&self.hosts),
        })
    }
    /// Exact runtime-name classifier input; no starts_with(plugin_namespace).
    pub fn registered_names(&self) -> Result<BTreeSet<String>, StoreError> {
        let hosts = self.hosts.lock().map_err(|_| unavailable())?;
        Ok(hosts
            .values()
            .filter(|h| live(&h.identity))
            .flat_map(|h| h.records.keys().cloned())
            .collect())
    }
    pub fn has_registered_name(&self, name: &str) -> Result<bool, StoreError> {
        let hosts = self.hosts.lock().map_err(|_| unavailable())?;
        Ok(hosts
            .values()
            .any(|host| host.records.contains_key(name) && live(&host.identity)))
    }
    /// Actual live module witness for envelope admission; a task actor supplies
    /// its own independently registered witness again before execution.
    pub(crate) fn envelope_capability(
        &self,
        name: &str,
    ) -> Result<PluginTaskCapability, StoreError> {
        let hosts = self.hosts.lock().map_err(|_| unavailable())?;
        hosts
            .values()
            .filter(|h| live(&h.identity))
            .find_map(|h| {
                h.records.get(name).map(|record| PluginTaskCapability {
                    record: record.clone(),
                    identity: h.identity.clone(),
                    witness: h.witness,
                    registry: Arc::downgrade(&self.hosts),
                })
            })
            .ok_or_else(unavailable)
    }
    pub(crate) fn task_capability(
        &self,
        identity: &Identity,
        name: &str,
    ) -> Result<PluginTaskCapability, StoreError> {
        let witness = {
            let hosts = self.hosts.lock().map_err(|_| unavailable())?;
            let host = hosts
                .get(&identity.pid)
                .filter(|h| h.role == HostRole::Tasks && same(&h.identity, identity))
                .ok_or_else(unavailable)?;
            host.witness
        };
        self.capability(identity, witness, name)
    }
    pub(crate) fn remove_host(&self, identity: &Identity) -> Result<(), StoreError> {
        let mut hosts = self.hosts.lock().map_err(|_| unavailable())?;
        if hosts
            .get(&identity.pid)
            .is_some_and(|h| same(&h.identity, identity))
        {
            hosts.remove(&identity.pid);
        }
        Ok(())
    }
}
