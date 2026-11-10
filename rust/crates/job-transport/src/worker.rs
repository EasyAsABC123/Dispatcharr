//! Fixture-only CreateBackup execution, with Source early-ACK and SUCCESS dicts.
//! Original Dispatcharr authors retain application/handler credit and AGPL license.
//! This is not a generic Celery consumer, production authority or public202 route.
use crate::{
    protocol,
    transport::{Error as TransportError, Jobs},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use dispatcharr_backups::{self as backups, Cancellation, Limits, PgConfig, Runtime, SourceClock};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Closed,
    Capacity,
    UnsupportedEnvelope,
    Transport(TransportError),
    HandlerPanic,
    DrainTimeout,
    InvalidResult,
    Configuration,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Completed {
    pub filename: String,
    pub size: u64,
}
/// The handler's returned failure is a Source SUCCESS/failure dictionary. Message
/// parity belongs to the specific handler fixture, not to this transport wrapper.
pub trait CreateHandler: Send + Sync + 'static {
    fn create(&self) -> Result<Completed, String>;
}
/// Actual bounded PostgreSQL/archive component. UTC/timezone configuration is
/// supplied explicitly by the fixture; no host-local timezone assumption.
pub struct ArchiveHandler {
    config: PgConfig,
    runtime: Runtime,
    backup_root: PathBuf,
    scratch_root: PathBuf,
    clock: Arc<dyn SourceClock + Send + Sync>,
    limits: Limits,
}
impl ArchiveHandler {
    pub fn new_fixture(
        config: PgConfig,
        runtime: Runtime,
        backup_root: PathBuf,
        scratch_root: PathBuf,
        clock: Arc<dyn SourceClock + Send + Sync>,
        limits: Limits,
    ) -> Result<Self, Error> {
        if std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref() != Ok("playback-fixture-only")
            || std::env::var("POSTGRES_DB").as_deref() != Ok(config.database.as_str())
            || !config.database.starts_with("rust_probe_backup_")
            || config.database.len() > 63
            || !config
                .database
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            || config.password.as_deref() != Some("playback-fixture-only")
            || !matches!(config.host.as_str(), "localhost" | "127.0.0.1" | "::1")
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            config,
            runtime,
            backup_root,
            scratch_root,
            clock,
            limits,
        })
    }
}
impl CreateHandler for ArchiveHandler {
    fn create(&self) -> Result<Completed, String> {
        let path = backups::create_backup(
            &self.config,
            &self.runtime,
            &self.backup_root,
            &self.scratch_root,
            self.clock.as_ref(),
            &self.limits,
            &Cancellation::default(),
        )
        // Existing archive errors deliberately omit child stderr. These
        // messages are bounded native diagnostics, NOT Source-error parity.
        .map_err(|e| format!("Native archive error: {e:?}"))?;
        let filename = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or("Native archive filename error")?
            .to_owned();
        let size = path
            .metadata()
            .map_err(|_| "Native archive metadata error")?
            .len();
        Ok(Completed { filename, size })
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Run {
    Empty,
    Completed(Uuid),
    ResultAlreadySucceeded(Uuid),
}
struct Owner {
    _slot: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
}
struct Inner {
    jobs: Jobs,
    handler: Arc<dyn CreateHandler>,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
}
#[derive(Clone)]
pub struct Worker {
    inner: Arc<Inner>,
}
impl Worker {
    /// Jobs itself has no production constructor. One physical task at a time;
    /// unsupported queue heads are retained, never acknowledged or reordered.
    pub fn new_fixture(jobs: Jobs, handler: Arc<dyn CreateHandler>) -> Self {
        Self {
            inner: Arc::new(Inner {
                jobs,
                handler,
                slots: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
            }),
        }
    }
    pub fn available_slots(&self) -> usize {
        self.inner.slots.available_permits()
    }
    pub async fn run_once(&self, caller: Arc<dyn Send + Sync>) -> Result<Run, Error> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let slot = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let owner = Arc::new(Owner {
            _slot: slot,
            _caller: caller,
        });
        let inner = self.inner.clone();
        // Dropping this caller detaches the supervised task. Its owner spans
        // physical pop, blocking pg_dump/ZIP work and actual result reply.
        tokio::spawn(async move {
            let Some(id) = inner
                .jobs
                .claim_create(owner.clone())
                .await
                .map_err(Error::Transport)?
            else {
                return Ok(Run::Empty);
            };
            let handler = inner.handler.clone();
            let physical = owner.clone();
            let outcome = tokio::task::spawn_blocking(move || {
                let _retained = physical;
                handler.create()
            })
            .await
            .map_err(|_| Error::HandlerPanic)?;
            let result = match outcome {
                Ok(value) => {
                    if value.filename.is_empty()
                        || value.filename.len() > 255
                        || value.filename.contains(['/', '\\', '\0'])
                    {
                        return Err(Error::InvalidResult);
                    }
                    json!({"status":"completed","filename":value.filename,"size":value.size})
                }
                Err(message) => {
                    if message.len() > 65536 {
                        return Err(Error::InvalidResult);
                    };
                    json!({"status":"failed","error":message})
                }
            };
            match inner
                .jobs
                .write_success(id, result, owner)
                .await
                .map_err(Error::Transport)?
            {
                true => Ok(Run::Completed(id)),
                false => Ok(Run::ResultAlreadySucceeded(id)),
            }
        })
        .await
        .map_err(|_| Error::HandlerPanic)?
    }
    /// Warm shutdown drains accepted work; it does not cancel a pg_dump child
    /// or close result admission while a task still needs its result publication.
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.inner.closed.store(true, Ordering::Release);
        let drain = self.inner.slots.clone().acquire_owned();
        let _held = tokio::time::timeout(timeout, drain)
            .await
            .map_err(|_| Error::DrainTimeout)?
            .map_err(|_| Error::Closed)?;
        // Shared transport remains open for other active worker families.
        // The owner closes Jobs once all producer/worker drains complete.
        Ok(())
    }
}
/// Validate before pop. No general task, ETA, expiry, callbacks, chain, retry,
/// priorities, reply RPC, stamps or altered serializer support is implied.
pub(crate) fn create_identity(bytes: &[u8]) -> Result<Uuid, Error> {
    if bytes.len() > protocol::MAX_ENVELOPE {
        return Err(Error::UnsupportedEnvelope);
    }
    let e: Value = serde_json::from_slice(bytes).map_err(|_| Error::UnsupportedEnvelope)?;
    let fields = |v: &Value, names: &[&str]| {
        v.as_object()
            .is_some_and(|m| m.len() == names.len() && names.iter().all(|k| m.contains_key(*k)))
    };
    if !fields(
        &e,
        &[
            "body",
            "content-encoding",
            "content-type",
            "headers",
            "properties",
        ],
    ) || !fields(
        &e["headers"],
        &[
            "lang",
            "task",
            "id",
            "shadow",
            "eta",
            "expires",
            "group",
            "group_index",
            "retries",
            "timelimit",
            "root_id",
            "parent_id",
            "argsrepr",
            "kwargsrepr",
            "origin",
            "ignore_result",
            "replaced_task_nesting",
            "stamped_headers",
            "stamps",
        ],
    ) || !fields(
        &e["properties"],
        &[
            "correlation_id",
            "reply_to",
            "delivery_mode",
            "delivery_info",
            "priority",
            "body_encoding",
            "delivery_tag",
        ],
    ) {
        return Err(Error::UnsupportedEnvelope);
    }
    let h = &e["headers"];
    let p = &e["properties"];
    let id = Uuid::parse_str(h["id"].as_str().ok_or(Error::UnsupportedEnvelope)?)
        .map_err(|_| Error::UnsupportedEnvelope)?;
    if e["content-type"] != "application/json"
        || e["content-encoding"] != "utf-8"
        || h["task"] != "apps.backups.tasks.create_backup_task"
        || h["lang"] != "py"
        || h["root_id"] != id.to_string()
        || !h["parent_id"].is_null()
        || !h["eta"].is_null()
        || !h["expires"].is_null()
        || h["retries"] != 0
        || h["ignore_result"] != false
        || h["timelimit"] != json!([null, null])
        || !h["shadow"].is_null()
        || !h["group"].is_null()
        || !h["group_index"].is_null()
        || h["replaced_task_nesting"] != 0
        || !h["stamped_headers"].is_null()
        || h["stamps"] != json!({})
        || h["origin"]
            .as_str()
            .is_none_or(|v| v.is_empty() || v.len() > 128 || v.chars().any(char::is_control))
        || p["reply_to"]
            .as_str()
            .and_then(|v| Uuid::parse_str(v).ok())
            .is_none()
        || p["delivery_tag"]
            .as_str()
            .and_then(|v| Uuid::parse_str(v).ok())
            .is_none()
        || h["argsrepr"] != "()"
        || h["kwargsrepr"] != "{}"
        || p["correlation_id"] != id.to_string()
        || p["body_encoding"] != "base64"
        || p["delivery_mode"] != 2
        || p["priority"] != 0
        || p["delivery_info"] != json!({"exchange":"","routing_key":"celery"})
    {
        return Err(Error::UnsupportedEnvelope);
    }
    let body = STANDARD
        .decode(e["body"].as_str().ok_or(Error::UnsupportedEnvelope)?)
        .map_err(|_| Error::UnsupportedEnvelope)?;
    if body.len() > 32768 {
        return Err(Error::UnsupportedEnvelope);
    }
    let body: Value = serde_json::from_slice(&body).map_err(|_| Error::UnsupportedEnvelope)?;
    if body != json!([[],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}]) {
        return Err(Error::UnsupportedEnvelope);
    }
    Ok(id)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn create_only_shapes_reject_before_ack() {
        let id = Uuid::new_v4();
        let valid = protocol::encode(
            &protocol::Task::CreateBackup,
            id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "fixture",
        )
        .unwrap();
        assert_eq!(create_identity(&valid), Ok(id));
        for change in ["task", "eta", "ignore_result", "id", "retries"] {
            let mut v: Value = serde_json::from_slice(&valid).unwrap();
            v["headers"][change] = json!("unsupported");
            assert_eq!(
                create_identity(&serde_json::to_vec(&v).unwrap()),
                Err(Error::UnsupportedEnvelope)
            );
        }
        let restore = protocol::encode(
            &protocol::Task::RestoreBackup {
                filename: "fixture.zip".into(),
            },
            id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "fixture",
        )
        .unwrap();
        assert_eq!(create_identity(&restore), Err(Error::UnsupportedEnvelope));
    }
}
