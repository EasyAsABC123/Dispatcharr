//! Bounded plain normal plugin refresh; original authors retain full credit.
//! ETA intake reserves durably; due execution uses the same bounded normal dispatcher.
use crate::{
    milestone_dispatch::{Disposition as D, Family, Handler, Lifecycle},
    normal_revocation::Revoked,
    transport::Jobs,
};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_store::{AuthService, PluginRefreshAdmission, PluginRepositoryRefresh};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
#[derive(Clone, Debug)]
pub(crate) struct Message {
    pub id: Uuid,
    pub eta: Option<chrono::DateTime<chrono::Utc>>,
}
pub(crate) const TASK: &str = "apps.plugins.tasks.refresh_plugin_repos";

pub(crate) fn parse(raw: &[u8]) -> Result<Message, ()> {
    if raw.is_empty() || raw.len() > crate::protocol::MAX_ENVELOPE {
        return Err(());
    }
    let mut frame = crate::clocked_recording_envelope::json_value(raw).map_err(|_| ())?;
    if frame["headers"]["task"] != TASK {
        return Err(());
    }
    // Never ACK-and-sleep or hold the FIFO head until ETA as if that matched
    // Celery's durable reservation and later-task intake. Leave raw untouched.
    if !frame["headers"]["eta"].is_null() {
        return Err(());
    }
    frame["headers"]["task"] = json!("apps.proxy.tasks.fetch_channel_stats");
    let common = crate::stats_protocol::parse(&serde_json::to_vec(&frame).map_err(|_| ())?)
        .map_err(|_| ())?;
    Ok(Message {
        id: common.task_id,
        eta: None,
    })
}
pub(crate) fn parse_eta(raw: &[u8]) -> Result<Message, ()> {
    let mut frame = crate::clocked_recording_envelope::json_value(raw).map_err(|_| ())?;
    let eta = chrono::DateTime::parse_from_rfc3339(frame["headers"]["eta"].as_str().ok_or(())?)
        .map_err(|_| ())?
        .with_timezone(&chrono::Utc);
    frame["headers"]["eta"] = Value::Null;
    let mut message = parse(&serde_json::to_vec(&frame).map_err(|_| ())?)?;
    message.eta = Some(eta);
    Ok(message)
}
#[path = "normal_plugin_eta.rs"]
mod delayed;
fn startup_frame(
    id: Uuid,
    reply: Uuid,
    origin: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<u8>, D> {
    let base = crate::protocol::encode(
        &crate::protocol::Task::CreateBackup,
        id,
        reply,
        Uuid::new_v4(),
        origin,
    )
    .map_err(|_| D::Operational("Plugin startup base frame invalid".into()))?;
    let mut frame: Value = serde_json::from_slice(&base)
        .map_err(|_| D::Operational("Plugin startup base frame JSON invalid".into()))?;
    frame["headers"]["task"] = json!(TASK);
    frame["headers"]["eta"] =
        json!((now + chrono::Duration::seconds(10))
            .to_rfc3339_opts(chrono::SecondsFormat::Micros, false));
    let bytes = serde_json::to_vec(&frame).map_err(|_| D::UnsupportedDomain)?;
    parse_eta(&bytes).map_err(|_| D::UnsupportedDomain)?;
    Ok(bytes)
}
struct Inner {
    visibility_cursor: std::sync::atomic::AtomicUsize,
    producer: dispatcharr_store::ProducerIdentitySource,
    startup_attempted: AtomicBool,
    next_visibility: Mutex<Option<tokio::time::Instant>>,
    pending: Mutex<std::collections::VecDeque<crate::plugin_eta::Reservation>>,
    jobs: Jobs,
    executor: PluginRepositoryRefresh,
    revoked: Revoked,
    closed: AtomicBool,
    slots: Arc<Semaphore>,
    uncertain: Mutex<Vec<Arc<Owner>>>,
}
struct Owner {
    _slot: OwnedSemaphorePermit,
    caller: Arc<dyn Send + Sync>,
    _inner: Arc<Inner>,
    admission: Mutex<Option<Arc<PluginRefreshAdmission>>>,
}
#[derive(Clone)]
pub struct Prepared {
    inner: Arc<Inner>,
}
impl Prepared {
    pub fn new(jobs: Jobs, auth: AuthService, revoked: Revoked) -> Result<Self, D> {
        Self::new_configured(jobs, auth, revoked, None)
    }
    pub fn with_plugin_publications(
        jobs: Jobs,
        auth: AuthService,
        revoked: Revoked,
        publications: dispatcharr_store::PluginPublicationOwners,
    ) -> Result<Self, D> {
        Self::new_configured(jobs, auth, revoked, Some(publications))
    }
    fn new_configured(
        jobs: Jobs,
        auth: AuthService,
        revoked: Revoked,
        publications: Option<dispatcharr_store::PluginPublicationOwners>,
    ) -> Result<Self, D> {
        let executor = PluginRepositoryRefresh::new(auth.clone());
        let executor = match publications {
            Some(value) => executor.with_publications(value),
            None => executor,
        };
        if !jobs.allows_defaults() {
            return Err(D::UnsupportedDomain);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                visibility_cursor: std::sync::atomic::AtomicUsize::new(0),
                next_visibility: Mutex::new(None),
                pending: Mutex::new(std::collections::VecDeque::new()),
                jobs,
                producer: auth.producer_identity_source(),
                startup_attempted: AtomicBool::new(false),
                executor,
                revoked,
                closed: AtomicBool::new(false),
                slots: Arc::new(Semaphore::new(1)),
                uncertain: Mutex::new(Vec::with_capacity(1)),
            }),
        })
    }
    /// One process startup attempt, matching Source's caught apply_async(countdown=10).
    /// No distributed lock or retry is invented; Redis transport ambiguity is reported.
    pub async fn publish_startup(&self, caller: Arc<dyn Send + Sync>) -> Result<Option<Uuid>, D> {
        if std::env::var("DISPATCHARR_SKIP_PLUGIN_AUTODISCOVERY")
            .ok()
            .is_some_and(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        {
            return Ok(None);
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(D::Operational("Plugin startup closed".into()));
        }
        if self.inner.startup_attempted.swap(true, Ordering::AcqRel) {
            return Ok(None);
        }
        let slot = self
            .inner
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| D::Operational("Plugin startup admission closed".into()))?;
        let inner = self.inner.clone();
        let owner: Arc<dyn Send + Sync> = Arc::new((caller, slot, inner.clone()));
        tokio::spawn(async move {
            let identity = inner
                .producer
                .current()
                .map_err(|_| D::Operational("Plugin startup producer identity failed".into()))?;
            let id = Uuid::new_v4();
            let bytes = startup_frame(id, identity.reply_to, &identity.origin, chrono::Utc::now())?;
            inner
                .jobs
                .publish_plugin_startup(&bytes, owner)
                .await
                .map_err(|_| D::Operational("Plugin startup publication unsettled".into()))?;
            Ok(Some(id))
        })
        .await
        .map_err(|_| D::Operational("Plugin startup publication join unsettled".into()))?
    }
    pub fn entry(&self) -> (Family, Arc<dyn Handler>) {
        (Family::PluginRefresh, Arc::new(self.clone()))
    }
    async fn run(&self, expected: Option<Arc<[u8]>>, caller: Arc<dyn Send + Sync>) -> Result<D, D> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Ok(D::NotDue);
        }
        let slot = match self.inner.slots.clone().try_acquire_owned() {
            Ok(v) => v,
            Err(_) => return Ok(D::Capacity),
        };
        let owner = Arc::new(Owner {
            _slot: slot,
            caller,
            _inner: self.inner.clone(),
            admission: Mutex::new(None),
        });
        let inner = self.inner.clone();
        let join_owner = owner.clone();
        let join_inner = inner.clone();
        match tokio::spawn(async move {
            let candidate = match inner
                .jobs
                .peek_plugin_refresh(expected.as_deref(), owner.clone())
                .await
            {
                Ok(Some(v)) => v,
                Ok(None) => return Ok(D::Empty),
                Err(crate::transport::Error::ClaimRace) => return Ok(D::HeadChanged),
                Err(crate::transport::Error::UnsupportedTask) => return Err(D::UnsupportedHead),
                Err(_) => return Err(D::Operational("Plugin refresh peek failed".into())),
            };
            if candidate.message.eta.is_some() {
                return delayed::reserve(&inner, candidate.raw, owner).await;
            }
            let admission = match inner.executor.try_admit_frame(
                candidate.message.id,
                &candidate.raw,
                owner.caller.clone(),
            ) {
                Ok(v) => Arc::new(v),
                Err(_) => return Ok(D::Capacity),
            };
            *owner.admission.lock().unwrap_or_else(|v| v.into_inner()) = Some(admission.clone());
            admission
                .preflight()
                .await
                .map_err(|_| D::UnsupportedDomain)?;
            if inner.closed.load(Ordering::Acquire) {
                admission.close().await.map_err(|_| {
                    D::Operational("Plugin refresh preclaim SQL close failed".into())
                })?;
                return Ok(D::NotDue);
            }
            let message = match inner
                .jobs
                .claim_plugin_refresh(candidate, owner.clone())
                .await
            {
                Ok(v) => v,
                Err(crate::transport::Error::ClaimRace) => {
                    admission.close().await.map_err(|_| {
                        D::Operational("Plugin refresh changed-head SQL close failed".into())
                    })?;
                    return Ok(D::HeadChanged);
                }
                Err(_) => {
                    admission.close().await.map_err(|_| {
                        D::Operational("Plugin refresh ambiguous-claim SQL close failed".into())
                    })?;
                    retain(&inner, owner);
                    return Err(D::Operational("Plugin refresh claim ambiguous".into()));
                }
            };
            let result = async {
                if inner
                    .revoked
                    .contains(&message.id.to_string())
                    .map_err(|_| D::Operational("Plugin revoked-state read failed".into()))?
                {
                    inner
                        .jobs
                        .write_plugin_refresh(
                            &message,
                            Value::Null,
                            Vec::new(),
                            true,
                            owner.clone(),
                        )
                        .await
                        .map_err(|_| D::Operational("Plugin revoked backend failed".into()))?;
                    return Ok(D::Revoked(message.id));
                }
                let actual = admission.execute().await.map_err(|_| {
                    D::Operational("Plugin refresh outside settled supported domain".into())
                })?;
                inner
                    .jobs
                    .write_plugin_refresh(&message, actual, Vec::new(), false, owner.clone())
                    .await
                    .map_err(|_| D::Operational("Plugin refresh backend failed".into()))?;
                Ok(D::Completed(message.id))
            }
            .await;
            admission
                .close()
                .await
                .map_err(|_| D::Operational("Plugin refresh terminal SQL close failed".into()))?;
            if result.is_err() {
                retain(&inner, owner);
            }
            result
        })
        .await
        {
            Ok(value) => value,
            Err(_) => {
                retain(&join_inner, join_owner);
                Err(D::Operational(
                    "Plugin refresh join failed; physical admission retained".into(),
                ))
            }
        }
    }
}
fn retain(inner: &Inner, owner: Arc<Owner>) {
    let mut saved = inner.uncertain.lock().unwrap_or_else(|v| v.into_inner());
    if !saved.iter().any(|v| Arc::ptr_eq(v, &owner)) {
        saved.push(owner);
    }
}
fn admission_failure(error: dispatcharr_store::StoreError) -> D {
    match error {
        dispatcharr_store::StoreError::StatusBody(503, _) => D::Capacity,
        dispatcharr_store::StoreError::Unsupported("Owned task SQL session unavailable") => {
            D::Operational("Plugin refresh SQL session unsettled".into())
        }
        dispatcharr_store::StoreError::Database(_) => {
            D::Operational("Plugin refresh SQL preflight failed".into())
        }
        _ => D::UnsupportedDomain,
    }
}

impl Handler for Prepared {
    fn maintenance(&self, caller: Arc<dyn Send + Sync>) -> BoxFuture<'_, Result<(), D>> {
        Box::pin(async move {
            if self.inner.closed.load(Ordering::Acquire) {
                return Ok(());
            }
            {
                let mut next = self
                    .inner
                    .next_visibility
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if next.is_some_and(|at| at > tokio::time::Instant::now()) {
                    return Ok(());
                }
                *next = Some(tokio::time::Instant::now() + Duration::from_secs(100));
            }
            let slot = match self.inner.slots.clone().try_acquire_owned() {
                Ok(slot) => slot,
                Err(_) => return Ok(()),
            };
            let caller: Arc<dyn Send + Sync> = Arc::new((caller, slot, self.inner.clone()));
            let inner = self.inner.clone();
            // Spawn owns the entire physical scan/mutex release even if supervisor cancels.
            let offset = inner.visibility_cursor.load(Ordering::Acquire);
            let next = tokio::spawn(async move {
                inner
                    .jobs
                    .recover_plugin_visible_with_publications(
                        caller,
                        offset,
                        inner.executor.clone(),
                    )
                    .await
            })
            .await
            .map_err(|_| D::Operational("Plugin visibility join failed".into()))?
            .map_err(|_| D::Operational("Plugin visibility recovery failed".into()))?;
            self.inner.visibility_cursor.store(next, Ordering::Release);
            Ok(())
        })
    }

    fn due_head(&self) -> Option<Arc<[u8]>> {
        delayed::due_head(&self.inner)
    }
    fn run_due(
        &self,
        expected: Arc<[u8]>,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(delayed::run_due(self.inner.clone(), expected, caller))
    }

    fn supports_selected_head(&self) -> bool {
        true
    }
    fn run_selected(
        &self,
        expected: Arc<[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.run(Some(expected), owner))
    }
    fn run_once(&self, owner: Arc<dyn Send + Sync>) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.run(None, owner))
    }
}
impl Lifecycle for Prepared {
    fn stop_admission(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }
    fn shutdown(&self, timeout: Duration) -> BoxFuture<'_, Result<(), D>> {
        Box::pin(async move {
            self.stop_admission();
            let deadline = tokio::time::Instant::now() + timeout;
            let held = tokio::time::timeout_at(deadline, self.inner.slots.clone().acquire_owned())
                .await
                .map_err(|_| D::Operational("Plugin refresh physical drain incomplete".into()))?
                .map_err(|_| D::Operational("Plugin refresh permits closed".into()))?;
            let inner = self.inner.clone();
            let restore = tokio::spawn(async move {
                let _held = held;
                delayed::restore_all(&inner).await
            });
            tokio::time::timeout_at(deadline, restore)
                .await
                .map_err(|_| D::Operational("Plugin ETA warm restore deadline incomplete".into()))?
                .map_err(|_| D::Operational("Plugin ETA warm restore join incomplete".into()))?
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn natural_startup_has_real_identity_fresh_tag_and_exact_countdown() {
        let now = chrono::DateTime::from_timestamp(1800000000, 123456000).unwrap();
        let id = Uuid::new_v4();
        let reply = Uuid::new_v4();
        let bytes = startup_frame(id, reply, "gen17@owned-host", now).unwrap();
        let parsed = parse_eta(&bytes).unwrap();
        assert_eq!(parsed.id, id);
        assert_eq!(parsed.eta, Some(now + chrono::Duration::seconds(10)));
        let frame: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(frame["headers"]["origin"], "gen17@owned-host");
        assert_eq!(frame["properties"]["reply_to"], reply.to_string());
        assert_eq!(frame["headers"]["root_id"], id.to_string());
        assert!(frame["headers"]["parent_id"].is_null());
        assert_eq!(frame["headers"]["ignore_result"], false);
        assert_eq!(frame["headers"]["argsrepr"], "()");
    }
    #[test]
    fn source_startup_eta_requires_typed_receipt_not_plain_pop() {
        let source: Value =
            serde_json::from_str(include_str!("../datasets/source-plugin-startup-wire.json"))
                .unwrap();
        let frame = &source["receipt"]["frame"];
        let bytes = serde_json::to_vec(frame).unwrap();
        assert!(parse(&bytes).is_err());
        let delayed = parse_eta(&bytes).unwrap();
        assert_eq!(delayed.id.to_string(), frame["headers"]["id"]);
        assert!(delayed.eta.is_some());
        // Derived plain-Beat root parser control, not an observed Source task run.
        let mut plain = frame.clone();
        plain["headers"]["eta"] = Value::Null;
        let parsed = parse(&serde_json::to_vec(&plain).unwrap()).unwrap();
        assert_eq!(parsed.id.to_string(), plain["headers"]["id"]);
        plain["headers"]["task"] = json!("core.tasks.check_for_version_update");
        assert!(parse(&serde_json::to_vec(&plain).unwrap()).is_err());
        plain = frame.clone();
        plain["headers"]["eta"] = Value::Null;
        plain["body"] = json!("W1sxXSwge30sIHt9XQ==");
        assert!(parse(&serde_json::to_vec(&plain).unwrap()).is_err());
    }
}

#[cfg(test)]
mod builtin_producer_domain_tests {
    use super::*;
    #[test]
    fn exact_native_refresh_frame_keeps_existing_plain_eta_and_parent_guards() {
        let bytes = startup_frame(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "owned@test",
            chrono::Utc::now(),
        )
        .unwrap();
        let mut frame: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(parse_eta(&bytes).is_ok());
        frame["headers"]["eta"] = Value::Null;
        assert!(parse(&serde_json::to_vec(&frame).unwrap()).is_ok());
        for (name, value) in [
            ("task", json!("apps.plugins.tasks.unsupported")),
            ("parent_id", json!(Uuid::new_v4().to_string())),
            ("expires", json!(chrono::Utc::now().to_rfc3339())),
            ("argsrepr", json!("[1]")),
        ] {
            let mut bad = frame.clone();
            bad["headers"][name] = value;
            assert!(parse(&serde_json::to_vec(&bad).unwrap()).is_err());
        }
    }
}
