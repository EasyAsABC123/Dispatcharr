//! Experimental actual provider account tasks, no private fixture identity gate.
//! Original Dispatcharr/Celery authors retain all application/protocol credit.
use crate::{
    milestone_dispatch::{Disposition as D, Family, Handler, Lifecycle},
    normal_revocation::Revoked,
    transport::Jobs,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use dispatcharr_contracts::{BoxFuture, UpdateEventSink};
use dispatcharr_coordination::{TaskLockDetailedError, TaskLockError, TaskLocks};
use dispatcharr_store::{AuthService, ProducerIdentitySource, StoreError};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::Semaphore;
use uuid::Uuid;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Info,
    Profiles,
}
#[derive(Clone, Debug)]
pub(crate) struct Message {
    pub id: Uuid,
    pub target: i64,
    pub kind: Kind,
    pub parent: Option<Uuid>,
}
pub(crate) fn parse(raw: &[u8]) -> Result<Message, ()> {
    if raw.is_empty() || raw.len() > crate::protocol::MAX_ENVELOPE {
        return Err(());
    }
    let mut frame = crate::clocked_recording_envelope::json_value(raw).map_err(|_| ())?;
    let kind = match frame["headers"]["task"].as_str() {
        Some("apps.m3u.tasks.refresh_account_info") => Kind::Info,
        Some("apps.m3u.tasks.refresh_account_profiles") => Kind::Profiles,
        _ => return Err(()),
    };
    let decoded = STANDARD
        .decode(frame["body"].as_str().ok_or(())?)
        .map_err(|_| ())?;
    if decoded.len() > 4096 {
        return Err(());
    }
    let body = crate::clocked_recording_envelope::json_value(&decoded).map_err(|_| ())?;
    if body.as_array().is_none_or(|v| v.len() != 3)
        || body[0].as_array().is_none_or(|v| v.len() != 1)
        || body[1] != json!({})
        || body[2] != json!({"callbacks":null,"errbacks":null,"chain":null,"chord":null})
    {
        return Err(());
    }
    let target = body[0][0]
        .as_i64()
        .filter(|v| *v > 0 && *v <= i64::from(i32::MAX))
        .ok_or(())?;
    if frame["headers"]["argsrepr"] != json!(format!("({target},)"))
        && frame["headers"]["argsrepr"] != json!(format!("[{target}]"))
    {
        return Err(());
    }
    let id = Uuid::parse_str(frame["headers"]["id"].as_str().ok_or(())?).map_err(|_| ())?;
    let parent = match &frame["headers"]["parent_id"] {
        Value::Null => None,
        Value::String(v) => Some(Uuid::parse_str(v).map_err(|_| ())?),
        _ => return Err(()),
    };
    let root = Uuid::parse_str(frame["headers"]["root_id"].as_str().ok_or(())?).map_err(|_| ())?;
    if id.is_nil()
        || root.is_nil()
        || parent.is_some_and(|v| v.is_nil())
        || (parent.is_none() && root != id)
    {
        return Err(());
    }
    // Preserve real lineage in Message/backend, validate the remaining common
    // envelope without requiring an in-process parent-success receipt.
    frame["headers"]["parent_id"] = Value::Null;
    frame["headers"]["root_id"] = json!(id.to_string());
    frame["headers"]["task"] = json!("apps.proxy.tasks.fetch_channel_stats");
    frame["headers"]["argsrepr"] = json!("()");
    frame["body"] = json!(STANDARD.encode(
        serde_json::to_vec(
            &json!([[],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        )
        .map_err(|_| ())?
    ));
    crate::stats_protocol::parse(&serde_json::to_vec(&frame).map_err(|_| ())?).map_err(|_| ())?;
    Ok(Message {
        id,
        target,
        kind,
        parent,
    })
}
struct Inner {
    jobs: Jobs,
    auth: AuthService,
    identity: ProducerIdentitySource,
    events: Arc<dyn UpdateEventSink>,
    profile_signal: Arc<dyn dispatcharr_store::xc_account_tasks::ProfileSaveSignal>,
    locks: TaskLocks,
    revoked: Revoked,
    closed: AtomicBool,
    slots: Arc<Semaphore>,
    uncertain: Mutex<Vec<Arc<dyn Send + Sync>>>,
    delay: Duration,
}
#[derive(Clone)]
pub struct Prepared {
    inner: Arc<Inner>,
}
impl Prepared {
    pub fn new(
        jobs: Jobs,
        auth: AuthService,
        events: Arc<dyn UpdateEventSink>,
        locks: TaskLocks,
        revoked: Revoked,
        delay: Duration,
    ) -> Result<Self, D> {
        if delay > Duration::from_secs(3600) {
            return Err(D::UnsupportedDomain);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                identity: auth.producer_identity_source(),
                auth,
                profile_signal: Arc::new(crate::xc_profile_notifications::Callback::new(
                    events.clone(),
                )),
                events,
                locks,
                revoked,
                closed: AtomicBool::new(false),
                slots: Arc::new(Semaphore::new(2)),
                uncertain: Mutex::new(Vec::with_capacity(2)),
                delay,
            }),
        })
    }
    pub fn entries(&self) -> Vec<(Family, Arc<dyn Handler>)> {
        vec![
            (
                Family::XcAccountInfo,
                Arc::new(Worker {
                    inner: self.inner.clone(),
                    kind: Kind::Info,
                }),
            ),
            (
                Family::XcAccountProfiles,
                Arc::new(Worker {
                    inner: self.inner.clone(),
                    kind: Kind::Profiles,
                }),
            ),
        ]
    }
}
impl dispatcharr_store::xc_account_http::Publisher for Prepared {
    fn publish_vod(
        &self,
        account: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Value, StoreError>> {
        Box::pin(async move {
            if self.inner.closed.load(Ordering::Acquire) {
                return Err(StoreError::Unsupported("VOD producer closed"));
            }
            let name = dispatcharr_store::vod_ingestion::api_target(
                &self.inner.auth,
                account,
                owner.clone(),
            )
            .await?;
            dispatcharr_store::vod_ingestion::preflight(&self.inner.auth, account, owner.clone())
                .await
                .map_err(|error| match error {
                    dispatcharr_store::vod_ingestion::AdmissionError::Unsupported => {
                        StoreError::Unsupported("Initial VOD ingestion domain unavailable")
                    }
                    dispatcharr_store::vod_ingestion::AdmissionError::Operational(error) => error,
                })?;
            self.publish_vod_settings_root(account, owner).await?;
            Ok(json!({"message":format!("VOD refresh initiated for account {name}")}))
        })
    }

    fn publish_vod_settings_root(
        &self,
        account: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            let identity = self.inner.identity.current()?;
            let raw = crate::vod_ingestion_protocol::encode(account, &identity)
                .map_err(|_| StoreError::Unsupported("VOD root envelope unavailable"))?;
            let message = crate::vod_ingestion_protocol::parse(&raw)
                .map_err(|_| StoreError::Unsupported("VOD root envelope invalid"))?;
            let retained: Arc<dyn Send + Sync> =
                Arc::new((owner, self.inner.clone(), raw.clone(), message.id));
            if self.inner.closed.load(Ordering::Acquire) {
                return Err(StoreError::Unsupported("VOD producer draining"));
            }
            match self.inner.jobs.publish_vod(&raw, retained.clone()).await {
                Ok(receipt) if receipt.task_id == message.id => Ok(()),
                other => {
                    if !matches!(other, Err(crate::transport::Error::Capacity)) {
                        self.inner
                            .uncertain
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .push(retained);
                    }
                    Err(StoreError::StatusBody(
                        503,
                        json!({"error":"VOD publication unsettled"}),
                    ))
                }
            }
        })
    }

    fn publish_all_refresh(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            if self.inner.closed.load(Ordering::Acquire) {
                return Err(StoreError::Unsupported("M3U aggregate producer closed"));
            }
            dispatcharr_store::m3u_refresh_all::require_supported_domain(
                &self.inner.auth,
                owner.clone(),
            )
            .await?;
            let identity = self.inner.identity.current()?;
            let id = Uuid::new_v4();
            let raw = crate::m3u_refresh_all_protocol::encode_root(id, &identity)
                .map_err(|_| StoreError::Unsupported("M3U aggregate root envelope"))?;
            if self.inner.closed.load(Ordering::Acquire) {
                return Err(StoreError::Unsupported("M3U aggregate producer draining"));
            }
            let retained: Arc<dyn Send + Sync> = Arc::new((owner, id, raw.clone()));
            match self
                .inner
                .jobs
                .publish_m3u_all_frame(&raw, None, retained.clone())
                .await
            {
                Ok(receipt) if receipt.task_id == id => Ok(()),
                other => {
                    // Even a wrong receipt cannot release the actual HTTP owner.
                    // Preserve exact frame/UUID for unknown publication outcome.
                    if !matches!(other, Err(crate::transport::Error::Capacity)) {
                        self.inner
                            .uncertain
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .push(retained);
                    }
                    Err(StoreError::StatusBody(
                        503,
                        json!({"error":"M3U aggregate broker publication did not acknowledge"}),
                    ))
                }
            }
        })
    }
    fn publish_refresh(
        &self,
        account: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            if self.inner.closed.load(Ordering::Acquire) {
                return Err(StoreError::Unsupported("XC producer closed"));
            }
            let identity = self.inner.identity.current()?;
            let id = Uuid::new_v4();
            let raw = crate::protocol::encode(
                &crate::protocol::Task::CreateBackup,
                id,
                identity.reply_to,
                Uuid::new_v4(),
                &identity.origin,
            )
            .map_err(|_| StoreError::Unsupported("XC refresh envelope"))?;
            let mut frame: Value = serde_json::from_slice(&raw)
                .map_err(|_| StoreError::Unsupported("XC refresh JSON"))?;
            frame["headers"]["task"] = json!("apps.m3u.tasks.refresh_single_m3u_account");
            frame["headers"]["argsrepr"] = json!(format!("({account},)"));
            frame["headers"]["timelimit"] = json!([3600, 3500]);
            frame["body"]=json!(STANDARD.encode(serde_json::to_vec(&json!([[account],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).map_err(|_|StoreError::Unsupported("XC refresh body"))?));
            let raw = serde_json::to_vec(&frame)
                .map_err(|_| StoreError::Unsupported("XC refresh encoding"))?;
            self.inner
                .jobs
                .publish_xc_live_root(&raw, owner)
                .await
                .map_err(|_| {
                    StoreError::StatusBody(
                        503,
                        json!({"error":"M3U broker publication did not acknowledge"}),
                    )
                })?;
            Ok(())
        })
    }

    fn publish_info(
        &self,
        profile: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            if self.inner.closed.load(Ordering::Acquire) {
                return Err(StoreError::Unsupported("XC producer closed"));
            }
            let identity = self.inner.identity.current()?;
            let id = Uuid::new_v4();
            let raw = crate::protocol::encode(
                &crate::protocol::Task::CreateBackup,
                id,
                identity.reply_to,
                Uuid::new_v4(),
                &identity.origin,
            )
            .map_err(|_| StoreError::Unsupported("XC producer envelope"))?;
            let mut frame: Value = serde_json::from_slice(&raw)
                .map_err(|_| StoreError::Unsupported("XC producer JSON"))?;
            frame["headers"]["task"] = json!("apps.m3u.tasks.refresh_account_info");
            frame["headers"]["argsrepr"] = json!(format!("({profile},)"));
            frame["body"]=json!(STANDARD.encode(serde_json::to_vec(&json!([[profile],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).map_err(|_|StoreError::Unsupported("XC producer body"))?));
            let raw = serde_json::to_vec(&frame)
                .map_err(|_| StoreError::Unsupported("XC producer encoding"))?;
            self.inner
                .jobs
                .publish_xc_info(&raw, owner)
                .await
                .map_err(|_| {
                    StoreError::StatusBody(
                        503,
                        json!({"error":"XC broker publication did not acknowledge"}),
                    )
                })?;
            Ok(())
        })
    }
}
struct Worker {
    inner: Arc<Inner>,
    kind: Kind,
}
fn retain(inner: &Inner, owner: Arc<dyn Send + Sync>) {
    let mut held = inner.uncertain.lock().unwrap_or_else(|e| e.into_inner());
    if !held.iter().any(|v| Arc::ptr_eq(v, &owner)) {
        held.push(owner)
    }
}
impl Worker {
    async fn run(&self, expected: Option<Arc<[u8]>>, caller: Arc<dyn Send + Sync>) -> Result<D, D> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(D::Operational("XC intake closed".into()));
        }
        let slot = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| D::Capacity)?;
        let owner: Arc<dyn Send + Sync> = Arc::new((slot, caller, self.inner.clone()));
        let inner = self.inner.clone();
        let kind = self.kind;
        let fail_owner = owner.clone();
        let fail_inner = inner.clone();
        let joined = tokio::spawn(async move {
            let candidate = match inner
                .jobs
                .peek_xc_account(expected.as_deref(), owner.clone())
                .await
            {
                Ok(Some(v)) => v,
                Ok(None) => return Ok(D::Empty),
                Err(crate::transport::Error::ClaimRace) => return Ok(D::HeadChanged),
                Err(crate::transport::Error::UnsupportedTask) => return Err(D::UnsupportedHead),
                Err(_) => return Err(D::Operational("XC broker peek failed".into())),
            };
            if candidate.message.kind != kind {
                return Err(D::UnsupportedHead);
            }
            let session = dispatcharr_store::xc_account_tasks::prepare(
                inner.auth.clone(),
                owner.clone(),
                inner.profile_signal.clone(),
            )
            .await
            .map_err(|e| match e {
                StoreError::StatusBody(503, _) => D::Capacity,
                _ => D::UnsupportedDomain,
            })?;
            if session
                .preflight(candidate.message.target, kind == Kind::Profiles)
                .await
                .is_err()
            {
                if session.close().await.is_err() {
                    retain(&inner, owner.clone());
                    return Err(D::Operational("XC preflight physical close failed".into()));
                }
                return Err(D::UnsupportedDomain);
            }
            if inner.closed.load(Ordering::Acquire) {
                if session.close().await.is_err() {
                    retain(&inner, owner.clone());
                    return Err(D::Operational("XC SQL close failed".into()));
                }
                return Err(D::Operational("XC admission stopped".into()));
            }
            let message = match inner.jobs.claim_xc_account(candidate, owner.clone()).await {
                Ok(v) => v,
                Err(crate::transport::Error::ClaimRace) => {
                    if session.close().await.is_err() {
                        retain(&inner, owner.clone());
                        return Err(D::Operational("XC SQL close failed".into()));
                    }
                    return Ok(D::HeadChanged);
                }
                Err(_) => {
                    let _ = session.close().await;
                    retain(&inner, owner);
                    return Err(D::Operational("XC claim outcome unknown".into()));
                }
            };
            let mut lease = None;
            let result = async {
                let revoked = inner
                    .revoked
                    .contains(&message.id.to_string())
                    .map_err(|_| D::Operational("XC revoke state".into()))?;
                let value = if revoked {
                    Value::Null
                } else if kind == Kind::Info {
                    match inner
                        .locks
                        .acquire_retained_detailed(
                            "refresh_account_info",
                            message.target,
                            owner.clone(),
                        )
                        .await
                    {
                        Ok(lock) => {
                            lease = Some(lock);
                            session
                                .info(message.target, inner.events.clone(), owner.clone())
                                .await
                                .map_err(|_| {
                                    D::Operational("XC info effects did not settle".into())
                                })?
                        }
                        Err(TaskLockDetailedError::Operational(TaskLockError::Held)) => {
                            json!(format!(
                                "Account info refresh task already running for profile_id={}.",
                                message.target
                            ))
                        }
                        Err(_) => {
                            return Err(D::Operational("XC info lock outcome unknown".into()))
                        }
                    }
                } else {
                    session
                        .profiles(message.target, inner.delay)
                        .await
                        .map_err(|_| D::Operational("XC profiles effects did not settle".into()))?
                };
                if let Some(lock) = lease.take() {
                    lock.release_owned_detailed()
                        .await
                        .map_err(|_| D::Operational("XC info lock release unknown".into()))?;
                }
                session
                    .close()
                    .await
                    .map_err(|_| D::Operational("XC SQL physical close failed".into()))?;
                inner
                    .jobs
                    .write_xc_account(&message, value, revoked, owner.clone())
                    .await
                    .map_err(|_| D::Operational("XC result outcome unknown".into()))?;
                Ok(if revoked {
                    D::Revoked(message.id)
                } else {
                    D::Completed(message.id)
                })
            }
            .await;
            if result.is_err() {
                // Source releases its acquired info lock in the caught-error
                // path as well. Physical SQL cleanup still runs if unlock fails.
                let unlock = if let Some(lock) = lease.take() {
                    lock.release_owned_detailed()
                        .await
                        .map(|_| ())
                        .map_err(|_| ())
                } else {
                    Ok(())
                };
                let close = session.close().await;
                retain(&inner, owner);
                if unlock.is_err() || close.is_err() {
                    return Err(D::Operational(format!(
                        "{result:?}; XC cleanup lock or SQL outcome remains unknown"
                    )));
                }
            }
            result
        })
        .await;
        match joined {
            Ok(v) => v,
            Err(_) => {
                retain(&fail_inner, fail_owner);
                Err(D::Operational("XC task join failed; owner retained".into()))
            }
        }
    }
}
impl Handler for Worker {
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn run_selected(
        &self,
        raw: Arc<[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.run(Some(raw), owner))
    }
    fn run_once(&self, owner: Arc<dyn Send + Sync>) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.run(None, owner))
    }
}
impl Lifecycle for Worker {
    fn stop_admission(&self) {
        self.inner.closed.store(true, Ordering::Release)
    }
    fn shutdown(&self, duration: Duration) -> BoxFuture<'_, Result<(), D>> {
        Box::pin(async move {
            self.stop_admission();
            let held =
                tokio::time::timeout(duration, self.inner.slots.clone().acquire_many_owned(2))
                    .await
                    .map_err(|_| D::Operational("XC physical drain deadline".into()))?
                    .map_err(|_| D::Operational("XC admission closed".into()))?;
            drop(held);
            if !self
                .inner
                .uncertain
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty()
            {
                return Err(D::Operational(
                    "XC/compact accepted ownership remains uncertain".into(),
                ));
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod protocol_controls {
    use super::*;
    fn raw(kind: &str, parent: Option<Uuid>) -> Vec<u8> {
        let id = Uuid::new_v4();
        let encoded = crate::protocol::encode(
            &crate::protocol::Task::CreateBackup,
            id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "fixture@parser",
        )
        .unwrap();
        let mut v: Value = serde_json::from_slice(&encoded).unwrap();
        v["headers"]["task"] = json!(kind);
        v["headers"]["argsrepr"] = json!("(41,)");
        if let Some(parent) = parent {
            v["headers"]["parent_id"] = json!(parent.to_string());
            v["headers"]["root_id"] = json!(Uuid::new_v4().to_string());
        }
        v["body"] = json!(STANDARD.encode(
            serde_json::to_vec(
                &json!([[41],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
            )
            .unwrap()
        ));
        serde_json::to_vec(&v).unwrap()
    }
    #[test]
    fn xc_normal_roots_and_external_parented_tasks_need_no_in_process_receipt() {
        for name in [
            "apps.m3u.tasks.refresh_account_info",
            "apps.m3u.tasks.refresh_account_profiles",
        ] {
            assert_eq!(parse(&raw(name, None)).unwrap().target, 41);
            let parent = Uuid::new_v4();
            assert_eq!(
                parse(&raw(name, Some(parent))).unwrap().parent,
                Some(parent)
            );
        }
        assert!(parse(&raw("apps.m3u.tasks.refresh_m3u_groups", None)).is_err());
        let mut v: Value =
            serde_json::from_slice(&raw("apps.m3u.tasks.refresh_account_info", None)).unwrap();
        v["properties"]["delivery_info"]["routing_key"] = json!("dvr");
        assert!(parse(&serde_json::to_vec(&v).unwrap()).is_err());
    }
}
