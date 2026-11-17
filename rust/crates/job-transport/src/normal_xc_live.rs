//! Normal XC Groups/Single over the existing M3U slot and shared Main ports.
//! Application, provider and Celery algorithms remain credited to Original authors.
use crate::{
    milestone_dispatch::Disposition as D,
    scanner_m3u_protocol::Kind,
    transport::{Jobs, M3uPorts},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_coordination::{TaskLockDetailedError, TaskLockError, TaskLocks};
use dispatcharr_store::{AuthService, ProducerIdentitySource, Store, StoreError};
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
type Owner = Arc<dyn Send + Sync>;
#[derive(Clone)]
pub(crate) struct Message {
    pub id: Uuid,
    pub root: Uuid,
    pub parent: Option<Uuid>,
    pub account: i64,
    pub kind: Kind,
    pub full: bool,
    pub use_cache: bool,
}
pub(crate) fn parse(raw: &[u8]) -> Result<Message, ()> {
    if raw.is_empty() || raw.len() > crate::protocol::MAX_ENVELOPE {
        return Err(());
    }
    let mut v = crate::clocked_recording_envelope::json_value(raw).map_err(|_| ())?;
    let kind = match v["headers"]["task"].as_str() {
        Some("apps.m3u.tasks.refresh_m3u_groups") => Kind::Groups,
        Some("apps.m3u.tasks.refresh_single_m3u_account") => Kind::Single,
        _ => return Err(()),
    };
    let bytes = STANDARD
        .decode(v["body"].as_str().ok_or(())?)
        .map_err(|_| ())?;
    if bytes.len() > 4096 {
        return Err(());
    }
    let body = crate::clocked_recording_envelope::json_value(&bytes).map_err(|_| ())?;
    if body.as_array().is_none_or(|v| v.len() != 3)
        || body[0].as_array().is_none_or(|v| v.len() > 1)
        || body[2] != json!({"callbacks":null,"errbacks":null,"chain":null,"chord":null})
    {
        return Err(());
    }
    let kwargs = body[1].as_object().ok_or(())?;
    if kwargs.keys().any(|key| {
        key != "account_id"
            && (kind == Kind::Single || !matches!(key.as_str(), "full_refresh" | "use_cache"))
    }) {
        return Err(());
    }
    let arguments = body[0].as_array().ok_or(())?;
    let account_value = match arguments.as_slice() {
        [id] => {
            if kwargs.contains_key("account_id") {
                return Err(());
            }
            id
        }
        [] => kwargs.get("account_id").ok_or(())?,
        _ => return Err(()),
    };
    let account = account_value
        .as_i64()
        .filter(|v| *v > 0 && *v <= i64::from(i32::MAX))
        .ok_or(())?;
    let full = match kwargs.get("full_refresh") {
        None => false,
        Some(Value::Bool(v)) => *v,
        _ => return Err(()),
    };
    if kwargs.get("use_cache").is_some_and(|v| !v.is_boolean()) {
        return Err(());
    }
    let argsrepr = v["headers"]["argsrepr"].as_str().ok_or(())?;
    if (arguments.is_empty() && !matches!(argsrepr, "[]" | "()"))
        || (!arguments.is_empty()
            && argsrepr != format!("({account},)")
            && argsrepr != format!("[{account}]"))
    {
        return Err(());
    }
    let mut fields = Vec::new();
    for (key, value) in kwargs {
        let spelling = match value {
            Value::Bool(true) => "True".to_owned(),
            Value::Bool(false) => "False".to_owned(),
            Value::Number(v) if v.as_i64().is_some() => v.to_string(),
            _ => return Err(()),
        };
        fields.push(format!("'{key}': {spelling}"));
    }
    if v["headers"]["kwargsrepr"] != format!("{{{}}}", fields.join(", ")) {
        return Err(());
    }
    let id = Uuid::parse_str(v["headers"]["id"].as_str().ok_or(())?).map_err(|_| ())?;
    let root = Uuid::parse_str(v["headers"]["root_id"].as_str().ok_or(())?).map_err(|_| ())?;
    let parent = match &v["headers"]["parent_id"] {
        Value::Null => None,
        Value::String(v) => Some(Uuid::parse_str(v).map_err(|_| ())?),
        _ => return Err(()),
    };
    if id.is_nil()
        || root.is_nil()
        || parent.is_some_and(|v| v.is_nil())
        || (parent.is_none() && id != root)
    {
        return Err(());
    }
    let expected = match kind {
        Kind::Groups => json!([null, null]),
        Kind::Single => json!([3600, 3500]),
    };
    if v["headers"]["timelimit"] != expected {
        return Err(());
    }
    v["headers"]["timelimit"] = json!([null, null]);
    v["headers"]["parent_id"] = Value::Null;
    v["headers"]["root_id"] = json!(id);
    v["headers"]["task"] = json!("apps.proxy.tasks.fetch_channel_stats");
    v["headers"]["argsrepr"] = json!("()");
    v["headers"]["kwargsrepr"] = json!("{}");
    v["body"] = json!(STANDARD.encode(
        serde_json::to_vec(
            &json!([[],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        )
        .map_err(|_| ())?
    ));
    crate::stats_protocol::parse(&serde_json::to_vec(&v).map_err(|_| ())?).map_err(|_| ())?;
    Ok(Message {
        id,
        root,
        parent,
        account,
        kind,
        full,
        use_cache: kwargs
            .get("use_cache")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}
struct Inner {
    sd: std::sync::OnceLock<crate::normal_sd_guide::Capability>,
    flows: crate::auto_sync_flow::ActiveFlows,
    jobs: Jobs,
    ports: M3uPorts,
    store: Store,
    auth: AuthService,
    identity: ProducerIdentitySource,
    events: Arc<dyn UpdateEventSink>,
    locks: TaskLocks,
    revoked: crate::normal_revocation::Revoked,
    slots: Arc<Semaphore>,
    cache_root: std::path::PathBuf,
    auto_jobs: Option<Arc<dyn dispatcharr_store::recording_http::RecordingJobs>>,
    closed: AtomicBool,
    uncertain: Mutex<Vec<Owner>>,
}
struct FaultGuard {
    inner: Arc<Inner>,
    owner: Owner,
    armed: bool,
}
impl Drop for FaultGuard {
    fn drop(&mut self) {
        if self.armed {
            let mut held = self
                .inner
                .uncertain
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if !held.iter().any(|v| Arc::ptr_eq(v, &self.owner)) {
                held.push(self.owner.clone())
            }
        }
    }
}
#[derive(Clone)]
pub(crate) struct Worker {
    inner: Arc<Inner>,
}
impl Worker {
    pub(crate) fn bind_sd(
        &self,
        capability: crate::normal_sd_guide::Capability,
    ) -> Result<(), crate::milestone_dispatch::Disposition> {
        if !capability.owns(&self.inner.jobs) {
            return Err(crate::milestone_dispatch::Disposition::UnsupportedDomain);
        }
        self.inner
            .sd
            .set(capability)
            .map_err(|_| crate::milestone_dispatch::Disposition::UnsupportedDomain)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        jobs: Jobs,
        ports: M3uPorts,
        store: Store,
        auth: AuthService,
        events: Arc<dyn UpdateEventSink>,
        locks: TaskLocks,
        slots: Arc<Semaphore>,
        revoked: crate::normal_revocation::Revoked,
        cache_root: std::path::PathBuf,
        auto_jobs: Option<Arc<dyn dispatcharr_store::recording_http::RecordingJobs>>,
    ) -> Self {
        let identity = auth.producer_identity_source();
        Self {
            inner: Arc::new(Inner {
                sd: std::sync::OnceLock::new(),
                flows: crate::auto_sync_flow::ActiveFlows::default(),
                jobs,
                ports,
                store,
                auth,
                events,
                locks,
                revoked,
                identity,
                slots,
                cache_root,
                auto_jobs,
                closed: AtomicBool::new(false),
                uncertain: Mutex::new(Vec::with_capacity(1)),
            }),
        }
    }
    pub(crate) fn stop(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }
    pub(crate) fn begin_drain(&self, budget: Duration) {
        self.inner.flows.begin_drain(budget);
    }
    pub(crate) async fn shutdown(&self, t: Duration) -> bool {
        self.stop();
        self.begin_drain(t);
        tokio::time::timeout(t, self.inner.slots.clone().acquire_owned())
            .await
            .is_ok_and(|v| v.is_ok())
    }
    /// None means the actual SQL account type was STD and the exact raw head is
    /// untouched; only then may the existing STD implementation be delegated.
    pub(crate) async fn run(
        &self,
        expected: Option<Arc<[u8]>>,
        caller: Owner,
    ) -> Result<Option<D>, D> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(D::Operational("XC live intake stopped".into()));
        }
        let slot = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| D::Capacity)?;
        let owner: Owner = Arc::new((slot, caller, self.inner.clone()));
        let inner = self.inner.clone();
        let failure_inner = inner.clone();
        let failure_owner = owner.clone();
        let joined = tokio::spawn(async move {
            let mut guard = FaultGuard {
                inner: inner.clone(),
                owner: owner.clone(),
                armed: true,
            };
            let result = async move {
                let candidate = match inner
                    .jobs
                    .peek_xc_live(expected.as_deref(), owner.clone())
                    .await
                {
                    Ok(Some(v)) => v,
                    Ok(None) => return Ok(Some(D::Empty)),
                    Err(crate::transport::Error::ClaimRace) => return Ok(Some(D::HeadChanged)),
                    Err(crate::transport::Error::UnsupportedTask) => {
                        return Err(D::UnsupportedHead)
                    }
                    Err(_) => return Err(D::Operational("XC live head outcome".into())),
                };
                if inner
                    .revoked
                    .contains(&candidate.message.id.to_string())
                    .map_err(|_| D::Operational("XC revoked state".into()))?
                {
                    let message = inner
                        .jobs
                        .claim_xc_live(candidate, owner.clone())
                        .await
                        .map_err(|_| D::Operational("XC revoked claim outcome".into()))?;
                    if !inner
                        .jobs
                        .write_xc_live_revoked(&message, owner)
                        .await
                        .map_err(|_| D::Operational("XC revoked backend outcome".into()))?
                    {
                        return Err(D::Operational("XC revoked backend rejected".into()));
                    }
                    return Ok(Some(D::Revoked(message.id)));
                }
                // HTTP account creation publishes a real unparented Groups
                // root. Its inactive STD branch needs neither the parent-only
                // scanner protocol nor the active uploaded/remote input domain.
                // All parented work and active rows keep their existing path.
                if direct_std_groups(&candidate.message) {
                    let session = match inactive_std_session(
                        &inner.auth,
                        candidate.message.account,
                        owner.clone(),
                    )
                    .await
                    {
                        Ok(value) => value,
                        Err(D::Capacity) => return Ok(Some(D::Capacity)),
                        Err(error) => return Err(error),
                    };
                    if let Some(session) = session {
                        let message = match inner.jobs.claim_xc_live(candidate, owner.clone()).await
                        {
                            Ok(message) => message,
                            Err(crate::transport::Error::ClaimRace) => {
                                session
                                    .close()
                                    .await
                                    .map_err(|_| D::Operational("STD root SQL close".into()))?;
                                return Ok(Some(D::HeadChanged));
                            }
                            Err(_) => {
                                let _ = session.close().await;
                                return Err(D::Operational("STD root claim unsettled".into()));
                            }
                        };
                        return finish_inactive_std_groups(&inner, session, message, owner.clone())
                            .await
                            .map(Some);
                    }
                }
                let prepared = if inner.auto_jobs.is_some() {
                    dispatcharr_store::xc_live_tasks::prepare_with_auto(
                        inner.store.clone(),
                        inner.auth.clone(),
                        candidate.message.account,
                        owner.clone(),
                        inner.events.clone(),
                    )
                    .await
                } else {
                    dispatcharr_store::xc_live_tasks::prepare(
                        inner.store.clone(),
                        inner.auth.clone(),
                        candidate.message.account,
                        owner.clone(),
                        inner.events.clone(),
                    )
                    .await
                };
                let session = match prepared {
                    Ok(Some(v)) => v,
                    Ok(None) => return Ok(None),
                    Err(StoreError::StatusBody(503, _)) => return Ok(Some(D::Capacity)),
                    Err(StoreError::Unsupported(_)) => return Err(D::UnsupportedDomain),
                    Err(_) => return Err(D::Operational("XC live SQL admission".into())),
                };
                let path = inner
                    .cache_root
                    .join(format!("{}.json", candidate.message.account));
                let retained = owner.clone();
                let cache = tokio::task::spawn_blocking(move || {
                    let _owner = retained;
                    match std::fs::symlink_metadata(path) {
                        Ok(_) => Ok(true),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
                        Err(_) => Err(()),
                    }
                })
                .await;
                if !matches!(cache, Ok(Ok(false))) {
                    session
                        .close()
                        .await
                        .map_err(|_| D::Operational("XC cache admission SQL close".into()))?;
                    return Err(D::UnsupportedDomain);
                }
                if inner.closed.load(Ordering::Acquire) {
                    session
                        .close()
                        .await
                        .map_err(|_| D::Operational("XC live close".into()))?;
                    return Err(D::Operational("XC live admission stopped".into()));
                }
                let message = match inner.jobs.claim_xc_live(candidate, owner.clone()).await {
                    Ok(v) => v,
                    Err(crate::transport::Error::ClaimRace) => {
                        session
                            .close()
                            .await
                            .map_err(|_| D::Operational("XC live close".into()))?;
                        return Ok(Some(D::HeadChanged));
                    }
                    Err(_) => {
                        let _ = session.close().await;
                        return Err(D::Operational("XC live claim unsettled".into()));
                    }
                };
                let key = match message.kind {
                    Kind::Groups => "refresh_m3u_account_groups",
                    Kind::Single => "refresh_single_m3u_account",
                };
                let lock = match inner
                    .locks
                    .acquire_retained_detailed(key, message.account, owner.clone())
                    .await
                {
                    Ok(v) => Some(v),
                    Err(TaskLockDetailedError::Operational(TaskLockError::Held)) => None,
                    Err(_) => {
                        let _ = session.close().await;
                        return Err(D::Operational("XC live lock outcome".into()));
                    }
                };
                let flow = if message.kind == Kind::Single && inner.auto_jobs.is_some() {
                    let stop = inner.clone();
                    Some(
                        inner.flows.track(
                            crate::auto_sync_flow::Flow::new(
                                inner.jobs.clone(),
                                inner.identity.clone(),
                                inner.cache_root.clone(),
                                message.id,
                                message.parent,
                                message.root,
                                owner.clone(),
                                Arc::new(move || stop.closed.load(Ordering::Acquire)),
                            )
                            .with_sd(inner.sd.get().cloned()),
                        ),
                    )
                } else {
                    None
                };
                let mut children = Vec::new();
                let work: Result<Value, D> = async {
                    if lock.is_none() {
                        return Ok(if message.kind == Kind::Groups {
                            json!([
                                format!("Task already running for account_id={}.", message.account),
                                null
                            ])
                        } else {
                            json!(format!(
                                "Task already running for account_id={}.",
                                message.account
                            ))
                        });
                    }
                    let start = chrono::Utc::now();
                    let elapsed = std::time::Instant::now();
                    if message.kind == Kind::Single {
                        session.begin_single().await.map_err(domain)?;
                    }
                    // Source Single directly calls Groups, which has its OWN token
                    // and releases it before stream collection/import starts.
                    let group_lock = if message.kind == Kind::Single {
                        match inner
                            .locks
                            .acquire_retained_detailed(
                                "refresh_m3u_account_groups",
                                message.account,
                                owner.clone(),
                            )
                            .await
                        {
                            Ok(v) => Some(v),
                            Err(TaskLockDetailedError::Operational(TaskLockError::Held)) => {
                                return Err(D::UnsupportedDomain)
                            }
                            Err(_) => return Err(D::Operational("XC nested groups lock".into())),
                        }
                    } else {
                        None
                    };
                    let group_work = async {
                        if session.publishes_profiles() {
                            match publish_profile(&inner, &message, owner.clone()).await {
                                Ok(receipt) => {
                                    children.push(receipt.id);
                                    if let Some(flow) = &flow {
                                        flow.append_child(&receipt).await.map_err(|_| {
                                            D::Operational(
                                                "XC actual child metadata append failed".into(),
                                            )
                                        })?;
                                    }
                                }
                                Err(crate::transport::Error::Capacity) => {}
                                Err(_) => {
                                    return Err(D::Operational(
                                        "XC profile child publication unsettled".into(),
                                    ))
                                }
                            }
                        }
                        session
                            .groups(message.kind == Kind::Single || message.full, start)
                            .await
                            .map_err(domain)
                    }
                    .await;
                    if let Some(group_lock) = group_lock {
                        if !group_lock.release_owned_detailed().await.is_ok_and(|v| v) {
                            return Err(D::Operational("XC nested group token release".into()));
                        }
                    }
                    let groups = group_work?;
                    if message.kind == Kind::Groups {
                        return Ok(groups.result);
                    }
                    if groups.result[1].is_null() {
                        return Ok(json!(
                            "Failed to update m3u account - download failed or other error"
                        ));
                    }
                    let completion = if let (Some(jobs), Some(flow)) = (&inner.auto_jobs, &flow) {
                        session
                            .single_with_auto(start, elapsed, flow.services(jobs.clone()))
                            .await
                    } else {
                        session.single(start, elapsed).await
                    }
                    .map_err(domain)?;
                    if completion.invalidate {
                        inner.ports.invalidate(owner.clone()).await.map_err(|_| {
                            D::Operational("XC cache invalidation unsettled".into())
                        })?;
                    }
                    Ok(completion.result)
                }
                .await;
                let regex_closed = if let Some(flow) = &flow {
                    flow.retire(Duration::from_secs(5)).await.is_ok()
                } else {
                    true
                };
                let closed = session.close().await;
                let unlocked = if let Some(lock) = lock {
                    lock.release_owned_detailed().await.is_ok_and(|v| v)
                } else {
                    true
                };
                if closed.is_err() || !unlocked || !regex_closed {
                    return Err(D::Operational(
                        "XC live physical drain or token release".into(),
                    ));
                }
                let result = work?;
                let ack = if let Some(flow) = &flow {
                    flow.complete(result).await
                } else {
                    inner
                        .jobs
                        .write_xc_live(&message, result, children, owner)
                        .await
                }
                .map_err(|_| D::Operational("XC live backend unsettled".into()))?;
                if !ack {
                    return Err(D::Operational("XC live backend rejected".into()));
                }
                Ok(Some(D::Completed(message.id)))
            }
            .await;
            if result.is_ok() {
                guard.armed = false;
            }
            result
        })
        .await;
        let result = match joined {
            Ok(v) => v,
            Err(_) => Err(D::Operational("XC live supervisor lost".into())),
        };
        if result.is_err() {
            let mut held = failure_inner
                .uncertain
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if !held.iter().any(|v| Arc::ptr_eq(v, &failure_owner)) {
                held.push(failure_owner)
            }
        }
        result
    }
}
struct ProfilePublication {
    id: Uuid,
    parent: Uuid,
    root: Uuid,
}
impl crate::full_metadata::ActualPublishedChildren for ProfilePublication {
    fn parent_task_id(&self) -> Uuid {
        self.parent
    }
    fn root_task_id(&self) -> Uuid {
        self.root
    }
    fn children(
        &self,
        after: i64,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<Vec<(i64, Uuid)>, crate::transport::Error>>
    {
        Box::pin(async move {
            Ok(if after < 1 {
                vec![(1, self.id)]
            } else {
                Vec::new()
            })
        })
    }
}
async fn publish_profile(
    inner: &Inner,
    message: &Message,
    owner: Owner,
) -> Result<ProfilePublication, crate::transport::Error> {
    let identity = inner
        .identity
        .current()
        .map_err(|_| crate::transport::Error::Protocol)?;
    let id = Uuid::new_v4();
    let raw = crate::protocol::encode(
        &crate::protocol::Task::CreateBackup,
        id,
        identity.reply_to,
        Uuid::new_v4(),
        &identity.origin,
    )
    .map_err(|_| crate::transport::Error::Protocol)?;
    let mut frame: Value =
        serde_json::from_slice(&raw).map_err(|_| crate::transport::Error::Protocol)?;
    frame["headers"]["task"] = json!("apps.m3u.tasks.refresh_account_profiles");
    frame["headers"]["argsrepr"] = json!(format!("({},)", message.account));
    frame["headers"]["root_id"] = json!(message.root);
    frame["headers"]["parent_id"] = json!(message.id);
    frame["body"]=json!(STANDARD.encode(serde_json::to_vec(&json!([[message.account],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).map_err(|_|crate::transport::Error::Protocol)?));
    let raw = serde_json::to_vec(&frame).map_err(|_| crate::transport::Error::Protocol)?;
    let receipt = inner.jobs.publish_xc_profile_child(&raw, owner).await?;
    if receipt.task_id != id {
        return Err(crate::transport::Error::Corrupt);
    }
    Ok(ProfilePublication {
        id,
        parent: message.id,
        root: message.root,
    })
}

/// Admission is deliberately limited to the actual create producer's default
/// Groups root. Surviving parented scanner children remain byte-for-byte on the
/// original route and do not acquire an invented parent/root receipt here.
fn direct_std_groups(message: &Message) -> bool {
    message.kind == Kind::Groups && message.parent.is_none() && !message.full && !message.use_cache
}
fn std_groups_early_result(account: i64, already_running: bool) -> Value {
    json!([
        if already_running {
            format!("Task already running for account_id={account}.")
        } else {
            format!("M3UAccount with ID={account} not found or inactive.")
        },
        null
    ])
}
/// Read-only admission owns the original Store four-writer budget and closes
/// the SAME physical session for every decline/error. No cache/settings/UA,
/// provider, parser, file or account-save operation runs on the inactive path.
async fn inactive_std_session(
    auth: &AuthService,
    account: i64,
    owner: Owner,
) -> Result<Option<dispatcharr_store::TaskSqlSession>, D> {
    let session = auth
        .borrow_task_sql_session(owner)
        .await
        .map_err(|error| match error {
            StoreError::StatusBody(503, _) => D::Capacity,
            _ => D::Operational("STD root SQL admission".into()),
        })?;
    let observed = session
        .run(move |c| {
            Box::pin(async move {
                sqlx::query_scalar::<_, bool>(
                    "SELECT account_type='STD' AND NOT is_active FROM m3u_m3uaccount WHERE id=$1",
                )
                .bind(account)
                .fetch_optional(c)
                .await
            })
        })
        .await;
    if matches!(observed, Ok(Ok(Some(true)))) {
        return Ok(Some(session));
    }
    let closed = session.close().await;
    match observed {
        Ok(Ok(_)) => {
            closed.map_err(|_| D::Operational("STD root declined SQL close".into()))?;
            Ok(None)
        }
        _ => Err(D::Operational(
            "STD root account observation unsettled".into(),
        )),
    }
}
async fn finish_inactive_std_groups(
    inner: &Arc<Inner>,
    session: dispatcharr_store::TaskSqlSession,
    message: Message,
    owner: Owner,
) -> Result<D, D> {
    // Source acquires this actual lock before its account lookup, and releases
    // it before Celery records SUCCESS. Keep real contention/token ownership.
    let lock = match inner
        .locks
        .acquire_retained_detailed("refresh_m3u_account_groups", message.account, owner.clone())
        .await
    {
        Ok(value) => Some(value),
        Err(TaskLockDetailedError::Operational(TaskLockError::Held)) => None,
        Err(_) => {
            let _ = session.close().await;
            return Err(D::Operational("STD root lock unsettled".into()));
        }
    };
    let outcome = if lock.is_none() {
        Ok(std_groups_early_result(message.account, true))
    } else {
        let account = message.account;
        // Re-observe at Source's actual lookup boundary after lock acquisition.
        // A concurrently reactivated account is not falsely declared inactive;
        // that wider race domain remains an actual fault requiring qualification.
        match session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query_scalar::<_, bool>(
                        "SELECT EXISTS(SELECT 1 FROM m3u_m3uaccount WHERE id=$1 AND is_active)",
                    )
                    .bind(account)
                    .fetch_one(c)
                    .await
                })
            })
            .await
        {
            Ok(Ok(false)) => Ok(std_groups_early_result(account, false)),
            Ok(Ok(true)) => Err(D::UnsupportedDomain),
            _ => Err(D::Operational(
                "STD root locked account lookup unsettled".into(),
            )),
        }
    };
    let unlocked = if let Some(lock) = lock {
        lock.release_owned_detailed().await.is_ok_and(|v| v)
    } else {
        true
    };
    let closed = session.close().await;
    if !unlocked || closed.is_err() {
        return Err(D::Operational(
            "STD root lock release or physical SQL close".into(),
        ));
    }
    let result = outcome?;
    let acknowledged = inner
        .jobs
        .write_xc_live(&message, result, Vec::new(), owner)
        .await
        .map_err(|_| D::Operational("STD root terminal metadata unsettled".into()))?;
    if !acknowledged {
        return Err(D::Operational("STD root terminal metadata rejected".into()));
    }
    Ok(D::Completed(message.id))
}

fn domain(error: StoreError) -> D {
    match error {
        StoreError::Unsupported(_) => D::UnsupportedDomain,
        _ => D::Operational("XC live effects unsettled".into()),
    }
}

#[cfg(test)]
mod source_controls {
    use super::*;
    fn frame(kind: Kind, parent: Option<Uuid>, root: Uuid) -> Vec<u8> {
        let id = if parent.is_none() {
            root
        } else {
            Uuid::new_v4()
        };
        let bytes = crate::protocol::encode(
            &crate::protocol::Task::CreateBackup,
            id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "gen42@owned-host",
        )
        .unwrap();
        let mut value: Value = serde_json::from_slice(&bytes).unwrap();
        value["headers"]["task"] = json!(kind.name());
        value["headers"]["argsrepr"] = json!("(17,)");
        value["headers"]["root_id"] = json!(root);
        value["headers"]["parent_id"] = json!(parent);
        value["headers"]["timelimit"] = match kind {
            Kind::Groups => json!([null, null]),
            Kind::Single => json!([3600, 3500]),
        };
        value["body"] = json!(STANDARD.encode(
            serde_json::to_vec(
                &json!([[17],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
            )
            .unwrap()
        ));
        serde_json::to_vec(&value).unwrap()
    }
    #[test]
    fn direct_inactive_std_admission_preserves_parented_scanner_and_single_domains() {
        let root = Uuid::new_v4();
        let parent = Uuid::new_v4();
        let mut direct = parse(&frame(Kind::Groups, None, root)).unwrap();
        assert!(direct_std_groups(&direct));
        assert_eq!(direct.parent, None);
        let child = parse(&frame(Kind::Groups, Some(parent), root)).unwrap();
        assert!(!direct_std_groups(&child));
        assert_eq!(child.parent, Some(parent));
        assert!(!direct_std_groups(
            &parse(&frame(Kind::Single, None, root)).unwrap()
        ));
        direct.full = true;
        assert!(!direct_std_groups(&direct));
        direct.full = false;
        direct.use_cache = true;
        assert!(!direct_std_groups(&direct));
    }
    #[test]
    fn direct_inactive_std_results_match_source_missing_inactive_and_lock_contention_tuples() {
        // Retained Source1 account-create terminal returned this exact tuple.
        assert_eq!(
            std_groups_early_result(5, false),
            json!(["M3UAccount with ID=5 not found or inactive.", null])
        );
        // Original apps.m3u.tasks.refresh_m3u_groups early lock branch.
        assert_eq!(
            std_groups_early_result(5, true),
            json!(["Task already running for account_id=5.", null])
        );
    }
    #[test]
    fn xc_live_root_and_surviving_parented_frames_keep_real_lineage() {
        let root = Uuid::new_v4();
        let parent = Uuid::new_v4();
        let direct = parse(&frame(Kind::Single, None, root)).unwrap();
        assert_eq!(direct.root, root);
        assert_eq!(direct.parent, None);
        let descendant = parse(&frame(Kind::Groups, Some(parent), root)).unwrap();
        assert_eq!(descendant.parent, Some(parent));
        assert_eq!(descendant.root, root);
        assert_eq!(descendant.account, 17);
    }
    #[test]
    fn xc_live_actual_beat_keyword_target_is_not_an_unsupported_fifo_head() {
        let raw = frame(Kind::Single, None, Uuid::new_v4());
        let mut value: Value = serde_json::from_slice(&raw).unwrap();
        value["headers"]["argsrepr"] = json!("()");
        value["headers"]["kwargsrepr"] = json!("{'account_id': 17}");
        value["headers"]["periodic_task_name"] = json!("m3u_account-refresh-17");
        value["body"]=json!(STANDARD.encode(serde_json::to_vec(&json!([[],{"account_id":17},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).unwrap()));
        assert_eq!(
            parse(&serde_json::to_vec(&value).unwrap()).unwrap().account,
            17
        );
        value["headers"]["kwargsrepr"] = json!("{'account_id': 18}");
        assert!(parse(&serde_json::to_vec(&value).unwrap()).is_err());
    }
    #[test]
    fn xc_live_frame_validation_rejects_wrong_task_time_limit_and_correlation() {
        let raw = frame(Kind::Single, None, Uuid::new_v4());
        let mut value: Value = serde_json::from_slice(&raw).unwrap();
        value["headers"]["timelimit"] = json!([null, null]);
        assert!(parse(&serde_json::to_vec(&value).unwrap()).is_err());
        value = serde_json::from_slice(&raw).unwrap();
        value["properties"]["correlation_id"] = json!(Uuid::new_v4());
        assert!(parse(&serde_json::to_vec(&value).unwrap()).is_err());
        value = serde_json::from_slice(&raw).unwrap();
        value["headers"]["task"] = json!("apps.vod.tasks.refresh_vod_content");
        assert!(parse(&serde_json::to_vec(&value).unwrap()).is_err());
    }
}

#[path = "normal_m3u_all.rs"]
mod all;

#[path = "normal_vod_ingestion.rs"]
mod vod_ingestion;
