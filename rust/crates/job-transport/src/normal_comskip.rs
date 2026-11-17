//! Manual Comskip over the actual shared Celery root/CAS/metadata lifecycle.
//! Original Dispatcharr/Celery/Kombu authors retain protocol/application credit.
use crate::{
    milestone_dispatch::{Disposition as D, Handler, Lifecycle},
    normal_revocation::Revoked,
    transport::{Error, Jobs},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use dispatcharr_contracts::{BoxFuture, UpdateEventSink};
use dispatcharr_store::{comskip_task as task, AuthService, ProducerIdentitySource, StoreError};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use uuid::Uuid;
type Scope = Arc<dyn Send + Sync>;
pub const TASK: &str = "apps.channels.tasks.comskip_process_recording";
#[derive(Clone, Debug)]
pub(crate) struct Message {
    pub id: Uuid,
    pub recording: i64,
    pub parent: Option<String>,
}
pub(crate) fn parse(raw: &[u8]) -> Result<Message, ()> {
    if raw.is_empty() || raw.len() > crate::protocol::MAX_ENVELOPE {
        return Err(());
    }
    let mut frame = crate::clocked_recording_envelope::json_value(raw).map_err(|_| ())?;
    if frame["headers"]["task"] != TASK {
        return Err(());
    }
    let bytes = STANDARD
        .decode(frame["body"].as_str().ok_or(())?)
        .map_err(|_| ())?;
    if bytes.len() > 4096 {
        return Err(());
    }
    let body = crate::clocked_recording_envelope::json_value(&bytes).map_err(|_| ())?;
    let p = body.as_array().filter(|v| v.len() == 3).ok_or(())?;
    let args = p[0].as_array().filter(|v| v.len() == 1).ok_or(())?;
    let recording = args[0]
        .as_i64()
        .filter(|v| *v > 0 && *v <= i64::from(i32::MAX))
        .ok_or(())?;
    if p[1] != json!({})
        || p[2] != json!({"callbacks":null,"errbacks":null,"chain":null,"chord":null})
    {
        return Err(());
    }
    if frame["headers"]["argsrepr"] != format!("({recording},)") {
        return Err(());
    }
    let id = frame["headers"]["id"].as_str().ok_or(())?.to_owned();
    let root = frame["headers"]["root_id"].as_str().ok_or(())?.to_owned();
    let parent = match &frame["headers"]["parent_id"] {
        Value::Null => {
            if root != id {
                return Err(());
            }
            None
        }
        Value::String(parent) => {
            if root == id || parent == &id || !lineage_key(parent) || !lineage_key(&root) {
                return Err(());
            }
            Some(parent.clone())
        }
        _ => return Err(()),
    };
    // The shared no-argument validator checks the normalized common framing;
    // actual nested lineage above remains in the original CAS bytes and result.
    frame["headers"]["root_id"] = json!(id);
    frame["headers"]["parent_id"] = Value::Null;
    frame["headers"]["task"] = json!("apps.proxy.tasks.fetch_channel_stats");
    frame["headers"]["argsrepr"] = json!("()");
    frame["headers"]["kwargsrepr"] = json!("{}");
    frame["body"] = json!(STANDARD.encode(
        serde_json::to_vec(
            &json!([[],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        )
        .map_err(|_| ())?
    ));
    let common = crate::stats_protocol::parse(&serde_json::to_vec(&frame).map_err(|_| ())?)
        .map_err(|_| ())?;
    Ok(Message {
        id: common.task_id,
        recording,
        parent,
    })
}
fn lineage_key(key: &str) -> bool {
    if let Ok(id) = Uuid::parse_str(key) {
        return id.to_string() == key
            && id.get_version_num() == 4
            && id.get_variant() == uuid::Variant::RFC4122;
    }
    if let Some(recording) = key.strip_prefix("dvr-recover-") {
        return recording
            .parse::<i32>()
            .is_ok_and(|id| id > 0 && id.to_string() == recording);
    }
    if let Some(value) = key.strip_prefix("dvr-now-") {
        if let Some((recording, nonce)) = value.split_once('-') {
            return recording
                .parse::<i32>()
                .is_ok_and(|id| id > 0 && id.to_string() == recording)
                && Uuid::parse_str(nonce).is_ok_and(|id| {
                    id.simple().to_string() == nonce
                        && id.get_version_num() == 4
                        && id.get_variant() == uuid::Variant::RFC4122
                });
        }
    }
    false
}
struct Inner {
    jobs: Jobs,
    auth: AuthService,
    events: Arc<dyn UpdateEventSink>,
    identity: ProducerIdentitySource,
    revoked: Revoked,
    closed: Arc<AtomicBool>,
    active: AtomicUsize,
    changed: tokio::sync::Notify,
    uncertain: Mutex<Vec<Scope>>,
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
        identity: ProducerIdentitySource,
        revoked: Revoked,
    ) -> Result<Self, D> {
        if !jobs.allows_defaults() {
            return Err(D::UnsupportedDomain);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                auth,
                events,
                identity,
                revoked,
                closed: Arc::new(AtomicBool::new(false)),
                active: AtomicUsize::new(0),
                changed: tokio::sync::Notify::new(),
                uncertain: Mutex::new(Vec::new()),
            }),
        })
    }
    pub fn handler(&self) -> Arc<dyn Handler> {
        Arc::new(Worker(self.inner.clone()))
    }
    pub async fn publish(&self, recording: i64, owner: Scope) -> Result<Uuid, StoreError> {
        self.publish_context(recording, None, owner).await
    }
    pub(crate) fn uses_store(&self, store: &dispatcharr_store::Store) -> bool {
        self.inner.auth.uses_store_pool(store)
    }
    pub(crate) async fn publish_after_recording(
        &self,
        recording: i64,
        parent: String,
        root: String,
        owner: Scope,
    ) -> Result<Uuid, StoreError> {
        if !lineage_key(&parent) || !lineage_key(&root) {
            return Err(StoreError::Unsupported(
                "Comskip recording lineage unavailable",
            ));
        }
        self.publish_context(recording, Some((parent, root)), owner)
            .await
    }
    async fn publish_context(
        &self,
        recording: i64,
        lineage: Option<(String, String)>,
        owner: Scope,
    ) -> Result<Uuid, StoreError> {
        if self.inner.closed.load(Ordering::Acquire)
            || recording <= 0
            || recording > i64::from(i32::MAX)
        {
            return Err(StoreError::Unsupported("Comskip intake unavailable"));
        }
        let id = Uuid::new_v4();
        let identity = self.inner.identity.current()?;
        let raw = crate::protocol::encode(
            &crate::protocol::Task::CreateBackup,
            id,
            identity.reply_to,
            Uuid::new_v4(),
            &identity.origin,
        )
        .map_err(|_| StoreError::UnhandledRestFault)?;
        let mut frame: Value =
            serde_json::from_slice(&raw).map_err(|_| StoreError::UnhandledRestFault)?;
        frame["headers"]["task"] = json!(TASK);
        frame["headers"]["argsrepr"] = json!(format!("({recording},)"));
        frame["headers"]["timelimit"] = json!([null, null]);
        if let Some((parent, root)) = lineage {
            frame["headers"]["parent_id"] = json!(parent);
            frame["headers"]["root_id"] = json!(root);
        }
        frame["body"]=json!(STANDARD.encode(serde_json::to_vec(&json!([[recording],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).map_err(|_|StoreError::UnhandledRestFault)?));
        let raw = serde_json::to_vec(&frame).map_err(|_| StoreError::UnhandledRestFault)?;
        self.inner
            .jobs
            .publish_comskip_task(id, raw, owner)
            .await
            .map_err(|_| StoreError::UnhandledRestFault)?;
        Ok(id)
    }
}
struct Owner {
    transcript: Arc<crate::recording_process::Transcript>,
    _caller: Scope,
    inner: Arc<Inner>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.inner.active.fetch_sub(1, Ordering::AcqRel);
        self.inner.changed.notify_waiters();
    }
}
struct Worker(Arc<Inner>);
fn retain(inner: &Inner, owner: Scope) {
    inner
        .uncertain
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(owner);
}
impl Worker {
    async fn run(&self, expected: Option<Arc<[u8]>>, caller: Scope) -> Result<D, D> {
        let inner = self.0.clone();
        if inner.closed.load(Ordering::Acquire) {
            return Ok(D::Empty);
        }
        inner.active.fetch_add(1, Ordering::AcqRel);
        let owner = Arc::new(Owner {
            transcript: Arc::new(crate::recording_process::Transcript::new_normal()),
            _caller: caller,
            inner: inner.clone(),
        });
        let fallback = inner.clone();
        let held: Scope = owner.clone();
        match tokio::spawn(async move {
            let candidate = match inner
                .jobs
                .peek_comskip_task(expected.as_deref(), owner.clone())
                .await
            {
                Ok(Some(v)) => v,
                Ok(None) => return Ok(D::Empty),
                Err(Error::ClaimRace) => return Ok(D::HeadChanged),
                Err(Error::UnsupportedTask) => return Err(D::UnsupportedHead),
                Err(_) => return Err(D::Operational("Comskip peek unsettled".into())),
            };
            // Plugin namespace guard is existing physical admission, never a JSON flag.
            dispatcharr_store::m3u_refresh_all::require_supported_domain(
                &inner.auth,
                owner.clone(),
            )
            .await
            .map_err(|_| D::UnsupportedDomain)?;
            let _admission_plan =
                task::preflight(&inner.auth, candidate.message.recording, owner.clone())
                    .await
                    .map_err(|_| D::UnsupportedDomain)?;
            if inner.closed.load(Ordering::Acquire) {
                return Ok(D::Empty);
            }
            let message = match inner
                .jobs
                .claim_comskip_task(candidate, owner.clone())
                .await
            {
                Ok(v) => v,
                Err(Error::ClaimRace) => return Ok(D::HeadChanged),
                Err(_) => {
                    retain(&inner, owner);
                    return Err(D::Operational("Comskip CAS unsettled".into()));
                }
            };
            let operation = async {
                let revoked = inner
                    .revoked
                    .contains(&message.id.to_string())
                    .map_err(|_| D::Operational("Comskip revocation lookup unsettled".into()))?;
                let result = if revoked {
                    Value::Null
                } else {
                    let plan = task::preflight(&inner.auth, message.recording, owner.clone())
                        .await
                        .map_err(|_| D::Operational("Claimed comskip domain changed".into()))?;
                    let media = Arc::new(crate::comskip_media::ActualMedia {
                        shutdown: inner.closed.clone(),
                        events: inner.events.clone(),
                        transcript: owner.transcript.clone(),
                    });
                    task::execute(
                        &inner.auth,
                        inner.events.clone(),
                        plan,
                        media,
                        owner.clone(),
                    )
                    .await
                    .map_err(|_| D::Operational("Comskip execution owner unsettled".into()))?
                };
                inner
                    .jobs
                    .write_comskip_task(&message, result, revoked, owner.clone())
                    .await
                    .map_err(|_| D::Operational("Comskip terminal write unsettled".into()))?;
                Ok(if revoked {
                    D::Revoked(message.id)
                } else {
                    D::Completed(message.id)
                })
            }
            .await;
            if operation.is_err() {
                retain(&inner, owner);
            }
            operation
        })
        .await
        {
            Ok(v) => v,
            Err(_) => {
                retain(&fallback, held);
                Err(D::Operational("Comskip worker join unsettled".into()))
            }
        }
    }
}
impl Handler for Worker {
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn run_selected(&self, expected: Arc<[u8]>, owner: Scope) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.run(Some(expected), owner))
    }
    fn run_once(&self, owner: Scope) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.run(None, owner))
    }
}
impl Lifecycle for Worker {
    fn begin_drain(&self, _budget: Duration) {
        self.stop_admission();
    }
    fn stop_admission(&self) {
        self.0.closed.store(true, Ordering::Release);
    }
    fn shutdown(&self, timeout: Duration) -> BoxFuture<'_, Result<(), D>> {
        Box::pin(async move {
            self.stop_admission();
            tokio::time::timeout(timeout, async {
                loop {
                    let n = self.0.changed.notified();
                    tokio::pin!(n);
                    n.as_mut().enable();
                    if self.0.active.load(Ordering::Acquire) == 0 {
                        break;
                    }
                    n.await;
                }
            })
            .await
            .map_err(|_| D::Operational("Comskip physical drain incomplete".into()))
        })
    }
}
impl dispatcharr_store::comskip_http::Publisher for Prepared {
    fn auth_service(&self) -> AuthService {
        self.inner.auth.clone()
    }
    fn admission(&self) -> Result<(), StoreError> {
        if self.inner.closed.load(Ordering::Acquire) {
            Err(StoreError::Unsupported("Comskip prepared producer closed"))
        } else {
            Ok(())
        }
    }
    fn publish(&self, recording: i64, owner: Scope) -> BoxFuture<'_, Result<Uuid, StoreError>> {
        Box::pin(Prepared::publish(self, recording, owner))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn frame() -> Value {
        let id = Uuid::new_v4();
        let raw = crate::protocol::encode(
            &crate::protocol::Task::CreateBackup,
            id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "owned-source-comskip-control",
        )
        .unwrap();
        let mut v: Value = serde_json::from_slice(&raw).unwrap();
        v["headers"]["task"] = json!(TASK);
        v["headers"]["timelimit"] = json!([null, null]);
        v["headers"]["argsrepr"] = json!("(12,)");
        v["body"] = json!(STANDARD.encode(
            serde_json::to_vec(
                &json!([[12],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
            )
            .unwrap()
        ));
        v
    }
    #[test]
    fn same_root_accepts_real_integer_but_refuses_foreign_lineage_and_callbacks() {
        let good = frame();
        assert_eq!(
            parse(&serde_json::to_vec(&good).unwrap())
                .unwrap()
                .recording,
            12
        );
        for (key, value) in [
            ("parent_id", json!(Uuid::new_v4())),
            ("root_id", json!(Uuid::new_v4())),
            ("eta", json!("2027-01-01T00:00:00+00:00")),
        ] {
            let mut bad = good.clone();
            bad["headers"][key] = value;
            assert!(parse(&serde_json::to_vec(&bad).unwrap()).is_err());
        }
        for body in [
            json!([[12],{},{"callbacks":[{"task":"foreign"}],"errbacks":null,"chain":null,"chord":null}]),
            json!([["12"],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}]),
        ] {
            let mut bad = good.clone();
            bad["body"] = json!(STANDARD.encode(serde_json::to_vec(&body).unwrap()));
            assert!(parse(&serde_json::to_vec(&bad).unwrap()).is_err());
        }
    }
    #[test]
    fn nested_recording_comskip_keeps_original_parent_without_relaxing_common_frame() {
        let mut value = frame();
        let parent = Uuid::new_v4().to_string();
        value["headers"]["parent_id"] = json!(parent);
        value["headers"]["root_id"] = json!(parent);
        let message = parse(&serde_json::to_vec(&value).unwrap()).unwrap();
        assert_eq!(message.parent.as_deref(), Some(parent.as_str()));
        value["headers"]["parent_id"] = value["headers"]["id"].clone();
        assert!(parse(&serde_json::to_vec(&value).unwrap()).is_err());
        value["headers"]["parent_id"] = json!(parent);
        value["headers"]["root_id"] = json!("foreign");
        assert!(parse(&serde_json::to_vec(&value).unwrap()).is_err());
    }
}
