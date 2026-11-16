//! Native channel matching roots and real descendants. Original Dispatcharr/Celery authors retain credit.
use crate::{
    milestone_dispatch::{Disposition as D, Handler, Lifecycle},
    normal_revocation::Revoked,
    transport::{Error, Jobs},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use dispatcharr_contracts::{BoxFuture, UpdateEventSink};
use dispatcharr_store::{
    channel_epg_match_task::{self as task, MatchingChild, MatchingEffects, Request},
    AuthService, ProducerIdentitySource, StoreError,
};
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    All,
    Selected,
    Single,
}
impl Kind {
    pub fn task(self) -> &'static str {
        match self {
            Self::All => "apps.channels.tasks.match_epg_channels",
            Self::Selected => "apps.channels.tasks.match_selected_channels_epg",
            Self::Single => "apps.channels.tasks.match_single_channel_epg",
        }
    }
}
fn kind(request: &Request) -> Kind {
    match request {
        Request::All => Kind::All,
        Request::Selected(_) => Kind::Selected,
        Request::Single(_) => Kind::Single,
    }
}
#[derive(Clone, Debug)]
pub(crate) struct Message {
    pub id: Uuid,
    pub request: Request,
}
pub(crate) fn parse(raw: &[u8]) -> Result<Message, ()> {
    if raw.is_empty() || raw.len() > crate::protocol::MAX_ENVELOPE {
        return Err(());
    }
    let mut frame = crate::clocked_recording_envelope::json_value(raw).map_err(|_| ())?;
    let selected = [Kind::All, Kind::Selected, Kind::Single]
        .into_iter()
        .find(|k| frame["headers"]["task"] == k.task())
        .ok_or(())?;
    let bytes = STANDARD
        .decode(frame["body"].as_str().ok_or(())?)
        .map_err(|_| ())?;
    if bytes.len() > 32768 {
        return Err(());
    }
    let body = crate::clocked_recording_envelope::json_value(&bytes).map_err(|_| ())?;
    let p = body.as_array().filter(|v| v.len() == 3).ok_or(())?;
    let args = p[0].as_array().ok_or(())?;
    if p[1] != json!({})
        || p[2] != json!({"callbacks":null,"errbacks":null,"chain":null,"chord":null})
    {
        return Err(());
    }
    let request = match (selected, args.as_slice()) {
        (Kind::All, []) => Request::All,
        (Kind::Selected, [v]) => Request::Selected(v.clone()),
        (Kind::Single, [v]) => Request::Single(v.clone()),
        _ => return Err(()),
    };
    // Common Source root authority is validated by the existing strict parser:
    // no parent/callback/ETA/foreign headers, root_id must equal the actual UUID.
    frame["body"] = json!(STANDARD.encode(
        serde_json::to_vec(
            &json!([[],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        )
        .map_err(|_| ())?
    ));
    frame["headers"]["task"] = json!("apps.proxy.tasks.fetch_channel_stats");
    frame["headers"]["argsrepr"] = json!("()");
    frame["headers"]["kwargsrepr"] = json!("{}");
    let common = crate::stats_protocol::parse(&serde_json::to_vec(&frame).map_err(|_| ())?)
        .map_err(|_| ())?;
    Ok(Message {
        id: common.task_id,
        request,
    })
}
struct Inner {
    jobs: Jobs,
    sd: std::sync::OnceLock<crate::normal_sd_guide::Capability>,
    auth: AuthService,
    events: Arc<dyn UpdateEventSink>,
    identity: ProducerIdentitySource,
    revoked: Revoked,
    closed: AtomicBool,
    active: AtomicUsize,
    changed: tokio::sync::Notify,
    uncertain: Mutex<Vec<Scope>>,
}
impl task::MatchingCancellation for Inner {
    fn is_draining(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
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
                sd: std::sync::OnceLock::new(),
                auth,
                events,
                identity,
                revoked,
                closed: AtomicBool::new(false),
                active: AtomicUsize::new(0),
                changed: tokio::sync::Notify::new(),
                uncertain: Mutex::new(Vec::new()),
            }),
        })
    }
    pub fn bind_sd(&self, capability: crate::normal_sd_guide::Capability) -> Result<(), D> {
        if !capability.owns(&self.inner.jobs) {
            return Err(D::UnsupportedDomain);
        }
        self.inner
            .sd
            .set(capability)
            .map_err(|_| D::Operational("Matching SD capability already bound".into()))
    }
    pub fn handler(&self, kind: Kind) -> Arc<dyn Handler> {
        Arc::new(Worker {
            inner: self.inner.clone(),
            kind,
        })
    }
}
// No new semaphore: the actual ordinary dispatcher admission remains retained.
struct Owner {
    _caller: Scope,
    inner: Arc<Inner>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.inner.active.fetch_sub(1, Ordering::AcqRel);
        self.inner.changed.notify_waiters();
    }
}
struct Worker {
    inner: Arc<Inner>,
    kind: Kind,
}
fn retain(inner: &Inner, owner: Scope) {
    let mut held = inner.uncertain.lock().unwrap_or_else(|v| v.into_inner());
    if !held.iter().any(|v| Arc::ptr_eq(v, &owner)) {
        held.push(owner);
    }
}
impl Worker {
    async fn run(&self, expected: Option<Arc<[u8]>>, caller: Scope) -> Result<D, D> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Ok(D::Empty);
        }
        self.inner.active.fetch_add(1, Ordering::AcqRel);
        let owner = Arc::new(Owner {
            _caller: caller,
            inner: self.inner.clone(),
        });
        if self.inner.closed.load(Ordering::Acquire) {
            return Ok(D::Empty);
        }
        let held: Scope = owner.clone();
        let fallback = self.inner.clone();
        let inner = self.inner.clone();
        let wanted = self.kind;
        let result = tokio::spawn(async move {
            let candidate = match inner
                .jobs
                .peek_matching_task(expected.as_deref(), owner.clone())
                .await
            {
                Ok(Some(v)) => v,
                Ok(None) => return Ok(D::Empty),
                Err(Error::ClaimRace) => return Ok(D::HeadChanged),
                Err(Error::UnsupportedTask) => return Err(D::UnsupportedHead),
                Err(_) => return Err(D::Operational("Matching peek failed".into())),
            };
            if kind(&candidate.message.request) != wanted {
                return Err(D::UnsupportedHead);
            }
            if !inner
                .sd
                .get()
                .is_some_and(|capability| capability.owns(&inner.jobs))
            {
                return Err(D::UnsupportedDomain);
            }
            let plan = match task::preflight(
                &inner.auth,
                &candidate.message.request,
                owner.clone(),
                inner.clone(),
            )
            .await
            {
                Ok(Some(plan)) => plan,
                Ok(None) => return Ok(D::Empty),
                Err(StoreError::Unsupported(_)) => return Err(D::UnsupportedDomain),
                Err(_) => {
                    retain(&inner, owner);
                    return Err(D::Operational("Matching preflight unsettled".into()));
                }
            };
            if inner.closed.load(Ordering::Acquire) {
                return Ok(D::Empty);
            }
            let message = match inner
                .jobs
                .claim_matching_task(candidate, owner.clone())
                .await
            {
                Ok(v) => v,
                Err(Error::ClaimRace) => return Ok(D::HeadChanged),
                Err(_) => {
                    retain(&inner, owner);
                    return Err(D::Operational("Matching claim unsettled".into()));
                }
            };
            // Only successful CAS constructs the descendant authority. JSON cannot mint it.
            let effects = Arc::new(Effects {
                jobs: inner.jobs.clone(),
                identity: inner.identity.clone(),
                parent: message.id,
                children: Mutex::new(Vec::new()),
                failed: AtomicBool::new(false),
            });
            let operation = async {
                if inner
                    .revoked
                    .contains(&message.id.to_string())
                    .map_err(|_| D::Operational("Matching revoke lookup failed".into()))?
                {
                    inner
                        .jobs
                        .write_matching_task(
                            &message,
                            Value::Null,
                            Vec::new(),
                            true,
                            None,
                            owner.clone(),
                        )
                        .await
                        .map_err(|_| D::Operational("Matching REVOKED write unsettled".into()))?;
                    return Ok(D::Revoked(message.id));
                }
                let result = task::execute(
                    inner.auth.clone(),
                    inner.events.clone(),
                    plan,
                    message.id,
                    effects.clone(),
                    owner.clone(),
                )
                .await;
                if effects.failed.load(Ordering::Acquire) {
                    return Err(D::Operational(
                        "Matching child publication unsettled".into(),
                    ));
                }
                let children = effects
                    .children
                    .lock()
                    .map_err(|_| D::Operational("Matching trail poisoned".into()))?
                    .clone();
                let (value, failure) = match result {
                    Ok(value) => (value, None),
                    Err(task::ExecutionError::Raised { class, message }) => {
                        (Value::Null, Some((class, message)))
                    }
                    Err(task::ExecutionError::Operational(_)) => {
                        return Err(D::Operational("Matching execution unsettled".into()))
                    }
                };
                inner
                    .jobs
                    .write_matching_task(
                        &message,
                        value,
                        children,
                        false,
                        failure.clone(),
                        owner.clone(),
                    )
                    .await
                    .map_err(|_| D::Operational("Matching backend write unsettled".into()))?;
                Ok(if failure.is_some() {
                    D::TaskFailed(message.id)
                } else {
                    D::Completed(message.id)
                })
            }
            .await;
            if operation.is_err() {
                retain(&inner, Arc::new((owner, effects)));
            }
            operation
        })
        .await;
        match result {
            Ok(v) => v,
            Err(_) => {
                retain(&fallback, held);
                Err(D::Operational(
                    "Matching worker join unsettled; admission retained".into(),
                ))
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
        self.inner.closed.store(true, Ordering::Release);
    }
    fn shutdown(&self, timeout: Duration) -> BoxFuture<'_, Result<(), D>> {
        Box::pin(async move {
            self.stop_admission();
            tokio::time::timeout(timeout, async {
                loop {
                    let notified = self.inner.changed.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if self.inner.active.load(Ordering::Acquire) == 0 {
                        break;
                    }
                    notified.await;
                }
            })
            .await
            .map_err(|_| D::Operational("Matching physical drain incomplete".into()))
        })
    }
}
struct Effects {
    jobs: Jobs,
    identity: ProducerIdentitySource,
    parent: Uuid,
    children: Mutex<Vec<Uuid>>,
    failed: AtomicBool,
}
impl MatchingEffects for Effects {
    fn publish(&self, child: MatchingChild, owner: Scope) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            let outcome=async{
 let identity=self.identity.current()?;let id=Uuid::new_v4();
 let(arg,sd)=match child{MatchingChild::ParsePrograms(id)=>(id,false),MatchingChild::SdMapped(id)=>(id,true)};
 let raw=crate::protocol::encode(&crate::protocol::Task::CreateBackup,id,identity.reply_to,Uuid::new_v4(),&identity.origin).map_err(|_|StoreError::Unsupported("Matching child envelope"))?;
 let mut frame:Value=serde_json::from_slice(&raw).map_err(|_|StoreError::Unsupported("Matching child JSON"))?;
 frame["headers"]["task"]=json!(if sd{"apps.epg.tasks.fetch_sd_mapped_guide_batch"}else{"apps.epg.tasks.parse_programs_for_tvg_id"});frame["headers"]["timelimit"]=json!([3600,3500]);frame["headers"]["argsrepr"]=json!(format!("({arg},)"));frame["headers"]["root_id"]=json!(self.parent);frame["headers"]["parent_id"]=json!(self.parent);
 frame["body"]=json!(STANDARD.encode(serde_json::to_vec(&json!([[arg],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).map_err(|_|StoreError::Unsupported("Matching child body"))?));
 let raw=serde_json::to_vec(&frame).map_err(|_|StoreError::Unsupported("Matching child encoding"))?;
 let receipt=if sd{self.jobs.publish_sd_guide(&raw,owner).await}else{self.jobs.publish_auto_sync_epg(&raw,owner).await}.map_err(|_|StoreError::UnhandledRestFault)?;
 if receipt.task_id!=id{return Err(StoreError::UnhandledRestFault);}
 self.children.lock().map_err(|_|StoreError::UnhandledRestFault)?.push(id);Ok(())
 }.await;
            if outcome.is_err() {
                self.failed.store(true, Ordering::Release);
            }
            outcome
        })
    }
}
#[derive(Clone)]
pub struct Publisher {
    jobs: Jobs,
    identity: ProducerIdentitySource,
}
impl Publisher {
    pub fn new(jobs: Jobs, identity: ProducerIdentitySource) -> Self {
        Self { jobs, identity }
    }
}
impl dispatcharr_store::channel_epg_match_http::Publisher for Publisher {
    fn publish(&self, request: Request, owner: Scope) -> BoxFuture<'_, Result<Uuid, StoreError>> {
        Box::pin(async move {
            let id = Uuid::new_v4();
            let identity = self.identity.current()?;
            let selected = kind(&request);
            let args = match request {
                Request::All => json!([]),
                Request::Selected(v) | Request::Single(v) => json!([v]),
            };
            let kwargs = json!({});
            let (repr, kwrepr) = self.identity.channel_task_representations(&args, &kwargs)?;
            let raw = crate::protocol::encode(
                &crate::protocol::Task::CreateBackup,
                id,
                identity.reply_to,
                Uuid::new_v4(),
                &identity.origin,
            )
            .map_err(|_| StoreError::Unsupported("Matching envelope"))?;
            let mut frame: Value = serde_json::from_slice(&raw)
                .map_err(|_| StoreError::Unsupported("Matching JSON"))?;
            frame["headers"]["task"] = json!(selected.task());
            frame["headers"]["argsrepr"] = json!(repr);
            frame["headers"]["kwargsrepr"] = json!(kwrepr);
            frame["body"]=json!(STANDARD.encode(serde_json::to_vec(&json!([args,kwargs,{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).map_err(|_|StoreError::Unsupported("Matching body"))?));
            let raw = serde_json::to_vec(&frame)
                .map_err(|_| StoreError::Unsupported("Matching encoding"))?;
            self.jobs
                .publish_matching_task(id, raw, owner)
                .await
                .map_err(|_| StoreError::UnhandledRestFault)?;
            Ok(id)
        })
    }
}

#[cfg(test)]
mod wire_controls {
    use super::*;
    fn frame(task: Kind, args: Value) -> Value {
        let captured: Value =
            serde_json::from_str(include_str!("../datasets/source-default-stats-wire.json"))
                .unwrap();
        let mut frame = captured["defaults"][0]["envelope"].clone();
        frame["headers"]["task"] = json!(task.task());
        frame["body"] = json!(STANDARD.encode(
            serde_json::to_vec(
                &json!([args, {}, {"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
            )
            .unwrap()
        ));
        frame
    }
    #[test]
    fn raw_selected_values_survive_and_foreign_authority_never_becomes_a_claim() {
        for raw in [
            json!([9, 9, 404]),
            json!("12"),
            json!({"9":true}),
            json!(42),
            json!(true),
            Value::Null,
        ] {
            let frame = frame(Kind::Selected, json!([raw]));
            let parsed = parse(&serde_json::to_vec(&frame).unwrap()).unwrap();
            assert!(matches!(parsed.request,Request::Selected(value) if value==raw));
            for (field, value) in [
                ("parent_id", json!(Uuid::new_v4())),
                ("root_id", json!(Uuid::new_v4())),
                ("ignore_result", json!(true)),
                ("eta", json!("2026-11-16T00:00:00Z")),
                ("foreign_authority", json!(true)),
            ] {
                let mut bad = frame.clone();
                bad["headers"][field] = value;
                assert!(parse(&serde_json::to_vec(&bad).unwrap()).is_err());
            }
            let mut bad = frame;
            bad["body"]=json!(STANDARD.encode(serde_json::to_vec(&json!([[raw],{},{"callbacks":[{"task":"foreign.callback"}],"errbacks":null,"chain":null,"chord":null}])).unwrap()));
            assert!(parse(&serde_json::to_vec(&bad).unwrap()).is_err());
        }
        assert!(parse(&serde_json::to_vec(&frame(Kind::All, json!([1]))).unwrap()).is_err());
        assert!(parse(&serde_json::to_vec(&frame(Kind::Single, json!([]))).unwrap()).is_err());
    }
}
