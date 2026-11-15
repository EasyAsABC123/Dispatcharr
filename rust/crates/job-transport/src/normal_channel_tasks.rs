//! Four normal channel tasks; Original Dispatcharr/Celery authors retain credit.
use crate::{
    milestone_dispatch::{Disposition as D, Family, Handler, Lifecycle},
    normal_revocation::Revoked,
    transport::Jobs,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use dispatcharr_contracts::{BoxFuture, UpdateEventSink};
use dispatcharr_store::{
    channel_maintenance::{Kind, Request},
    ProducerIdentitySource, Store,
};
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
    pub request: Request,
}
fn list(value: &Value) -> Result<Vec<Value>, ()> {
    value.as_array().cloned().ok_or(())
}
pub(crate) fn parse(raw: &[u8]) -> Result<Message, ()> {
    if raw.is_empty() || raw.len() > crate::protocol::MAX_ENVELOPE {
        return Err(());
    }
    let mut frame = crate::clocked_recording_envelope::json_value(raw).map_err(|_| ())?;
    let kind = [Kind::Bulk, Kind::Names, Kind::Logos, Kind::TvgIds]
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
    let parts = body.as_array().filter(|a| a.len() == 3).ok_or(())?;
    let args = parts[0].as_array().ok_or(())?;
    let kw = parts[1].as_object().ok_or(())?;
    if parts[2] != json!({"callbacks":null,"errbacks":null,"chain":null,"chord":null}) {
        return Err(());
    }
    let mut request = Request {
        kind,
        ids: Vec::new(),
        source: None,
        profiles: None,
        starting: None,
    };
    match kind {
        Kind::Names | Kind::TvgIds => {
            if args.len() != 1 || !kw.is_empty() {
                return Err(());
            }
            request.ids = list(&args[0])?;
        }
        Kind::Bulk => {
            if args.len() != 3 || !kw.is_empty() {
                return Err(());
            }
            request.ids = list(&args[0])?;
            request.profiles = if args[1].is_null() {
                None
            } else {
                Some(list(&args[1])?)
            };
            request.starting = if args[2].is_null() {
                None
            } else {
                Some(args[2].clone())
            };
        }
        Kind::Logos => {
            if !args.is_empty() || kw.len() > 1 {
                return Err(());
            }
            if let Some(ids) = kw.get("channel_ids") {
                request.ids = list(ids)?;
            } else if !kw.is_empty() {
                request.source = kw
                    .get("epg_source_id")
                    .filter(|v| dispatcharr_store::channel_maintenance::source_truthy(v))
                    .cloned();
            }
        }
    }
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
    store: Store,
    events: Arc<dyn UpdateEventSink>,
    revoked: Revoked,
    closed: AtomicBool,
    slots: Arc<Semaphore>,
    uncertain: Mutex<Vec<Arc<dyn Send + Sync>>>,
}
#[derive(Clone)]
pub struct Prepared {
    inner: Arc<Inner>,
}
impl Prepared {
    pub fn new(
        jobs: Jobs,
        store: Store,
        events: Arc<dyn UpdateEventSink>,
        revoked: Revoked,
    ) -> Result<Self, D> {
        if !jobs.allows_defaults() {
            return Err(D::UnsupportedDomain);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                store,
                events,
                revoked,
                closed: AtomicBool::new(false),
                slots: Arc::new(Semaphore::new(4)),
                uncertain: Mutex::new(Vec::with_capacity(4)),
            }),
        })
    }
    pub fn entries(&self) -> Vec<(Family, Arc<dyn Handler>)> {
        [
            (Family::ChannelBulk, Kind::Bulk),
            (Family::ChannelNames, Kind::Names),
            (Family::ChannelLogos, Kind::Logos),
            (Family::ChannelTvgIds, Kind::TvgIds),
        ]
        .into_iter()
        .map(|(family, kind)| {
            (
                family,
                Arc::new(Worker {
                    inner: self.inner.clone(),
                    kind,
                }) as Arc<dyn Handler>,
            )
        })
        .collect()
    }
}
struct Owner {
    _slot: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
    _inner: Arc<Inner>,
}
struct Worker {
    inner: Arc<Inner>,
    kind: Kind,
}
impl Worker {
    async fn run(&self, expected: Option<Arc<[u8]>>, caller: Arc<dyn Send + Sync>) -> Result<D, D> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(D::Operational("Channel workers closed".into()));
        }
        let slot = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| D::Capacity)?;
        let owner = Arc::new(Owner {
            _slot: slot,
            _caller: caller,
            _inner: self.inner.clone(),
        });
        let inner = self.inner.clone();
        let kind = self.kind;
        let retained_owner: Arc<dyn Send + Sync> = owner.clone();
        let retained_inner = inner.clone();
        let joined = tokio::spawn(async move {
            let candidate = match inner
                .jobs
                .peek_channel_task(expected.as_deref(), owner.clone())
                .await
            {
                Ok(Some(v)) => v,
                Ok(None) => return Ok(D::Empty),
                Err(crate::transport::Error::ClaimRace) => return Ok(D::HeadChanged),
                Err(crate::transport::Error::UnsupportedTask) => return Err(D::UnsupportedHead),
                Err(_) => return Err(D::Operational("Channel peek failed".into())),
            };
            if candidate.message.request.kind != kind {
                return Err(D::UnsupportedHead);
            }
            dispatcharr_store::channel_maintenance::preflight(
                &inner.store,
                &candidate.message.request,
            )
            .await
            .map_err(|_| D::UnsupportedDomain)?;
            if inner.closed.load(Ordering::Acquire) {
                return Err(D::Operational(
                    "Channel admission closed before claim".into(),
                ));
            }
            let message = match inner
                .jobs
                .claim_channel_task(candidate, owner.clone())
                .await
            {
                Ok(v) => v,
                Err(crate::transport::Error::ClaimRace) => return Ok(D::HeadChanged),
                Err(_) => {
                    retain(&inner, owner);
                    return Err(D::Operational("Channel claim ambiguous/failed".into()));
                }
            };
            let operation = async {
                let revoked = inner
                    .revoked
                    .contains(&message.id.to_string())
                    .map_err(|_| D::Operational("Channel revoked lookup failed".into()))?;
                if revoked {
                    inner
                        .jobs
                        .write_channel_task(&message, Value::Null, Vec::new(), true, owner.clone())
                        .await
                        .map_err(|_| D::Operational("Channel revoked backend failed".into()))?;
                    return Ok(D::Revoked(message.id));
                }
                match dispatcharr_store::channel_maintenance::execute(
                    inner.store.clone(),
                    inner.events.clone(),
                    message.request.clone(),
                    message.id,
                    owner.clone(),
                )
                .await
                {
                    Ok(value) => {
                        inner
                            .jobs
                            .write_channel_task(&message, value, Vec::new(), false, owner.clone())
                            .await
                            .map_err(|_| D::Operational("Channel backend write failed".into()))?;
                        Ok(D::Completed(message.id))
                    }
                    Err(error) => {
                        let (failure, total) = Failure::from_execution(error).map_err(|_| {
                            D::Operational(
                                "Channel failure outside measured settled profile".into(),
                            )
                        })?;
                        dispatcharr_store::channel_maintenance::publish_failed(
                            inner.events.clone(),
                            message.request.kind,
                            message.id,
                            total,
                            &failure.message,
                            owner.clone(),
                        )
                        .await;
                        inner
                            .jobs
                            .write_channel_failure(&message, &failure, owner.clone())
                            .await
                            .map_err(|_| {
                                D::Operational("Channel FAILURE backend write failed".into())
                            })?;
                        Ok(D::TaskFailed(message.id))
                    }
                }
            }
            .await;
            if operation.is_err() {
                retain(&inner, owner);
            }
            operation
        })
        .await;
        match joined {
            Ok(v) => v,
            Err(_) => {
                retain(&retained_inner, retained_owner);
                Err(D::Operational(
                    "Channel worker join failed; physical admission retained".into(),
                ))
            }
        }
    }
}
fn retain(inner: &Inner, owner: Arc<dyn Send + Sync>) {
    let mut retained = inner.uncertain.lock().unwrap_or_else(|v| v.into_inner());
    if !retained.iter().any(|v| Arc::ptr_eq(v, &owner)) {
        retained.push(owner);
    }
}
impl Handler for Worker {
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
impl Lifecycle for Worker {
    fn stop_admission(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }
    fn shutdown(&self, timeout: Duration) -> BoxFuture<'_, Result<(), D>> {
        Box::pin(async move {
            self.stop_admission();
            let held =
                tokio::time::timeout(timeout, self.inner.slots.clone().acquire_many_owned(4))
                    .await
                    .map_err(|_| D::Operational("Channel physical drain incomplete".into()))?
                    .map_err(|_| D::Operational("Channel permits closed".into()))?;
            drop(held);
            Ok(())
        })
    }
}
/// Normal API producer shares the exact Main Jobs/identity ports. The emitted
/// frame is parsed by this SAME family parser before any physical publication.
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
impl dispatcharr_store::channel_maintenance_http::Publisher for Publisher {
    fn publish(
        &self,
        request: Request,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Uuid, dispatcharr_store::StoreError>> {
        Box::pin(async move {
            let id = Uuid::new_v4();
            let identity = self.identity.current()?;
            let (args, kwargs) = match request.kind {
                Kind::Bulk => (
                    json!([request.ids, request.profiles, request.starting]),
                    json!({}),
                ),
                Kind::Names | Kind::TvgIds => (json!([request.ids]), json!({})),
                Kind::Logos => {
                    if let Some(source) = request.source {
                        (json!([]), json!({"epg_source_id":source}))
                    } else {
                        (json!([]), json!({"channel_ids":request.ids}))
                    }
                }
            };
            let (repr, kwrepr) = self.identity.channel_task_representations(&args, &kwargs)?;
            let raw = crate::protocol::encode(
                &crate::protocol::Task::CreateBackup,
                id,
                identity.reply_to,
                Uuid::new_v4(),
                &identity.origin,
            )
            .map_err(|_| dispatcharr_store::StoreError::Unsupported("Channel envelope"))?;
            let mut frame: Value = serde_json::from_slice(&raw)
                .map_err(|_| dispatcharr_store::StoreError::Unsupported("Channel JSON"))?;
            frame["headers"]["task"] = json!(request.kind.task());
            frame["headers"]["argsrepr"] = json!(repr);
            frame["headers"]["kwargsrepr"] = json!(kwrepr);
            frame["body"]=json!(STANDARD.encode(serde_json::to_vec(&json!([args,kwargs,{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).map_err(|_|dispatcharr_store::StoreError::Unsupported("Channel body"))?));
            let bytes = serde_json::to_vec(&frame)
                .map_err(|_| dispatcharr_store::StoreError::Unsupported("Channel serialization"))?;
            parse(&bytes).map_err(|_| {
                dispatcharr_store::StoreError::Unsupported(
                    "Channel task capacity or argument profile",
                )
            })?;
            self.jobs
                .publish_channel_task(id, bytes, owner)
                .await
                .map_err(|_| dispatcharr_store::StoreError::UnhandledRestFault)?;
            Ok(id)
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn frame(task: Kind, args: Value, kwargs: Value) -> Value {
        // Retained actual Source common transport envelope, not synthetic parent
        // authority. Only new function/arguments are proposed, still unmeasured.
        let source: Value =
            serde_json::from_str(include_str!("../datasets/source-default-stats-wire.json"))
                .unwrap();
        let mut frame = source["defaults"][0]["envelope"].clone();
        frame["headers"]["task"] = json!(task.task());
        frame["body"] = json!(STANDARD.encode(
            serde_json::to_vec(
                &json!([args,kwargs,{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
            )
            .unwrap()
        ));
        frame
    }
    #[test]
    fn concrete_channel_bodies_preserve_duplicates_and_reject_foreign_authority_before_claim() {
        let cases = [
            (Kind::Bulk, json!([[9, 9, 404], [], 0]), json!({})),
            (Kind::Names, json!([[9, 9, 404]]), json!({})),
            (Kind::TvgIds, json!([[9]]), json!({})),
            (Kind::Logos, json!([]), json!({"epg_source_id":7})),
            (Kind::Logos, json!([]), json!({"channel_ids":[9,9]})),
        ];
        for (kind, args, kwargs) in cases {
            let frame = frame(kind, args, kwargs);
            let raw = serde_json::to_vec(&frame).unwrap();
            let parsed = parse(&raw).unwrap();
            assert_eq!(parsed.request.kind, kind);
            if kind == Kind::Bulk {
                assert_eq!(parsed.request.ids, vec![json!(9), json!(9), json!(404)]);
                assert_eq!(parsed.request.profiles, Some(Vec::<Value>::new()));
                assert_eq!(parsed.request.starting, Some(json!(0)));
            }
            for (field, value) in [
                ("parent_id", json!(Uuid::new_v4())),
                ("ignore_result", json!(true)),
                ("eta", json!("2026-11-16T00:00:00Z")),
                ("foreign_authority", json!(true)),
            ] {
                let mut bad = frame.clone();
                bad["headers"][field] = value;
                assert!(parse(&serde_json::to_vec(&bad).unwrap()).is_err());
            }
            let mut bad = frame;
            let bytes = STANDARD.decode(bad["body"].as_str().unwrap()).unwrap();
            let mut body: Value = serde_json::from_slice(&bytes).unwrap();
            body[2]["callbacks"] = json!([{"task":"foreign.callback"}]);
            bad["body"] = json!(STANDARD.encode(serde_json::to_vec(&body).unwrap()));
            assert!(parse(&serde_json::to_vec(&bad).unwrap()).is_err());
        }
        let both = frame(
            Kind::Logos,
            json!([]),
            json!({"channel_ids":[1],"epg_source_id":7}),
        );
        assert!(parse(&serde_json::to_vec(&both).unwrap()).is_err());
        let too_many = frame(Kind::Names, json!([vec![1; 32769]]), json!({}));
        assert!(parse(&serde_json::to_vec(&too_many).unwrap()).is_err());
    }
}

/// Classification is limited to the actual new Source observations. The
/// traceback field is an explicit Native diagnostic, never invented Python.
pub(crate) struct Failure {
    class: &'static str,
    module: &'static str,
    message: String,
    diagnostic: String,
}
impl Failure {
    fn from_execution(
        error: dispatcharr_store::channel_maintenance::ExecutionError,
    ) -> Result<(Self, usize), ()> {
        use dispatcharr_store::channel_maintenance::ExecutionError;
        match error {
            ExecutionError::ValueError{message,total}=>Ok((Self{class:"ValueError",module:"builtins",message,diagnostic:"Native Dispatcharr channel task diagnostic: settled Django-compatible argument preparation failure; Python traceback is not reproduced.".into()},total)),
            ExecutionError::TypeError{message,total}=>Ok((Self{class:"TypeError",module:"builtins",message,diagnostic:"Native Dispatcharr channel task diagnostic: settled Django-compatible integer preparation TypeError; Python traceback is not reproduced.".into()},total)),
            ExecutionError::Raised{error:dispatcharr_store::StoreError::Database(error),total}=>{
                let pg=error.as_database_error().and_then(|v|v.try_downcast_ref::<sqlx::postgres::PgDatabaseError>()).ok_or(())?;
                if pg.severity()!=sqlx::postgres::PgSeverity::Error||pg.position().is_some(){return Err(());}
                Ok((Self::from_pg_fields(pg.code(),pg.message(),pg.detail(),pg.hint(),pg.r#where())?,total))
            }
            _=>Err(())
        }
    }
    fn from_pg_fields(
        code: &str,
        primary: &str,
        detail: Option<&str>,
        hint: Option<&str>,
        context: Option<&str>,
    ) -> Result<Self, ()> {
        let class = match code {
            "P0001" => "ProgrammingError",
            "22001" => "DataError",
            "23505" | "23514" => "IntegrityError",
            _ => return Err(()),
        };
        let fields = [Some(primary), detail, hint, context];
        if fields.into_iter().flatten().any(|v| v.len() > 8192) {
            return Err(());
        }
        let mut message = primary.to_owned();
        for (label, value) in [("DETAIL", detail), ("HINT", hint), ("CONTEXT", context)] {
            if let Some(value) = value {
                message.push('\n');
                message.push_str(label);
                message.push_str(":  ");
                message.push_str(value);
            }
        }
        let message=message.trim_matches(|ch:char|matches!(ch,'\u{0009}'..='\u{000d}'|'\u{001c}'..='\u{0020}'|'\u{0085}'|'\u{00a0}'|'\u{1680}'|'\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}')).to_owned();
        if message.len() > 8192 {
            return Err(());
        }
        Ok(Self{class,module:"django.db.utils",message,diagnostic:format!("Native Dispatcharr channel task diagnostic: settled PostgreSQL ERROR SQLSTATE {}; actual connection Ready and close completed; original inner rollback/earlier effects retained. Python traceback is not reproduced.",code)})
    }
    pub(crate) fn metadata(&self, id: Uuid) -> Result<Vec<u8>, crate::transport::Error> {
        let bytes=serde_json::to_vec(&json!({"status":"FAILURE","result":{"exc_type":self.class,"exc_module":self.module,"exc_message":[self.message]},"traceback":self.diagnostic,"children":[],"date_done":chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string(),"task_id":id.to_string()})).map_err(|_|crate::transport::Error::Protocol)?;
        crate::decode::decode(&bytes, id).map_err(|_| crate::transport::Error::Protocol)?;
        Ok(bytes)
    }
}
#[cfg(test)]
mod observed_failures {
    use super::*;
    #[test]
    fn actual_source_exception_class_message_and_backend_shape_without_python_frames() {
        let source: Value = serde_json::from_str(include_str!(
            "../datasets/source-channel-maintenance-failures.json"
        ))
        .unwrap();
        let mut count = 0;
        for case in source["cases"].as_array().unwrap() {
            if case["metadata"]["status"] != "FAILURE" {
                continue;
            }
            let observed = &case["metadata"]["result"];
            let failure = if case["controlled_owned_PG_error"].is_object() {
                // Primary/code are the actual owned trigger declaration; context
                // is parsed from the retained Source exception string only.
                let message = observed["exc_message"][0].as_str().unwrap();
                let (primary, context) = message.split_once("\nCONTEXT:  ").unwrap();
                assert_eq!(primary, case["controlled_owned_PG_error"]["message"]);
                Failure::from_pg_fields(
                    case["controlled_owned_PG_error"]["sqlstate"]
                        .as_str()
                        .unwrap(),
                    primary,
                    None,
                    None,
                    Some(context),
                )
                .unwrap()
            } else {
                Failure::from_execution(
                    dispatcharr_store::channel_maintenance::ExecutionError::ValueError {
                        message: "channel_ids or epg_source_id is required".into(),
                        total: 0,
                    },
                )
                .unwrap()
                .0
            };
            let id = Uuid::parse_str(case["actual_task_id"].as_str().unwrap()).unwrap();
            let bytes = failure.metadata(id).unwrap();
            let metadata: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(metadata["result"], *observed);
            assert_eq!(metadata["status"], "FAILURE");
            assert_eq!(metadata["children"], json!([]));
            assert_eq!(metadata["task_id"], case["actual_task_id"]);
            assert!(!metadata["traceback"]
                .as_str()
                .unwrap()
                .contains("/app/apps/channels/tasks.py"));
            assert_ne!(metadata["traceback"], case["metadata"]["traceback"]);
            count += 1;
        }
        assert_eq!(count, 5);
        assert!(Failure::from_pg_fields("08006", "connection lost", None, None, None).is_err());
        assert!(Failure::from_pg_fields("P0001", &"x".repeat(8193), None, None, None).is_err());
        let unsupported = Failure::from_execution(
            dispatcharr_store::channel_maintenance::ExecutionError::Operational,
        );
        assert!(unsupported.is_err());
    }
}
#[cfg(test)]
mod source_http_gap_frames {
    use super::*;
    #[test]
    fn all_actual_source_http_frames_remain_consumable_without_early_type_coercion() {
        let source: Value = serde_json::from_str(include_str!(
            "../datasets/source-channel-http-producers.json"
        ))
        .unwrap();
        let frames = source["frames"].as_array().unwrap();
        assert_eq!(frames.len(), 29);
        let mut special = 0;
        for row in frames {
            let raw = STANDARD
                .decode(row["raw_base64"].as_str().unwrap())
                .unwrap();
            let message = parse(&raw).unwrap();
            let frame = &row["frame"];
            assert_eq!(message.id.to_string(), frame["headers"]["id"]);
            assert_eq!(message.request.kind.task(), frame["headers"]["task"]);
            let body: Value =
                serde_json::from_slice(&STANDARD.decode(frame["body"].as_str().unwrap()).unwrap())
                    .unwrap();
            match message.request.kind {
                Kind::Names | Kind::TvgIds => {
                    assert_eq!(message.request.ids, body[0][0].as_array().unwrap().clone())
                }
                Kind::Bulk => {
                    assert_eq!(message.request.ids, body[0][0].as_array().unwrap().clone());
                    assert_eq!(message.request.profiles, body[0][1].as_array().cloned());
                    assert_eq!(
                        message.request.starting,
                        if body[0][2].is_null() {
                            None
                        } else {
                            Some(body[0][2].clone())
                        }
                    );
                }
                Kind::Logos => {
                    if body[1]["epg_source_id"].is_null() {
                        assert_eq!(
                            message.request.ids,
                            body[1]["channel_ids"].as_array().unwrap().clone()
                        );
                    } else {
                        assert_eq!(
                            message.request.source,
                            Some(body[1]["epg_source_id"].clone())
                        );
                    }
                }
            }
            let name = row["case"].as_str().unwrap();
            if name.contains("noninteger")
                || name.contains("fractional")
                || name.contains("4097")
                || name.contains("invalid-source")
            {
                special += 1;
            }
        }
        assert_eq!(special, 8);
    }
}
