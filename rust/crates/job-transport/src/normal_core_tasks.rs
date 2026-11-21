//! Experimental actual Core task handlers. No application/poller starts here.
//! Original Dispatcharr/Celery/Django authors retain all algorithm/protocol credit.
use crate::{
    milestone_dispatch::{Disposition as D, Family, Handler, Lifecycle},
    normal_revocation::Revoked,
    transport::Jobs,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use dispatcharr_contracts::{BoxFuture, UpdateEventSink};
use dispatcharr_store::{
    core_task_rehash::Request,
    recording_http::{execute_recurring_purge, preflight_recurring_purge, RecordingJobs},
    AuthService, Store,
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
pub(crate) enum Kind {
    Rehash(Request),
    Offsets,
    RecurringPurge(i64),
}
impl Kind {
    fn family(&self) -> Family {
        match self {
            Self::Rehash(_) => Family::RehashStreams,
            Self::Offsets => Family::RescheduleOffsets,
            Self::RecurringPurge(_) => Family::RecurringPurge,
        }
    }
}
#[derive(Clone, Debug)]
pub(crate) struct Message {
    pub id: Uuid,
    pub kind: Kind,
}
pub(crate) fn parse(raw: &[u8]) -> Result<Message, ()> {
    if raw.is_empty() || raw.len() > crate::protocol::MAX_ENVELOPE {
        return Err(());
    }
    let mut frame = crate::clocked_recording_envelope::json_value(raw).map_err(|_| ())?;
    let kind = match frame["headers"]["task"].as_str() {
        Some("apps.channels.tasks.reschedule_upcoming_recordings_for_offset_change") => {
            Kind::Offsets
        }
        Some("apps.channels.tasks.purge_recurring_rule") => {
            let bytes = STANDARD
                .decode(frame["body"].as_str().ok_or(())?)
                .map_err(|_| ())?;
            let body = crate::clocked_recording_envelope::json_value(&bytes).map_err(|_| ())?;
            let args = body[0]
                .as_array()
                .filter(|args| args.len() == 1)
                .ok_or(())?;
            let rule = args[0].as_i64().filter(|id| *id > 0).ok_or(())?;
            let scheduled_name = frame["headers"]["periodic_task_name"].as_str().ok_or(())?;
            let expected_repr = format!("({rule},)");
            let expected_list_repr = format!("[{rule}]");
            if body
                != json!([[rule], {}, {"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
                || scheduled_name.is_empty()
                || !matches!(frame["headers"]["argsrepr"].as_str(), Some(repr) if repr == expected_repr || repr == expected_list_repr)
            {
                return Err(());
            }
            frame["body"] = json!(STANDARD.encode(
                serde_json::to_vec(
                    &json!([[],{},{ "callbacks":null,"errbacks":null,"chain":null,"chord":null }])
                )
                .map_err(|_| ())?
            ));
            frame["headers"]["argsrepr"] = json!("()");
            Kind::RecurringPurge(rule)
        }
        Some("core.tasks.rehash_streams") => {
            let bytes = STANDARD
                .decode(frame["body"].as_str().ok_or(())?)
                .map_err(|_| ())?;
            if bytes.len() > 32768 {
                return Err(());
            }
            let body = crate::clocked_recording_envelope::json_value(&bytes).map_err(|_| ())?;
            if body.as_array().is_none_or(|v| v.len() != 3) {
                return Err(());
            }
            let args = body[0].as_array().filter(|v| v.len() == 1).ok_or(())?;
            let keys = args[0]
                .as_array()
                .filter(|v| v.len() <= 64)
                .ok_or(())?
                .iter()
                .map(|v| {
                    v.as_str()
                        .filter(|v| v.len() <= 64)
                        .map(str::to_owned)
                        .ok_or(())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let kwargs = body[1].as_object().ok_or(())?;
            if kwargs.keys().any(|k| {
                !matches!(
                    k.as_str(),
                    "account_id"
                        | "locks_preacquired"
                        | "preacquired_lock_ids"
                        | "preacquired_lock_token"
                )
            }) {
                return Err(());
            }
            let account = match kwargs.get("account_id") {
                None | Some(Value::Null) => None,
                Some(v) => Some(v.as_i64().filter(|id| *id > 0).ok_or(())?),
            };
            let preacquired = match kwargs.get("locks_preacquired") {
                None => false,
                Some(v) => v.as_bool().ok_or(())?,
            };
            let ids = match kwargs.get("preacquired_lock_ids") {
                None | Some(Value::Null) => Vec::new(),
                Some(v) => v
                    .as_array()
                    .ok_or(())?
                    .iter()
                    .map(|v| v.as_i64().filter(|v| *v > 0).ok_or(()))
                    .collect::<Result<Vec<_>, _>>()?,
            };
            let token = match kwargs.get("preacquired_lock_token") {
                None | Some(Value::Null) => None,
                Some(v) => Some(v.as_str().filter(|v| v.len() <= 255).ok_or(())?.to_owned()),
            };
            if body.get(2)
                != Some(&json!({"callbacks":null,"errbacks":null,"chain":null,"chord":null}))
            {
                return Err(());
            }
            frame["body"] = json!(STANDARD.encode(
                serde_json::to_vec(
                    &json!([[],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
                )
                .map_err(|_| ())?
            ));
            frame["headers"]["argsrepr"] = json!("()");
            frame["headers"]["kwargsrepr"] = json!("{}");
            Kind::Rehash(Request {
                keys,
                account,
                preacquired,
                ids,
                token,
            })
        }
        _ => return Err(()),
    };
    frame["headers"]["task"] = json!("apps.proxy.tasks.fetch_channel_stats");
    let common = crate::stats_protocol::parse(&serde_json::to_vec(&frame).map_err(|_| ())?)
        .map_err(|_| ())?;
    Ok(Message {
        id: common.task_id,
        kind,
    })
}
struct Inner {
    jobs: Jobs,
    store: Store,
    auth: AuthService,
    recordings: Arc<dyn RecordingJobs>,
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
        auth: AuthService,
        recordings: Arc<dyn RecordingJobs>,
        events: Arc<dyn UpdateEventSink>,
        revoked: Revoked,
    ) -> Result<Self, D> {
        if !auth.uses_store_pool(&store)
            || !recordings.supports_task_signals()
            || !recordings.supports_task_session()
            || !dispatcharr_store::core_task_rehash::available(&auth)
        {
            return Err(D::UnsupportedDomain);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                store,
                auth,
                recordings,
                events,
                revoked,
                closed: AtomicBool::new(false),
                slots: Arc::new(Semaphore::new(2)),
                uncertain: Mutex::new(Vec::with_capacity(2)),
            }),
        })
    }
    pub fn entries(&self) -> Vec<(Family, Arc<dyn Handler>)> {
        [
            Family::RehashStreams,
            Family::RescheduleOffsets,
            Family::RecurringPurge,
        ]
        .into_iter()
        .map(|family| {
            let worker = Arc::new(Worker {
                inner: self.inner.clone(),
                family,
            }) as Arc<dyn Handler>;
            (family, worker)
        })
        .collect()
    }
}
struct WorkOwner {
    _slot: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
    _inner: Arc<Inner>,
}
struct Worker {
    inner: Arc<Inner>,
    family: Family,
}
impl Worker {
    async fn run(&self, expected: Option<Arc<[u8]>>, caller: Arc<dyn Send + Sync>) -> Result<D, D> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(D::Operational("Core workers closed".into()));
        }
        let slot = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| D::Capacity)?;
        let owner = Arc::new(WorkOwner {
            _slot: slot,
            _caller: caller,
            _inner: self.inner.clone(),
        });
        let inner = self.inner.clone();
        let family = self.family;
        let join_owner: Arc<dyn Send + Sync> = owner.clone();
        let join_inner = inner.clone();
        let joined = tokio::spawn(async move {
            let candidate = match inner
                .jobs
                .peek_core_task(expected.as_deref(), owner.clone())
                .await
            {
                Ok(Some(v)) => v,
                Ok(None) => return Ok(D::Empty),
                Err(crate::transport::Error::ClaimRace) => return Ok(D::HeadChanged),
                Err(crate::transport::Error::UnsupportedTask) => return Err(D::UnsupportedHead),
                Err(e) => return Err(D::Operational(format!("Core peek {e:?}"))),
            };
            if candidate.message.kind.family() != family {
                return Err(D::UnsupportedHead);
            }
            match &candidate.message.kind {
                Kind::Rehash(_) => {
                    dispatcharr_store::core_task_rehash::preflight(&inner.auth).await
                }
                Kind::RecurringPurge(rule) => preflight_recurring_purge(&inner.store, *rule).await,
                Kind::Offsets => {
                    inner
                        .recordings
                        .admit_scheduling(owner.clone())
                        .await
                        .map_err(|error| match error {
                            dispatcharr_store::recording_http::RecordingEffectError::Unsupported => D::UnsupportedDomain,
                            _ => D::Operational("Recording domain admission is unsettled".into()),
                        })?;
                    dispatcharr_store::core_task_offsets::preflight(&inner.store).await
                }
            }
            .map_err(|_| D::UnsupportedDomain)?;
            if inner.closed.load(Ordering::Acquire) {
                return Err(D::Operational("Core intake stopped before claim".into()));
            }
            let message = match inner.jobs.claim_core_task(candidate, owner.clone()).await {
                Ok(v) => v,
                Err(crate::transport::Error::ClaimRace) => return Ok(D::HeadChanged),
                Err(e) => {
                    retain(&inner, owner);
                    return Err(D::Operational(format!("Core claim {e:?}")));
                }
            };
            let operation = async {
                let revoked = inner
                    .revoked
                    .contains(&message.id.to_string())
                    .map_err(|_| D::Operational("Core revoke membership".into()))?;
                let (value, children) = if revoked {
                    (Value::Null, Vec::new())
                } else {
                    match &message.kind {
                        Kind::Rehash(request) => (
                            dispatcharr_store::core_task_rehash::execute(
                                inner.auth.clone(),
                                inner.events.clone(),
                                request.clone(),
                                owner.clone(),
                            )
                            .await
                            .map_err(|e| D::Operational(format!("Rehash effects {e:?}")))?,
                            Vec::new(),
                        ),
                        Kind::RecurringPurge(rule) => (
                            json!(execute_recurring_purge(
                                &inner.store,
                                &inner.auth,
                                inner.events.clone(),
                                *rule,
                                owner.clone(),
                            )
                            .await
                            .map_err(|error| D::Operational(format!("Recurring purge effects {error:?}")))?),
                            Vec::new(),
                        ),
                        Kind::Offsets => {
                            let report = dispatcharr_store::core_task_offsets::execute(
                                inner.store.clone(),
                                inner.recordings.clone(),
                                inner.events.clone(),
                                message.id,
                                owner.clone(),
                            )
                            .await
                            .map_err(|e| D::Operational(format!("Offset effects {e:?}")))?;
                            (report.value, report.children)
                        }
                    }
                };
                inner
                    .jobs
                    .write_core_task(&message, value, children, revoked, owner.clone())
                    .await
                    .map_err(|e| D::Operational(format!("Core backend {e:?}")))?;
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
        .await;
        match joined {
            Ok(outcome) => outcome,
            Err(_) => {
                retain(&join_inner, join_owner);
                Err(D::Operational(
                    "Core worker join failed; owner retained".into(),
                ))
            }
        }
    }
}
fn retain(inner: &Inner, owner: Arc<dyn Send + Sync>) {
    let mut held = inner
        .uncertain
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if !held.iter().any(|prior| Arc::ptr_eq(prior, &owner)) {
        held.push(owner);
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
            let owned =
                tokio::time::timeout(timeout, self.inner.slots.clone().acquire_many_owned(2))
                    .await
                    .map_err(|_| D::Operational("Core physical drain incomplete".into()))?
                    .map_err(|_| D::Operational("Core permits closed".into()))?;
            drop(owned);
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn configured_recurring_purge_requires_one_saved_numeric_operand_and_no_children() {
        let id = Uuid::new_v4();
        let raw = crate::protocol::encode(
            &crate::protocol::Task::CreateBackup,
            id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "rust@recurring-purge-fixture",
        )
        .unwrap();
        let mut frame: Value = serde_json::from_slice(&raw).unwrap();
        frame["headers"]["task"] = json!("apps.channels.tasks.purge_recurring_rule");
        frame["headers"]["argsrepr"] = json!("[27]");
        crate::periodic_headers::put(&mut frame, "owned-recurring-purge").unwrap();
        let mut body =
            json!([[27], {}, {"callbacks":null,"errbacks":null,"chain":null,"chord":null}]);
        frame["body"] = json!(STANDARD.encode(serde_json::to_vec(&body).unwrap()));
        let parsed = parse(&serde_json::to_vec(&frame).unwrap()).unwrap();
        assert_eq!(parsed.id, id);
        assert!(matches!(parsed.kind, Kind::RecurringPurge(27)));
        assert_eq!(
            crate::milestone_dispatch::classify(&serde_json::to_vec(&frame).unwrap()),
            Some(Family::RecurringPurge)
        );

        let mut unscheduled = frame.clone();
        unscheduled["headers"]
            .as_object_mut()
            .unwrap()
            .remove("periodic_task_name");
        assert!(parse(&serde_json::to_vec(&unscheduled).unwrap()).is_err());
        body[0][0] = json!("27");
        frame["body"] = json!(STANDARD.encode(serde_json::to_vec(&body).unwrap()));
        assert!(parse(&serde_json::to_vec(&frame).unwrap()).is_err());
        body[0][0] = json!(27);
        body[2]["callbacks"] = json!([]);
        frame["body"] = json!(STANDARD.encode(serde_json::to_vec(&body).unwrap()));
        assert!(parse(&serde_json::to_vec(&frame).unwrap()).is_err());
    }

    #[test]
    fn actual_retained_core_producer_frames_parse_without_execution_or_other_family_authority() {
        let source: Value =
            serde_json::from_str(include_str!("../datasets/source-core-produced-tasks.json"))
                .unwrap();
        let rows = source["frames"].as_array().unwrap();
        assert!(rows.len() >= 2);
        for row in rows {
            let raw = STANDARD
                .decode(row["raw_base64"].as_str().unwrap())
                .unwrap();
            let observed = parse(&raw).unwrap();
            let mut frame: Value = serde_json::from_slice(&raw).unwrap();
            assert_eq!(observed.id.to_string(), frame["headers"]["id"]);
            frame["headers"]["task"] = json!("unregistered.task");
            assert!(parse(&serde_json::to_vec(&frame).unwrap()).is_err());
            let mut frame: Value = serde_json::from_slice(&raw).unwrap();
            let mut body: Value =
                serde_json::from_slice(&STANDARD.decode(frame["body"].as_str().unwrap()).unwrap())
                    .unwrap();
            body[2]["callbacks"] = json!([{"task":"unregistered.callback"}]);
            frame["body"] = json!(STANDARD.encode(serde_json::to_vec(&body).unwrap()));
            assert!(parse(&serde_json::to_vec(&frame).unwrap()).is_err());
        }
    }
}
