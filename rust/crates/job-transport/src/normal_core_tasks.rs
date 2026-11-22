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
    recording_http::{
        execute_recurring_purge, preflight_recurring_purge, RecordingHttp, RecordingJobs,
        CONFIGURED_REBUILD_HORIZON_DAYS,
    },
    AuthService, ScheduledVod, ScheduledVodError, Store,
};
use serde_json::{json, Value};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
pub(crate) const SCHEDULED_MOVIE_ADVANCED: &str = "apps.vod.tasks.refresh_movie_advanced_data";
pub(crate) const SCHEDULED_SERIES_EPISODES: &str = "apps.vod.tasks.batch_refresh_series_episodes";
#[derive(Clone, Debug)]
pub(crate) enum Kind {
    Rehash(Request),
    Offsets,
    RecurringPurge(i64),
    RecurringRebuild { rule: i64, schedule: String },
    ScheduledSeriesEpisodes(i64),
    ScheduledMovieAdvanced(i64),
}
impl Kind {
    fn family(&self) -> Family {
        match self {
            Self::Rehash(_) => Family::RehashStreams,
            Self::Offsets => Family::RescheduleOffsets,
            Self::RecurringPurge(_) => Family::RecurringPurge,
            Self::RecurringRebuild { .. } => Family::RecurringRebuild,
            Self::ScheduledSeriesEpisodes(_) => Family::ScheduledSeriesEpisodes,
            Self::ScheduledMovieAdvanced(_) => Family::ScheduledMovieAdvanced,
        }
    }
}
#[derive(Clone, Debug)]
pub(crate) struct Message {
    pub id: Uuid,
    pub kind: Kind,
}
fn saved_numeric_operand(frame: &mut Value) -> Result<i64, ()> {
    let bytes = STANDARD
        .decode(frame["body"].as_str().ok_or(())?)
        .map_err(|_| ())?;
    let body = crate::clocked_recording_envelope::json_value(&bytes).map_err(|_| ())?;
    let args = body[0]
        .as_array()
        .filter(|args| args.len() == 1)
        .ok_or(())?;
    let operand = args[0].as_i64().filter(|id| *id > 0).ok_or(())?;
    let scheduled_name = frame["headers"]["periodic_task_name"].as_str().ok_or(())?;
    let expected_repr = format!("({operand},)");
    let expected_list_repr = format!("[{operand}]");
    if body != json!([[operand], {}, {"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
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
    Ok(operand)
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
            Kind::RecurringPurge(saved_numeric_operand(&mut frame)?)
        }
        Some(SCHEDULED_SERIES_EPISODES) => {
            Kind::ScheduledSeriesEpisodes(saved_numeric_operand(&mut frame)?)
        }
        Some(SCHEDULED_MOVIE_ADVANCED) => {
            Kind::ScheduledMovieAdvanced(saved_numeric_operand(&mut frame)?)
        }
        Some("apps.channels.tasks.rebuild_recurring_rule") => {
            let bytes = STANDARD
                .decode(frame["body"].as_str().ok_or(())?)
                .map_err(|_| ())?;
            let body = crate::clocked_recording_envelope::json_value(&bytes).map_err(|_| ())?;
            let args = body[0]
                .as_array()
                .filter(|args| args.len() == 1)
                .ok_or(())?;
            let rule = args[0].as_i64().filter(|id| *id > 0).ok_or(())?;
            let horizon = CONFIGURED_REBUILD_HORIZON_DAYS;
            let saved_name = frame["headers"]["periodic_task_name"].as_str().ok_or(())?;
            let expected_args = format!("[{rule}]");
            let expected_tuple = format!("({rule},)");
            let expected_kwargs = format!("{{'horizon_days': {horizon}}}");
            if body
                != json!([[rule], {"horizon_days":horizon}, {"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
                || saved_name.is_empty()
                || saved_name.len() > 255
                || !matches!(frame["headers"]["argsrepr"].as_str(), Some(repr) if repr == expected_args || repr == expected_tuple)
                || frame["headers"]["kwargsrepr"].as_str() != Some(expected_kwargs.as_str())
            {
                return Err(());
            }
            let schedule = saved_name.to_owned();
            frame["body"] =
                json!(STANDARD.encode(serde_json::to_vec(
                &json!([[], {}, {"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
            ).map_err(|_| ())?));
            frame["headers"]["argsrepr"] = json!("()");
            frame["headers"]["kwargsrepr"] = json!("{}");
            Kind::RecurringRebuild { rule, schedule }
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
struct SelectedResources {
    store: Store,
    recordings: Arc<dyn RecordingJobs>,
    recording_http: RecordingHttp,
    scheduled_vod: ScheduledVod,
}
struct Inner {
    jobs: Jobs,
    auth: AuthService,
    selected: Option<SelectedResources>,
    events: Arc<dyn UpdateEventSink>,
    revoked: Revoked,
    closed: AtomicBool,
    slots: Arc<Semaphore>,
    uncertain: Mutex<Vec<Arc<dyn Send + Sync>>>,
}
impl Inner {
    fn selected(&self) -> Result<&SelectedResources, D> {
        self.selected.as_ref().ok_or(D::UnsupportedDomain)
    }

    fn closed_before_claim(&self, selected_reason: &'static str) -> Result<D, D> {
        if self.selected.is_none() {
            Ok(D::NotDue)
        } else {
            Err(D::Operational(selected_reason.into()))
        }
    }
}

pub(super) fn owned_provider_rehash(kind: &Kind) -> bool {
    matches!(kind, Kind::Rehash(Request {account: Some(account), preacquired: true, ids, token: Some(token), ..})
        if ids.as_slice() == [*account]
            && token.len() == 32
            && token.bytes().all(|value| value.is_ascii_digit() || (b'a'..=b'f').contains(&value)))
}

/// Prepare only the qualified provider rehash consumer on already-owned Jobs.
/// The future registry owns the shared queue and transport retirement.
pub fn prepare_provider_rehash(
    jobs: Jobs,
    store: &Store,
    auth: AuthService,
    events: Arc<dyn UpdateEventSink>,
    revoked: Revoked,
) -> Result<Arc<dyn Handler>, D> {
    if !jobs.allows_defaults()
        || !auth.uses_store_pool(store)
        || !dispatcharr_store::core_task_rehash::available(&auth)
    {
        return Err(D::UnsupportedDomain);
    }
    Ok(Arc::new(Worker {
        inner: Prepared::resources(jobs, auth, events, revoked, None),
        family: Family::RehashStreams,
    }))
}

#[derive(Clone)]
pub struct Prepared {
    inner: Arc<Inner>,
}
impl Prepared {
    #[expect(
        clippy::too_many_arguments,
        reason = "Selected shares the existing owned service ports"
    )]
    pub fn new(
        jobs: Jobs,
        store: Store,
        auth: AuthService,
        recordings: Arc<dyn RecordingJobs>,
        recording_http: RecordingHttp,
        scheduled_vod: ScheduledVod,
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
            inner: Self::resources(
                jobs,
                auth,
                events,
                revoked,
                Some(SelectedResources {
                    store,
                    recordings,
                    recording_http,
                    scheduled_vod,
                }),
            ),
        })
    }

    fn resources(
        jobs: Jobs,
        auth: AuthService,
        events: Arc<dyn UpdateEventSink>,
        revoked: Revoked,
        selected: Option<SelectedResources>,
    ) -> Arc<Inner> {
        let capacity = if selected.is_some() { 2 } else { 1 };
        Arc::new(Inner {
            jobs,
            auth,
            selected,
            events,
            revoked,
            closed: AtomicBool::new(false),
            slots: Arc::new(Semaphore::new(capacity)),
            uncertain: Mutex::new(Vec::with_capacity(capacity)),
        })
    }

    pub fn entries(&self) -> Vec<(Family, Arc<dyn Handler>)> {
        [
            Family::RehashStreams,
            Family::RescheduleOffsets,
            Family::RecurringPurge,
            Family::RecurringRebuild,
            Family::ScheduledSeriesEpisodes,
            Family::ScheduledMovieAdvanced,
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
const CLAIM_PENDING: u8 = 0;
const CLAIM_STARTED: u8 = 1;
const CLAIM_CANCELLED: u8 = 2;

enum ScheduledVodTask {
    Series(i64),
    Movie(i64),
}
async fn run_scheduled_vod<F>(
    inner: Arc<Inner>,
    task: ScheduledVodTask,
    owner: Arc<WorkOwner>,
    broker_claim: F,
) -> Result<D, D>
where
    F: Future<Output = Result<Message, crate::transport::Error>> + Send + 'static,
{
    let (sender, receiver) = oneshot::channel();
    let claim_state = Arc::new(AtomicU8::new(CLAIM_PENDING));
    let authorizer = {
        let inner = inner.clone();
        let owner = owner.clone();
        let claim_state = claim_state.clone();
        move || async move {
            if inner.closed.load(Ordering::Acquire) {
                let _ = sender.send(Err(D::Operational(
                    "Core intake stopped before claim".into(),
                )));
                return false;
            }
            if claim_state
                .compare_exchange(
                    CLAIM_PENDING,
                    CLAIM_STARTED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                return false;
            }
            let status = match broker_claim.await {
                Ok(message) => match inner.revoked.contains(&message.id.to_string()) {
                    Ok(revoked) => Ok(Some((message, revoked))),
                    Err(_) => {
                        retain(&inner, owner);
                        Err(D::Operational(
                            "Scheduled VOD revoke membership unsettled".into(),
                        ))
                    }
                },
                Err(crate::transport::Error::ClaimRace) => Ok(None),
                Err(_) => {
                    retain(&inner, owner);
                    Err(D::Operational("Scheduled VOD claim unsettled".into()))
                }
            };
            let run = matches!(status, Ok(Some((_, false))));
            let _ = sender.send(status);
            run
        }
    };
    let scheduled_vod = &inner.selected()?.scheduled_vod;
    let claimed = match task {
        ScheduledVodTask::Series(account) => scheduled_vod
            .run_series(account, owner.clone(), authorizer)
            .await
            .map(|count| {
                count.map(|count| {
                    json!(format!(
                        "Batch episode refresh completed for {count} series"
                    ))
                })
            }),
        ScheduledVodTask::Movie(relation) => scheduled_vod
            .run_movie(relation, owner.clone(), authorizer)
            .await
            .map(|result| result.map(|result| json!(result))),
    };
    let value = match claimed {
        Ok(result) => result,
        Err(error)
            if claim_state
                .compare_exchange(
                    CLAIM_PENDING,
                    CLAIM_CANCELLED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok() =>
        {
            return Err(match error {
                ScheduledVodError::Capacity => D::Capacity,
                ScheduledVodError::Unsupported => D::UnsupportedDomain,
                ScheduledVodError::Unsettled => {
                    D::Operational("Scheduled VOD admission unsettled".into())
                }
            });
        }
        Err(_) => {
            retain(&inner, owner);
            return Err(D::Operational("Scheduled VOD execution unsettled".into()));
        }
    };
    let Some((message, revoked)) = receiver.await.map_err(|_| {
        retain(&inner, owner.clone());
        D::Operational("Scheduled VOD claim reply lost".into())
    })??
    else {
        return Ok(D::HeadChanged);
    };
    let value = match (value, revoked) {
        (None, true) => Value::Null,
        (Some(result), false) => result,
        _ => {
            retain(&inner, owner);
            return Err(D::Operational(
                "Scheduled VOD terminal state inconsistent".into(),
            ));
        }
    };
    if inner
        .jobs
        .write_core_task(&message, value, Vec::new(), revoked, owner.clone())
        .await
        .is_err()
    {
        retain(&inner, owner);
        return Err(D::Operational("Scheduled VOD backend unsettled".into()));
    }
    Ok(if revoked {
        D::Revoked(message.id)
    } else {
        D::Completed(message.id)
    })
}

struct Worker {
    inner: Arc<Inner>,
    family: Family,
}
impl Worker {
    async fn run(&self, expected: Option<Arc<[u8]>>, caller: Arc<dyn Send + Sync>) -> Result<D, D> {
        if self.inner.closed.load(Ordering::Acquire) {
            return self.inner.closed_before_claim("Core workers closed");
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
            if inner.selected.is_none() && !owned_provider_rehash(&candidate.message.kind) {
                return Err(D::UnsupportedDomain);
            }
            let scheduled = match candidate.message.kind {
                Kind::ScheduledSeriesEpisodes(account) => Some(ScheduledVodTask::Series(account)),
                Kind::ScheduledMovieAdvanced(relation) => Some(ScheduledVodTask::Movie(relation)),
                _ => None,
            };
            if let Some(task) = scheduled {
                let jobs = inner.jobs.clone();
                let claim_owner = owner.clone();
                let claim = async move { jobs.claim_core_task(candidate, claim_owner).await };
                return run_scheduled_vod(inner, task, owner, claim).await;
            }
            match &candidate.message.kind {
                Kind::Rehash(_) => {
                    dispatcharr_store::core_task_rehash::preflight(&inner.auth).await
                }
                Kind::RecurringPurge(rule) => preflight_recurring_purge(&inner.selected()?.store, *rule).await,
                Kind::RecurringRebuild { rule, schedule } => inner.selected()?.recording_http
                    .preflight_configured_recurring_rebuild(*rule, schedule, owner.clone()).await,
                Kind::ScheduledSeriesEpisodes(_) | Kind::ScheduledMovieAdvanced(_) => unreachable!("separate pre-claim refresh admission"),
                Kind::Offsets => {
                    inner.selected()?
                        .recordings
                        .admit_scheduling(owner.clone())
                        .await
                        .map_err(|error| match error {
                            dispatcharr_store::recording_http::RecordingEffectError::Unsupported => D::UnsupportedDomain,
                            _ => D::Operational("Recording domain admission is unsettled".into()),
                        })?;
                    dispatcharr_store::core_task_offsets::preflight(&inner.selected()?.store).await
                }
            }
            .map_err(|_| D::UnsupportedDomain)?;
            if inner.closed.load(Ordering::Acquire) {
                return inner.closed_before_claim("Core intake stopped before claim");
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
                                &inner.selected()?.store,
                                &inner.auth,
                                inner.events.clone(),
                                *rule,
                                owner.clone(),
                            )
                            .await
                            .map_err(|error| D::Operational(format!("Recurring purge effects {error:?}")))?),
                            Vec::new(),
                        ),
                        Kind::RecurringRebuild { rule, schedule } => {
                            let report = inner.selected()?.recording_http
                                .execute_configured_recurring_rebuild(*rule, schedule, message.id, owner.clone())
                                .await.map_err(|error| D::Operational(format!("Recurring rebuild effects {error:?}")))?;
                            (report.value, report.children)
                        }
                        Kind::ScheduledSeriesEpisodes(_) | Kind::ScheduledMovieAdvanced(_) => unreachable!("separate pre-claim refresh admission"),
                        Kind::Offsets => {
                            let selected = inner.selected()?;
                            let report = dispatcharr_store::core_task_offsets::execute(
                                selected.store.clone(),
                                selected.recordings.clone(),
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
            let owned = tokio::time::timeout(
                timeout,
                self.inner
                    .slots
                    .clone()
                    .acquire_many_owned(if self.inner.selected.is_some() { 2 } else { 1 }),
            )
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
    fn provider_rehash_owns_only_the_single_account_handoff() {
        let request = Request {
            keys: vec!["name".into()],
            account: Some(27),
            preacquired: true,
            ids: vec![27],
            token: Some("a".repeat(32)),
        };
        assert!(owned_provider_rehash(&Kind::Rehash(request.clone())));
        assert!(!owned_provider_rehash(&Kind::Rehash(Request {
            account: None,
            ..request.clone()
        })));
        assert!(!owned_provider_rehash(&Kind::Rehash(Request {
            ids: vec![27, 31],
            ..request.clone()
        })));
        assert!(!owned_provider_rehash(&Kind::Rehash(Request {
            preacquired: false,
            ..request.clone()
        })));
        assert!(!owned_provider_rehash(&Kind::Rehash(Request {
            token: Some("a".repeat(31)),
            ..request.clone()
        })));
        assert!(!owned_provider_rehash(&Kind::Rehash(Request {
            token: None,
            ..request
        })));
        assert!(!owned_provider_rehash(&Kind::Offsets));
    }
    #[test]
    fn configured_rebuild_requires_exact_saved_rule_and_two_day_horizon() {
        let task = Uuid::new_v4();
        let raw = crate::protocol::encode(
            &crate::protocol::Task::CreateBackup,
            task,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "rust@configured-rebuild",
        )
        .unwrap();
        let mut frame: Value = serde_json::from_slice(&raw).unwrap();
        frame["headers"]["task"] = json!("apps.channels.tasks.rebuild_recurring_rule");
        frame["headers"]["periodic_task_name"] = json!("owned-recurring-rebuild");
        frame["headers"]["argsrepr"] = json!("[27]");
        frame["headers"]["kwargsrepr"] = json!("{'horizon_days': 2}");

        let body = |args: Value, kwargs: Value| {
            STANDARD.encode(
                serde_json::to_vec(&json!([args,kwargs,
                {"callbacks":null,"errbacks":null,"chain":null,"chord":null}]))
                .unwrap(),
            )
        };
        frame["body"] = json!(body(json!([27]), json!({"horizon_days":2})));
        let bytes = serde_json::to_vec(&frame).unwrap();
        let message = parse(&bytes).unwrap();
        assert_eq!(message.id, task);
        assert_eq!(message.kind.family(), Family::RecurringRebuild);
        assert!(
            matches!(message.kind, Kind::RecurringRebuild { rule: 27, schedule }
                         if schedule == "owned-recurring-rebuild")
        );
        assert_eq!(
            crate::milestone_dispatch::classify(&bytes),
            Some(Family::RecurringRebuild)
        );

        for (args, kwargs) in [
            (json!(["27"]), json!({"horizon_days":2})),
            (json!([true]), json!({"horizon_days":2})),
            (json!([27, 2]), json!({})),
            (json!([27]), json!({"horizon_days":14})),
            (json!([27]), json!({"horizon_days":"2"})),
            (json!([27]), json!({"horizon_days":2,"extra":true})),
        ] {
            let mut refused = frame.clone();
            refused["body"] = json!(body(args, kwargs));
            assert!(parse(&serde_json::to_vec(&refused).unwrap()).is_err());
        }
        frame["headers"]["periodic_task_name"] = Value::Null;
        assert!(parse(&serde_json::to_vec(&frame).unwrap()).is_err());
    }

    #[test]
    fn scheduled_series_only_accepts_the_saved_age_selection() {
        let id = Uuid::new_v4();
        let raw = crate::protocol::encode(
            &crate::protocol::Task::CreateBackup,
            id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "fixture@scheduled-series",
        )
        .unwrap();
        let mut frame: Value = serde_json::from_slice(&raw).unwrap();
        frame["headers"]["task"] = json!(SCHEDULED_SERIES_EPISODES);
        frame["headers"]["argsrepr"] = json!("[27]");
        crate::periodic_headers::put(&mut frame, "owned-episode-batch").unwrap();
        let no_children = json!({"callbacks":null,"errbacks":null,"chain":null,"chord":null});
        frame["body"] =
            json!(STANDARD.encode(serde_json::to_vec(&json!([[27], {}, no_children])).unwrap()));
        let raw = serde_json::to_vec(&frame).unwrap();
        let parsed = parse(&raw).unwrap();
        assert_eq!(parsed.id, id);
        assert!(matches!(parsed.kind, Kind::ScheduledSeriesEpisodes(27)));
        assert_eq!(
            crate::milestone_dispatch::classify(&raw),
            Some(Family::ScheduledSeriesEpisodes)
        );

        for body in [
            json!([[27, []], {}, no_children]),
            json!([[27], {"series_ids":[31]}, no_children]),
            json!([[0], {}, no_children]),
            json!([[true], {}, no_children]),
        ] {
            frame["body"] = json!(STANDARD.encode(serde_json::to_vec(&body).unwrap()));
            assert!(parse(&serde_json::to_vec(&frame).unwrap()).is_err());
        }
    }

    #[test]
    fn scheduled_movie_uses_default_force_in_the_saved_scope() {
        let id = Uuid::new_v4();
        let raw = crate::protocol::encode(
            &crate::protocol::Task::CreateBackup,
            id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "fixture@scheduled-movie",
        )
        .unwrap();
        let mut frame: Value = serde_json::from_slice(&raw).unwrap();
        frame["headers"]["task"] = json!(SCHEDULED_MOVIE_ADVANCED);
        frame["headers"]["argsrepr"] = json!("[27]");
        crate::periodic_headers::put(&mut frame, "owned-movie-recent").unwrap();
        let children = json!({"callbacks":null,"errbacks":null,"chain":null,"chord":null});
        let encoded = |args: Value, kwargs: Value| {
            STANDARD.encode(serde_json::to_vec(&json!([args, kwargs, children])).unwrap())
        };
        frame["body"] = json!(encoded(json!([27]), json!({})));
        let raw = serde_json::to_vec(&frame).unwrap();
        assert_eq!(parse(&raw).unwrap().id, id);
        assert!(matches!(
            parse(&raw).unwrap().kind,
            Kind::ScheduledMovieAdvanced(27)
        ));
        assert_eq!(
            crate::milestone_dispatch::classify(&raw),
            Some(Family::ScheduledMovieAdvanced)
        );
        for (args, kwargs) in [
            (json!([27, true]), json!({})),
            (json!([27]), json!({"force_refresh":true})),
            (json!([0]), json!({})),
        ] {
            frame["body"] = json!(encoded(args, kwargs));
            assert!(parse(&serde_json::to_vec(&frame).unwrap()).is_err());
        }
    }

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
