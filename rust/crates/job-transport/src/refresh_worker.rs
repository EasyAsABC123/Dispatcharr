//! Bounded immediate refresh consumer and observed normal return writer.
//! Original Dispatcharr authors retain application/algorithm credit and AGPL.
//! No ETA consumer or scheduler authority; unrelated/parented heads are retained.
use crate::{
    refresh_dispatch::{
        parse_native_child, ChildPublication, PublishFuture, RefreshDispatcher, RefreshPublisher,
    },
    refresh_protocol::RefreshMessage,
    transport::{Error as TransportError, Jobs},
};
use dispatcharr_ingest_epg::{
    file_job::{FileJobOutcome, FileJobRequest, FileJobRunner},
    ImportError, SwapPolicyContext,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NormalReturn {
    Null,
    MissingSource,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Closed,
    Capacity,
    Transport(TransportError),
    Execution,
    WorkerPanic,
    DrainTimeout,
}
pub type ExecuteFuture<'a> = Pin<Box<dyn Future<Output = Result<NormalReturn, Error>> + Send + 'a>>;
pub use crate::refresh_aggregate::XmltvSources;

pub struct AggregateCompletion {
    pub(crate) children: Vec<Uuid>,
    pub(crate) series: Vec<Uuid>,
}
pub type AggregateFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AggregateCompletion, Error>> + Send + 'a>>;
pub trait RefreshExecutor: Send + Sync + 'static {
    fn execute<'a>(
        &'a self,
        message: RefreshMessage,
        publisher: ChildPublisher,
        owner: Arc<dyn Send + Sync>,
    ) -> ExecuteFuture<'a>;

    fn execute_aggregate<'a>(
        &'a self,
        _sources: XmltvSources,
        _publisher: ChildPublisher,
        _owner: Arc<dyn Send + Sync>,
    ) -> AggregateFuture<'a> {
        Box::pin(async { Err(Error::Configuration) })
    }
}
struct Owner {
    _slot: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
}
struct ChildBudget {
    _permit: OwnedSemaphorePermit,
}
type ChildRecords = Arc<Mutex<Vec<(Uuid, Arc<ChildBudget>)>>>;
#[derive(Clone)]
pub struct ChildPublisher {
    jobs: Jobs,
    root: Uuid,
    parent: Uuid,
    parent_key: String,
    claimed_raw: Option<Arc<[u8]>>,
    observed_trail: Arc<Mutex<Option<serde_json::Value>>>,
    trail_closed: Arc<AtomicBool>,
    owner: Arc<dyn Send + Sync>,
    children: ChildRecords,
    series_children: Arc<Mutex<Vec<Uuid>>>,
    child_slots: Arc<Semaphore>,
    failed: Arc<AtomicBool>,
}
impl ChildPublisher {
    pub(crate) fn context(
        jobs: Jobs,
        message: RefreshMessage,
        owner: Arc<dyn Send + Sync>,
    ) -> Self {
        Self::with_identity(jobs, message.root, message.task_id, owner)
    }

    fn with_identity(jobs: Jobs, root: Uuid, parent: Uuid, owner: Arc<dyn Send + Sync>) -> Self {
        Self {
            jobs,
            root,
            parent,
            parent_key: parent.to_string(),
            claimed_raw: None,
            observed_trail: Arc::new(Mutex::new(None)),
            trail_closed: Arc::new(AtomicBool::new(false)),
            owner,
            children: Arc::new(Mutex::new(Vec::new())),
            series_children: Arc::new(Mutex::new(Vec::new())),
            child_slots: Arc::new(Semaphore::new(64)),
            failed: Arc::new(AtomicBool::new(false)),
        }
    }
    pub(crate) fn claimed(
        jobs: Jobs,
        message: RefreshMessage,
        owner: Arc<dyn Send + Sync>,
        raw: Vec<u8>,
    ) -> Self {
        let mut publisher = Self::context(jobs, message, owner);
        publisher.claimed_raw = Some(raw.into());
        publisher
    }
    pub(crate) fn aggregate(
        jobs: Jobs,
        id: Uuid,
        owner: Arc<dyn Send + Sync>,
        raw: Vec<u8>,
    ) -> Self {
        let mut publisher = Self::with_identity(jobs, id, id, owner);
        publisher.claimed_raw = Some(raw.into());
        publisher
    }

    pub(crate) fn task_id(&self) -> Uuid {
        self.parent
    }

    fn inline_source(&self) -> Self {
        let mut scoped = Self::with_identity(
            self.jobs.clone(),
            self.root,
            self.parent,
            self.owner.clone(),
        );
        scoped.claimed_raw = self.claimed_raw.clone();
        scoped.child_slots = self.child_slots.clone();
        scoped
    }

    pub(crate) fn observed_trail(&self) -> Result<Option<serde_json::Value>, Error> {
        self.observed_trail
            .lock()
            .map(|value| value.clone())
            .map_err(|_| Error::Execution)
    }
    /// Called only after the actual native producer flow joined all effects.
    /// A later publication is rejected rather than silently omitted from hooks.
    pub(crate) fn close_native_trail(&self) -> Result<(), Error> {
        self.completed_children()?;
        self.trail_closed.store(true, Ordering::Release);
        Ok(())
    }
    pub(crate) fn completed_series_children(&self) -> Result<Vec<Uuid>, Error> {
        self.completed_children()?;
        self.series_children
            .lock()
            .map(|children| children.clone())
            .map_err(|_| Error::Execution)
    }
    pub(crate) fn completed_children(&self) -> Result<Vec<Uuid>, Error> {
        if self.failed.load(Ordering::Acquire) {
            return Err(Error::Execution);
        }
        let children = self
            .children
            .lock()
            .unwrap()
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        if children.len() > 64 {
            return Err(Error::Execution);
        }
        Ok(children)
    }
}
impl RefreshPublisher for ChildPublisher {
    fn publish<'a>(
        &'a self,
        id: Uuid,
        bytes: Vec<u8>,
        context: SwapPolicyContext,
    ) -> PublishFuture<'a> {
        Box::pin(async move {
            let result = async {
                context.ensure_active()?;
                if self.failed.load(Ordering::Acquire) || self.trail_closed.load(Ordering::Acquire)
                {
                    return Err(ImportError::Worker);
                }
                let budget = Arc::new(ChildBudget {
                    _permit: self
                        .child_slots
                        .clone()
                        .try_acquire_owned()
                        .map_err(|_| ImportError::Capacity("refresh child count"))?,
                });
                let child = parse_native_child(&bytes).map_err(|_| ImportError::Worker)?;
                if child.id != id
                    || child.root != self.root
                    || child.parent != self.parent
                    || self.children.lock().unwrap().len() >= 64
                {
                    return Err(ImportError::Capacity("refresh child contract"));
                }
                let retained: Arc<dyn Send + Sync> =
                    Arc::new((self.owner.clone(), context.retained.clone(), budget.clone()));
                match &child.child {
                    ChildPublication::Refresh { .. } => {
                        let message = crate::refresh_protocol::parse(&bytes)
                            .map_err(|_| ImportError::Worker)?;
                        self.jobs
                            .publish_refresh(&message, retained)
                            .await
                            .map_err(|_| ImportError::Worker)?;
                    }
                    ChildPublication::ProgrammeIndex { .. } | ChildPublication::SeriesRules => {
                        self.jobs
                            .publish_refresh_child_fixture(&child, retained)
                            .await
                            .map_err(|_| ImportError::Worker)?;
                    }
                }
                context.ensure_active()?;
                if matches!(child.child, ChildPublication::SeriesRules) {
                    self.series_children
                        .lock()
                        .map_err(|_| ImportError::Worker)?
                        .push(id);
                }
                self.children.lock().unwrap().push((id, budget));
                Ok(())
            }
            .await;
            if result.is_err() {
                self.failed.store(true, Ordering::Release)
            }
            result
        })
    }
}
/// This trusted compiled port reads only the private vector populated after
/// validated native publication replies. Tuple metadata grants no job authority.
impl dispatcharr_store::NativeTaskTrail for ChildPublisher {
    fn claimed_task_id(&self) -> &str {
        &self.parent_key
    }
    fn settled_after(
        &self,
        cursor: u64,
    ) -> Result<dispatcharr_store::NativeTaskTrailDelta, dispatcharr_store::StoreError> {
        use dispatcharr_store::{NativeTaskTrailDelta, NativeTaskTrailEntry, NativeTaskTrailState};
        let children = self.children.lock().map_err(|_| {
            dispatcharr_store::StoreError::Unsupported("Native refresh trail lock unavailable")
        })?;
        let start = usize::try_from(cursor).map_err(|_| {
            dispatcharr_store::StoreError::Unsupported("Native refresh trail cursor invalid")
        })?;
        if start > children.len() {
            return Err(dispatcharr_store::StoreError::Unsupported(
                "Native refresh trail cursor beyond settled publications",
            ));
        }
        let entries = children
            .iter()
            .enumerate()
            .skip(start)
            .map(|(index, (id, _))| NativeTaskTrailEntry {
                ordinal: index as u64 + 1,
                source_result_tuple: serde_json::json!([[id.to_string(), null], null]),
            })
            .collect();
        let state = if self.failed.load(Ordering::Acquire) {
            NativeTaskTrailState::Unsettled
        } else if self.trail_closed.load(Ordering::Acquire) {
            NativeTaskTrailState::Closed
        } else {
            NativeTaskTrailState::Open
        };
        Ok(NativeTaskTrailDelta {
            next_cursor: children.len() as u64,
            entries,
            state,
        })
    }
}
struct Inner {
    jobs: Jobs,
    executor: Arc<dyn RefreshExecutor>,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
}
#[derive(Clone)]
pub struct RefreshWorker {
    inner: Arc<Inner>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Run {
    Empty,
    Completed(Uuid),
    ResultAlreadySucceeded(Uuid),
}
impl RefreshWorker {
    pub fn new_fixture(jobs: Jobs, executor: Arc<dyn RefreshExecutor>) -> Result<Self, Error> {
        let db = jobs.copied_database().ok_or(Error::Configuration)?;
        if std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref() != Ok("playback-fixture-only")
            || std::env::var("POSTGRES_DB").as_deref() != Ok(db)
            || !db.starts_with("rust_probe_backup_jobs_")
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                executor,
                slots: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
            }),
        })
    }
    /// Explicit immediate-refresh consumer. The typed Jobs constructor keeps
    /// backup/restore authority separate; no polling begins until run_once.
    pub fn new(jobs: Jobs, executor: Arc<dyn RefreshExecutor>) -> Result<Self, Error> {
        if !jobs.allows_refresh() {
            return Err(Error::Configuration);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                executor,
                slots: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
            }),
        })
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
        tokio::spawn(async move {
            let Some(message) = inner
                .jobs
                .claim_refresh_fixture(owner.clone())
                .await
                .map_err(Error::Transport)?
            else {
                return Ok(Run::Empty);
            };
            let publisher =
                ChildPublisher::context(inner.jobs.clone(), message.clone(), owner.clone());
            let result = inner
                .executor
                .execute(message.clone(), publisher.clone(), owner.clone())
                .await?;
            let children = publisher.completed_children()?;
            match inner
                .jobs
                .write_refresh_success_fixture(&message, result, children, owner)
                .await
                .map_err(Error::Transport)?
            {
                true => Ok(Run::Completed(message.task_id)),
                false => Ok(Run::ResultAlreadySucceeded(message.task_id)),
            }
        })
        .await
        .map_err(|_| Error::WorkerPanic)?
    }
    /// Close this intake and drain its owned work only. The shared Jobs transport
    /// is closed by the fixture owner after all worker families/services drain.
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.inner.closed.store(true, Ordering::Release);
        let _permit = tokio::time::timeout(timeout, self.inner.slots.clone().acquire_owned())
            .await
            .map_err(|_| Error::DrainTimeout)?
            .map_err(|_| Error::Closed)?;
        Ok(())
    }
}
pub struct ServiceExecutor {
    runner: FileJobRunner,
    store: dispatcharr_store::Store,
    clock: Arc<dyn Fn() -> chrono::DateTime<chrono::Utc> + Send + Sync>,
    hooks: std::sync::OnceLock<(
        dispatcharr_store::AuthService,
        dispatcharr_store::PluginRuntime,
    )>,
}
impl ServiceExecutor {
    pub fn new(
        runner: FileJobRunner,
        store: dispatcharr_store::Store,
        clock: Arc<dyn Fn() -> chrono::DateTime<chrono::Utc> + Send + Sync>,
    ) -> Self {
        Self {
            runner,
            store,
            clock,
            hooks: std::sync::OnceLock::new(),
        }
    }
    pub(crate) fn bind_hooks(
        &self,
        auth: dispatcharr_store::AuthService,
        runtime: dispatcharr_store::PluginRuntime,
    ) -> Result<(), Error> {
        self.hooks
            .set((auth, runtime))
            .map_err(|_| Error::Configuration)
    }
}
impl ServiceExecutor {
    fn execute_source<'a>(
        &'a self,
        source_id: i64,
        force: bool,
        retry: u32,
        publisher: ChildPublisher,
        owner: Arc<dyn Send + Sync>,
    ) -> ExecuteFuture<'a> {
        Box::pin(async move {
            let dispatcher = Arc::new(RefreshDispatcher::new(
                publisher.root,
                publisher.parent,
                Arc::new(publisher.clone()),
                self.store.clone(),
                self.clock.clone(),
            ));
            let cancelled = Arc::new(AtomicBool::new(false));
            let hooks = match self.hooks.get() {
                Some((auth, runtime)) => {
                    let raw = publisher.claimed_raw.as_ref().ok_or(Error::Execution)?;
                    let context = dispatcharr_store::PluginHookTaskContext::from_native_claim(raw)
                        .map_err(|_| Error::Execution)?;
                    Some(
                        dispatcharr_store::epg_task_hooks::EpgTaskHooks::prepare(
                            auth.clone(),
                            runtime.clone(),
                            context,
                            Arc::new(publisher.clone()),
                            owner.clone(),
                            cancelled.clone(),
                        )
                        .map_err(|_| Error::Execution)?,
                    )
                }
                None => None,
            };
            let outcome = self
                .runner
                .run_report_with_dispatcher_scoped(
                    FileJobRequest {
                        source_id,
                        force,
                        defer_retry: retry,
                        cancelled,
                    },
                    owner,
                    dispatcher,
                    hooks.clone(),
                )
                .await;
            let report = match outcome {
                Ok(report) => report,
                Err(_) => {
                    if let Some(hooks) = &hooks {
                        hooks.abort().await.map_err(|_| Error::Execution)?;
                    }
                    return Err(Error::Execution);
                }
            };
            if publisher.close_native_trail().is_err() {
                if let Some(hooks) = &hooks {
                    hooks.abort().await.map_err(|_| Error::Execution)?;
                }
                return Err(Error::Execution);
            }
            let completion = if let Some(hooks) = &hooks {
                match hooks.complete().await {
                    Ok(completed) => {
                        if let Some(report) = completed.report() {
                            *publisher
                                .observed_trail
                                .lock()
                                .map_err(|_| Error::Execution)? =
                                Some(report.observed_request_children.clone());
                        }
                        Some(completed)
                    }
                    Err(_) => {
                        hooks.abort().await.map_err(|_| Error::Execution)?;
                        return Err(Error::Execution);
                    }
                }
            } else {
                None
            };
            publisher.completed_children()?;
            if let Some(settled) = report.settled_exhaustion {
                use dispatcharr_ingest_epg::file_job::{
                    ExhaustionReason, ExhaustionRelease, ExhaustionReporting, ExhaustionTerminal,
                };
                let known = matches!(
                    settled.reason,
                    ExhaustionReason::FileBusy | ExhaustionReason::ParserBusy
                ) && matches!(
                    settled.reporting,
                    ExhaustionReporting::StatusSaved
                        | ExhaustionReporting::StatusUnmatchedPublished
                        | ExhaustionReporting::CaughtGuardedStatusSql(_)
                ) && matches!(
                    settled.terminal,
                    ExhaustionTerminal::Verified
                        | ExhaustionTerminal::CaughtGuardedSelectSql(_)
                        | ExhaustionTerminal::CaughtGuardedStatusSql(_)
                );
                let release_known = matches!(
                    settled.release,
                    Some(ExhaustionRelease::Released | ExhaustionRelease::NotOwnerForeignPreserved)
                );
                return if known && release_known && report.outcome == FileJobOutcome::Failed {
                    Ok(NormalReturn::Null)
                } else {
                    Err(Error::Execution)
                };
            }
            match report.outcome {
                FileJobOutcome::Imported
                | FileJobOutcome::Skipped
                | FileJobOutcome::Deferred
                | FileJobOutcome::AlreadyRunning => Ok(NormalReturn::Null),
                FileJobOutcome::MissingSource(value) => {
                    // FileJobRunner now performs the proven retained orphan
                    // helper, including Source's caught P0001 partial commits.
                    if value != format!("EPG source {} does not exist, task cleaned up", source_id)
                    {
                        Err(Error::Execution)
                    } else {
                        Ok(NormalReturn::MissingSource)
                    }
                }
                FileJobOutcome::PendingDownloadRejected(proof) => {
                    let completed = completion.as_ref().ok_or(Error::Execution)?;
                    let proof = proof.settle(completed).map_err(|_| Error::Execution)?;
                    if proof.source_id() == source_id {
                        Ok(NormalReturn::Null)
                    } else {
                        Err(Error::Execution)
                    }
                }
                FileJobOutcome::DownloadRejected(proof) if proof.source_id() == source_id => {
                    Ok(NormalReturn::Null)
                }
                FileJobOutcome::DownloadRejected(_) => Err(Error::Execution),
                // Coarse Failed can hide an unclassified channel/SQL/runtime cause. Never
                // manufacture Source normalSUCCESS before detailed failure gates exist.
                FileJobOutcome::Failed | FileJobOutcome::Unsupported(_) => Err(Error::Execution),
            }
        })
    }
}

impl RefreshExecutor for ServiceExecutor {
    fn execute<'a>(
        &'a self,
        message: RefreshMessage,
        publisher: ChildPublisher,
        owner: Arc<dyn Send + Sync>,
    ) -> ExecuteFuture<'a> {
        self.execute_source(
            message.source_id,
            message.force,
            message.retry,
            publisher,
            owner,
        )
    }

    fn execute_aggregate<'a>(
        &'a self,
        sources: XmltvSources,
        publisher: ChildPublisher,
        owner: Arc<dyn Send + Sync>,
    ) -> AggregateFuture<'a> {
        Box::pin(async move {
            let mut children = Vec::new();
            let mut series = Vec::new();
            let mut scopes = Vec::new();
            for source_id in sources.into_ids() {
                let scoped = publisher.inline_source();
                self.execute_source(source_id, false, 0, scoped.clone(), owner.clone())
                    .await?;
                children.extend(scoped.completed_children()?);
                series.extend(scoped.completed_series_children()?);
                // Hold publication permits across source scopes until all publications settle.
                scopes.push(scoped);
            }
            Ok(AggregateCompletion { children, series })
        })
    }
}

#[cfg(test)]
#[path = "refresh_worker_peer.rs"]
mod peer;

#[cfg(test)]
#[path = "refresh_worker_physical.rs"]
mod physical;
