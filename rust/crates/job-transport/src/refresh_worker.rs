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
pub trait RefreshExecutor: Send + Sync + 'static {
    fn execute<'a>(
        &'a self,
        message: RefreshMessage,
        publisher: ChildPublisher,
        owner: Arc<dyn Send + Sync>,
    ) -> ExecuteFuture<'a>;
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
    owner: Arc<dyn Send + Sync>,
    children: ChildRecords,
    child_slots: Arc<Semaphore>,
    failed: Arc<AtomicBool>,
}
impl ChildPublisher {
    pub(crate) fn context(
        jobs: Jobs,
        message: RefreshMessage,
        owner: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            jobs,
            root: message.root,
            parent: message.task_id,
            owner,
            children: Arc::new(Mutex::new(Vec::new())),
            child_slots: Arc::new(Semaphore::new(64)),
            failed: Arc::new(AtomicBool::new(false)),
        }
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
                if self.failed.load(Ordering::Acquire) {
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
        }
    }
}
impl RefreshExecutor for ServiceExecutor {
    fn execute<'a>(
        &'a self,
        message: RefreshMessage,
        publisher: ChildPublisher,
        owner: Arc<dyn Send + Sync>,
    ) -> ExecuteFuture<'a> {
        Box::pin(async move {
            let dispatcher = Arc::new(RefreshDispatcher::new(
                message.root,
                message.task_id,
                Arc::new(publisher.clone()),
                self.store.clone(),
                self.clock.clone(),
            ));
            let report = self
                .runner
                .run_report_with_dispatcher(
                    FileJobRequest {
                        source_id: message.source_id,
                        force: message.force,
                        defer_retry: message.retry,
                        cancelled: Arc::new(AtomicBool::new(false)),
                    },
                    owner,
                    dispatcher,
                )
                .await
                .map_err(|_| Error::Execution)?;
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
                    if value
                        != format!(
                            "EPG source {} does not exist, task cleaned up",
                            message.source_id
                        )
                    {
                        Err(Error::Execution)
                    } else {
                        Ok(NormalReturn::MissingSource)
                    }
                }
                // Coarse Failed can hide an unclassified channel/SQL/runtime cause. Never
                // manufacture Source normalSUCCESS before detailed failure gates exist.
                FileJobOutcome::Failed | FileJobOutcome::Unsupported(_) => Err(Error::Execution),
            }
        })
    }
}

#[cfg(test)]
#[path = "refresh_worker_peer.rs"]
mod peer;

#[cfg(test)]
#[path = "refresh_worker_physical.rs"]
mod physical;
