//! Per-channel XML programme work; normal Source refresh-busy retry extension.
//! Original Dispatcharr authors retain application and algorithm credit.
//! Shared ETA delivery is opaque; legacy captured constructors stay unchanged.
use crate::{transport::TvgJobs, tvg_programme_cache::Cache, tvg_programme_files::LocalFiles};
use dispatcharr_coordination::{TaskLockDetailedError, TaskLockError, TaskLocks};
use dispatcharr_ingest_epg::{cache_policy::CachedSwapPolicy, SwapPolicyContext};
use dispatcharr_store::Store;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
#[derive(Debug)]
pub enum Error {
    Closed,
    Capacity,
    Unsupported,
    Transport(crate::transport::Error),
    Store(Box<dispatcharr_store::tvg_programme::Error>),
    Cache(crate::tvg_programme_cache::Error),
    File(String),
    Lock(String),
    Policy(String),
    Supervisor,
    Download(dispatcharr_store::xmltv_download::Error),
    Staging(dispatcharr_store::tvg_programme_stage::Error),
    DownloadClose {
        first: Option<Box<Error>>,
        close: Box<Error>,
    },
    Drain,
    AfterBody {
        body: Option<Box<Error>>,
        release: Option<Box<Error>>,
        leave: Option<Box<Error>>,
    },
}
#[derive(Debug, PartialEq, Eq)]
pub enum Run {
    Empty,
    UnsupportedHead,
    Completed(uuid::Uuid),
    Failed(uuid::Uuid),
}
struct Owner {
    _caller: Arc<dyn Send + Sync>,
    _slot: OwnedSemaphorePermit,
}
struct Receipt {
    owner: Option<Arc<Owner>>,
    done: Option<oneshot::Sender<()>>,
}
impl Drop for Receipt {
    fn drop(&mut self) {
        drop(self.owner.take());
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
    }
}
struct Inner {
    jobs: TvgJobs,
    download: std::sync::OnceLock<dispatcharr_store::xmltv_download::Downloader>,
    staging: std::sync::OnceLock<dispatcharr_store::tvg_programme_stage::Service>,
    store: Store,
    locks: TaskLocks,
    cache: Cache,
    files: Arc<LocalFiles>,
    policy: Arc<CachedSwapPolicy>,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
}
#[derive(Clone)]
pub struct Worker {
    inner: Arc<Inner>,
}
impl Worker {
    pub(crate) fn bind_staging(
        &self,
        service: dispatcharr_store::tvg_programme_stage::Service,
    ) -> Result<(), Error> {
        self.inner
            .staging
            .set(service)
            .map_err(|_| Error::Unsupported)
    }
    pub(crate) fn bind_download(
        &self,
        download: dispatcharr_store::xmltv_download::Downloader,
    ) -> Result<(), Error> {
        self.inner
            .download
            .set(download)
            .map_err(|_| Error::Unsupported)
    }
    pub fn prepare(
        jobs: TvgJobs,
        store: Store,
        locks: TaskLocks,
        cache: Cache,
        files: Arc<LocalFiles>,
        policy: Arc<CachedSwapPolicy>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                jobs,
                download: std::sync::OnceLock::new(),
                staging: std::sync::OnceLock::new(),
                store,
                locks,
                cache,
                files,
                policy,
                slots: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
            }),
        }
    }
    pub async fn run_once(&self, caller: Arc<dyn Send + Sync>) -> Result<Run, Error> {
        self.run_expected(None, caller).await
    }
    pub(crate) async fn run_expected(
        &self,
        expected: Option<Arc<[u8]>>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Run, Error> {
        self.run_delivery(expected, None, caller).await
    }
    pub(crate) async fn run_reserved(
        &self,
        reservation: crate::xml_tvg_eta::Reservation,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Run, Error> {
        self.run_delivery(None, Some(reservation), caller).await
    }
    async fn run_delivery(
        &self,
        expected: Option<Arc<[u8]>>,
        reserved: Option<crate::xml_tvg_eta::Reservation>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Run, Error> {
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
            _caller: caller,
            _slot: slot,
        });
        let inner = self.inner.clone();
        tokio::spawn(async move { run(inner, owner, expected, reserved).await })
            .await
            .map_err(|_| Error::Supervisor)?
    }
    pub fn available_slots(&self) -> usize {
        self.inner.slots.available_permits()
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.inner.closed.store(true, Ordering::Release);
        let permit = tokio::time::timeout(timeout, self.inner.slots.clone().acquire_owned())
            .await
            .map_err(|_| Error::Drain)?
            .map_err(|_| Error::Drain)?;
        drop(permit);
        Ok(())
    }
}
async fn run(
    inner: Arc<Inner>,
    owner: Arc<Owner>,
    expected: Option<Arc<[u8]>>,
    reserved: Option<crate::xml_tvg_eta::Reservation>,
) -> Result<Run, Error> {
    let candidate = if let Some(reserved) = reserved {
        inner
            .jobs
            .reserved_candidate(reserved)
            .map_err(Error::Transport)?
    } else {
        match inner
            .jobs
            .peek_selected(expected.as_deref(), owner.clone())
            .await
        {
            Ok(Some(v)) => v,
            Ok(None) => return Ok(Run::Empty),
            Err(crate::transport::Error::UnsupportedTask) => return Ok(Run::UnsupportedHead),
            Err(e) => return Err(Error::Transport(e)),
        }
    };
    let id = candidate.message.epg_data_id;
    let task = candidate.message.task_id;
    let context = inner
        .store
        .tvg_programme_context(id, owner.clone())
        .await
        .map_err(store_error)?;
    let mut input = None;
    let mut download = None;
    let mut staging = None;
    if let Some(context) = context.as_ref() {
        if context.source_type != "dummy" {
            if context.source_type != "xmltv" {
                return Ok(Run::UnsupportedHead);
            }
            if inner
                .cache
                .source_refresh_held(context.source_id, owner.clone())
                .await
                .map_err(Error::Cache)?
            {
                if !inner.jobs.supports_deferral() {
                    return Ok(Run::UnsupportedHead);
                }
                let completed = inner
                    .jobs
                    .complete_deferred(candidate, owner.clone())
                    .await
                    .map_err(Error::Transport)?;
                return Ok(Run::Completed(completed));
            }
            if context.mapped || candidate.message.force {
                let configured = context
                    .extracted_file_path
                    .as_ref()
                    .filter(|s| !s.is_empty())
                    .or(context.file_path.as_ref().filter(|s| !s.is_empty()));
                let fallback = match configured {
                    Some(_) => None,
                    None => Some(
                        inner
                            .download
                            .get()
                            .ok_or(Error::Unsupported)?
                            .cache_seed(context.source_id)
                            .map_err(Error::Download)?,
                    ),
                };
                let path = configured
                    .map(String::as_str)
                    .or_else(|| fallback.as_ref().and_then(|v| v.to_str()))
                    .ok_or(Error::Unsupported)?;
                match inner.files.open(path) {
                    Ok(file) => input = Some(file),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        let Some(port) = inner.download.get() else {
                            return Err(Error::File(format!("{error:?}")));
                        };
                        download = Some(
                            port.admit(context.source_id, owner.clone())
                                .await
                                .map_err(|e| match e {
                                    dispatcharr_store::xmltv_download::Error::Capacity => {
                                        Error::Capacity
                                    }
                                    other => Error::Download(other),
                                })?
                                .for_missing_tvg()
                                .await
                                .map_err(Error::Download)?,
                        );
                    }
                    Err(error) => return Err(Error::File(format!("{error:?}"))),
                }
            }
        }
    }
    if input.is_some() {
        if let Some(service) = inner.staging.get() {
            staging = Some(
                service
                    .admit(id, owner.clone())
                    .await
                    .map_err(|e| match e {
                        dispatcharr_store::tvg_programme_stage::Error::Store(
                            dispatcharr_store::StoreError::StatusBody(503, _),
                        ) => Error::Capacity,
                        other => Error::Staging(other),
                    })?,
            );
        }
    }
    let claimed = match inner.jobs.claim(candidate, owner.clone()).await {
        Ok(claimed) => claimed,
        Err(error) => {
            if let Some(admission) = download.take() {
                admission.close().await.map_err(Error::Download)?;
            }
            if let Some(stage) = staging.take() {
                stage.discard().await.map_err(Error::Staging)?;
            }
            return Err(Error::Transport(error));
        }
    };
    let outcome = async {
        use crate::transport::TvgNormal as Normal;
        let normal = if let Some(context) = context {
            if context.source_type == "dummy" {
                Normal::Null
            } else {
                inner
                    .cache
                    .enter(context.source_id, owner.clone())
                    .await
                    .map_err(Error::Cache)?;
                let lease = inner
                    .locks
                    .acquire_epg_programmes_retained(id, owner.clone())
                    .await;
                let (body, release) = match lease {
                    Err(TaskLockDetailedError::Operational(TaskLockError::Held)) => {
                        (Ok(Normal::Busy), Ok(()))
                    }
                    Err(error) => (Err(Error::Lock(format!("{error:?}"))), Ok(())),
                    Ok(lease) => {
                        let body = async {
                            if let Some(admission) = download.take() {
                                // Accepted download keeps the SAME caller/Auth admission
                                // until real HTTP/disk/SQL closure, then parser ownership.
                                let outcome = if let Some(service) = inner.staging.get() {
                                    let (outcome, session) = admission
                                        .execute_retained(Arc::new(AtomicBool::new(false)))
                                        .await
                                        .map_err(Error::Download)?;
                                    staging = Some(
                                        service
                                            .on_session(id, session, owner.clone())
                                            .await
                                            .map_err(Error::Staging)?,
                                    );
                                    outcome
                                } else {
                                    admission
                                        .execute(Arc::new(AtomicBool::new(false)))
                                        .await
                                        .map_err(Error::Download)?
                                };
                                match outcome {
                                    dispatcharr_store::xmltv_download::Outcome::Installed(file)
                                        if file.source_id() == context.source_id =>
                                    {
                                        input = Some(
                                            inner
                                                .files
                                                .open(
                                                    file.path()
                                                        .to_str()
                                                        .ok_or(Error::Unsupported)?,
                                                )
                                                .map_err(|e| Error::File(format!("{e:?}")))?,
                                        );
                                    }
                                    dispatcharr_store::xmltv_download::Outcome::HandledFailure(
                                        proof,
                                    ) if proof.source_id() == context.source_id => {
                                        return Ok(Normal::Null)
                                    }
                                    _ => return Err(Error::Unsupported),
                                }
                            }
                            if let Some(file) = input {
                                if let Some(mut stage) = staging.take() {
                                    let retained = Arc::new((owner.clone(), stage.session()));
                                    let mut parser =
                                        dispatcharr_ingest_epg::tvg_programme_file::stream(
                                            file,
                                            id,
                                            context.tvg_id,
                                            retained,
                                        );
                                    let mut append_error = None;
                                    while let Some(page) = parser.next().await {
                                        if let Err(error) = stage.append(page).await {
                                            append_error = Some(error);
                                            break;
                                        }
                                    }
                                    let parsed = parser.finish().await;
                                    if let Some(error) = append_error {
                                        stage.discard().await.map_err(Error::Staging)?;
                                        return Err(Error::Staging(error));
                                    }
                                    if let Err(error) = parsed {
                                        stage.discard().await.map_err(Error::Staging)?;
                                        return Err(Error::File(format!("{error:?}")));
                                    }
                                    stage.finish().await.map_err(stage_error)?;
                                    let _caught_invalidation =
                                        inner.cache.invalidate_output(owner.clone()).await;
                                    return Ok(Normal::Null);
                                }
                                let rows = dispatcharr_ingest_epg::tvg_programme_file::read(
                                    file,
                                    id,
                                    context.tvg_id,
                                    owner.clone(),
                                )
                                .await
                                .map_err(|e| Error::File(format!("{e:?}")))?;
                                // Source samples its retention clock before settings I/O, only when
                                // the initial parse has at least one programme. Empty parse preserves
                                // every stored row, including historical rows past cutoff.
                                if !rows.is_empty() {
                                    let (tx, rx) = oneshot::channel();
                                    let retained = Arc::new(Receipt {
                                        owner: Some(owner.clone()),
                                        done: Some(tx),
                                    });
                                    let policy = inner
                                        .policy
                                        .retained_tvg_history_policy(SwapPolicyContext {
                                            cancelled: Arc::new(AtomicBool::new(false)),
                                            retained,
                                        })
                                        .await;
                                    rx.await.map_err(|_| Error::Supervisor)?;
                                    let policy =
                                        policy.map_err(|e| Error::Policy(format!("{e:?}")))?;
                                    let cutoff = inner
                                        .store
                                        .tvg_retention_cutoff(
                                            id,
                                            policy.now,
                                            policy.history,
                                            owner.clone(),
                                        )
                                        .await
                                        .map_err(store_error)?;
                                    inner
                                        .store
                                        .swap_tvg_programmes(id, cutoff, rows, owner.clone())
                                        .await
                                        .map_err(store_error)?;
                                }
                                // Source catches output-generation invalidation failures. Physical
                                // cache completion is fenced; raw operator diagnostic remains TODO.
                                let _caught_invalidation =
                                    inner.cache.invalidate_output(owner.clone()).await;
                            }
                            Ok(Normal::Null)
                        }
                        .await;
                        let release = lease
                            .release_owned_detailed()
                            .await
                            .map_err(|e| Error::Lock(format!("{e:?}")))
                            .and_then(|owned| {
                                if owned {
                                    Ok(())
                                } else {
                                    Err(Error::Lock("owner lost".into()))
                                }
                            });
                        (body, release)
                    }
                };
                // Source finally order is stop/release first, source counter decrement next.
                // Attempt decrement even after body/release error; retain all diagnostic
                // branches in the eventual whole-batch peer, not a fake SUCCESS policy.
                let leave = inner
                    .cache
                    .leave(context.source_id, owner.clone())
                    .await
                    .map_err(Error::Cache);
                match (body, release, leave) {
                    (Ok(normal), Ok(()), Ok(())) => normal,
                    (Err(Error::Store(store_error)), Ok(()), Ok(())) => {
                        // Only an actual known insert P0001 with completed SQL
                        // rollback/Ready/close AND successful Source finally steps
                        // becomes FAILURE. Other errors remain operational.
                        let Some(failure) =
                            crate::tvg_programme_terminal::InsertFailure::classify(&store_error)
                        else {
                            return Err(Error::AfterBody {
                                body: Some(Box::new(Error::Store(store_error))),
                                release: None,
                                leave: None,
                            });
                        };
                        let written = inner
                            .jobs
                            .write_insert_failure(claimed, failure, owner)
                            .await
                            .map_err(Error::Transport)?;
                        return Ok(if written {
                            Run::Failed(task)
                        } else {
                            Run::Completed(task)
                        });
                    }
                    (body, release, leave) => {
                        return Err(Error::AfterBody {
                            body: body.err().map(Box::new),
                            release: release.err().map(Box::new),
                            leave: leave.err().map(Box::new),
                        })
                    }
                }
            }
        } else {
            Normal::MissingEpg
        };
        inner
            .jobs
            .write_normal(claimed, normal, owner)
            .await
            .map_err(Error::Transport)?;
        Ok(Run::Completed(task))
    }
    .await;
    if let Some(admission) = download.take() {
        if let Err(error) = admission.close().await {
            return Err(Error::DownloadClose {
                first: outcome.err().map(Box::new),
                close: Box::new(Error::Download(error)),
            });
        }
    }
    if let Some(stage) = staging.take() {
        if let Err(error) = stage.discard().await {
            return Err(Error::DownloadClose {
                first: outcome.err().map(Box::new),
                close: Box::new(Error::Staging(error)),
            });
        }
    }
    outcome
}
fn store_error(error: dispatcharr_store::tvg_programme::Error) -> Error {
    Error::Store(Box::new(error))
}
fn stage_error(error: dispatcharr_store::tvg_programme_stage::Error) -> Error {
    match error {
        // Stage.finish returns this only after actor Ready and actual physical
        // close. Existing insert-P0001 classifier therefore keeps real fences.
        dispatcharr_store::tvg_programme_stage::Error::Sql {
            stage,
            source,
            rollback,
        } => store_error(dispatcharr_store::tvg_programme::Error::Sql {
            stage,
            source,
            rollback,
            ready: None,
            close: None,
        }),
        other => Error::Staging(other),
    }
}

#[cfg(test)]
mod retained_error_layout {
    use super::*;
    #[test]
    fn worker_error_stays_bounded_and_preserves_actual_sql_settlement_fields() {
        assert!(std::mem::size_of::<Error>() <= 128);
        let error = store_error(dispatcharr_store::tvg_programme::Error::Sql {
            stage: "insert",
            source: sqlx::Error::PoolClosed,
            rollback: Some(sqlx::Error::PoolTimedOut),
            ready: None,
            close: None,
        });
        let Error::Store(error) = error else {
            panic!("Store error category lost")
        };
        assert!(matches!(
            error.as_ref(),
            dispatcharr_store::tvg_programme::Error::Sql {
                stage: "insert",
                source: sqlx::Error::PoolClosed,
                rollback: Some(sqlx::Error::PoolTimedOut),
                ready: None,
                close: None,
            }
        ));
        // Boxing changes representation only; an unsettled/non-PostgreSQL cause
        // still cannot mint the existing actual INSERT/P0001 terminal receipt.
        assert!(crate::tvg_programme_terminal::InsertFailure::classify(&error).is_none());
    }
}
