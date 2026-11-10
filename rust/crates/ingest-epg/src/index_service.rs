//! Owned programme-index task orchestration. Original Dispatcharr authors retain
//! full application/algorithm credit. App/queue/lookup wiring remains gated.
use crate::{
    cache_policy::PolicyClock,
    file_lease::{ReleaseOutcome, ReleaseWait, RetainedLease},
    programme_index::{scan_file, IndexLimits},
    ImportError, SwapPolicyContext,
};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_coordination::{
    TaskLease, TaskLockCleanupOutcome, TaskLockDetailedError, TaskLockError, TaskLocks,
};
use dispatcharr_store::Store;
use sqlx::{Connection, Row};
use std::{
    collections::HashMap,
    fs::File,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};
use tokio::sync::{watch, Notify, OwnedSemaphorePermit, Semaphore};
pub trait IndexDispatcher: Send + Sync {
    fn defer<'a>(
        &'a self,
        source_id: i64,
        retry: u32,
        seconds: u32,
        context: SwapPolicyContext,
    ) -> BoxFuture<'a, Result<(), ImportError>>;
}
/// Detailed component observations, not Python exception/result inference.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexStage {
    AcquireFileLock,
    ChildPublication,
    MemoryAdmission,
    SourceRead,
    CacheDirectory,
    FileMetadata,
    FileOpen,
    Scan,
    IndexEncoding,
    WriterAdmission,
    RowLock,
    Write,
    Commit,
    ConnectionClose,
    UnsupportedFileKind,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexCommitObservation {
    NotWritten,
    Written,
    Ambiguous,
}
#[derive(Debug)]
pub enum IndexFailureCause {
    Operation(ImportError),
    Lock(TaskLockDetailedError),
}
#[derive(Debug)]
pub struct IndexStageFailure {
    pub stage: IndexStage,
    pub cause: IndexFailureCause,
}
#[derive(Clone, Debug)]
pub enum IndexReleaseObservation {
    NotAcquired,
    AcquisitionCleanup(TaskLockCleanupOutcome),
    Released,
    NotOwner,
    Failed(Arc<TaskLockDetailedError>),
    TimedOut,
    RuntimeUnavailable,
}
#[derive(Debug)]
pub struct IndexExecutionReport {
    pub operation: Result<IndexOutcome, IndexStageFailure>,
    pub commit: IndexCommitObservation,
    pub release: IndexReleaseObservation,
}
struct ExecutionTrace {
    stage: Mutex<IndexStage>,
    commit: AtomicU8,
}
impl ExecutionTrace {
    fn new() -> Self {
        Self {
            stage: Mutex::new(IndexStage::AcquireFileLock),
            commit: AtomicU8::new(0),
        }
    }
    fn report(
        &self,
        operation: Result<IndexOutcome, ImportError>,
        release: IndexReleaseObservation,
    ) -> IndexExecutionReport {
        IndexExecutionReport {
            operation: operation.map_err(|cause| IndexStageFailure {
                stage: *self.stage.lock().unwrap(),
                cause: IndexFailureCause::Operation(cause),
            }),
            commit: match self.commit.load(Ordering::Acquire) {
                1 => IndexCommitObservation::Ambiguous,
                2 => IndexCommitObservation::Written,
                _ => IndexCommitObservation::NotWritten,
            },
            release,
        }
    }
}
fn stage(trace: &Option<Arc<ExecutionTrace>>, value: IndexStage) {
    if let Some(trace) = trace {
        *trace.stage.lock().unwrap() = value;
    }
}
#[derive(Clone)]
pub struct IndexServices {
    pub store: Store,
    pub locks: TaskLocks,
    pub dispatcher: Arc<dyn IndexDispatcher>,
    pub clock: PolicyClock,
}
#[derive(Clone, Debug)]
pub struct IndexConfig {
    /// None preserves the existing process-CWD/cache_dir component behavior.
    pub path_context: Option<crate::current_lookup::SourcePathContext>,
    pub max_jobs: usize,
    pub limits: IndexLimits,
    pub cache_dir: PathBuf,
    pub release_timeout: Duration,
    pub max_index_json_bytes: usize,
    pub max_memory_bytes: usize,
}
pub struct IndexRequest {
    pub source_id: i64,
    pub retry: u32,
    pub cancelled: Arc<AtomicBool>,
}
#[derive(Debug, PartialEq, Eq)]
pub enum IndexOutcome {
    Written,
    MissingSource,
    MissingFile,
    Deferred,
    Exhausted,
}
#[derive(Clone)]
pub struct IndexService {
    inner: Arc<Inner>,
}
struct Inner {
    services: IndexServices,
    config: IndexConfig,
    admission: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    reservation: usize,
    stopping: AtomicBool,
    jobs: Mutex<HashMap<u64, Weak<Job>>>,
    next: AtomicU64,
    changed: Notify,
}
struct Job {
    id: u64,
    inner: Weak<Inner>,
    cancelled: Arc<AtomicBool>,
    caller: Option<Arc<dyn Send + Sync>>,
    permit: Option<OwnedSemaphorePermit>,
    memory: Mutex<Option<OwnedSemaphorePermit>>,
}
impl Drop for Job {
    fn drop(&mut self) {
        drop(self.caller.take());
        drop(self.memory.get_mut().unwrap().take());
        drop(self.permit.take());
        if let Some(inner) = self.inner.upgrade() {
            inner.jobs.lock().unwrap().remove(&self.id);
            inner.changed.notify_waiters();
        }
    }
}
struct Cancel {
    flag: Arc<AtomicBool>,
    armed: bool,
}
impl Drop for Cancel {
    fn drop(&mut self) {
        if self.armed {
            self.flag.store(true, Ordering::Release);
        }
    }
}
struct Watch(watch::Sender<bool>);
impl Drop for Watch {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}
impl Watch {
    fn new(lease: &TaskLease, cancel: Arc<AtomicBool>) -> Self {
        let mut lost = lease.loss_signal();
        let (tx, mut done) = watch::channel(false);
        tokio::spawn(async move {
            loop {
                if *lost.borrow() {
                    cancel.store(true, Ordering::Release);
                    return;
                }
                tokio::select! {_=done.changed()=>return,result=lost.changed()=>if result.is_err(){return;}}
            }
        });
        Self(tx)
    }
}
struct Context {
    _watch: Watch,
    _lease: Arc<dyn Send + Sync>,
}
impl IndexService {
    pub fn new(services: IndexServices, config: IndexConfig) -> Result<Self, ImportError> {
        if !(1..=4).contains(&config.max_jobs)
            || !config.cache_dir.is_absolute()
            || config.cache_dir.as_os_str().len() > 4096
            || config.release_timeout < Duration::from_millis(1)
            || config.release_timeout > Duration::from_secs(60)
        {
            return Err(ImportError::Capacity("index service configuration"));
        }
        config.limits.validate()?;
        // Reserve conservative scanner/tree/JSON/driver working capacity before
        // any heavy work. Queued memory waiters are bounded by job admission.
        let reservation = config
            .limits
            .state_bytes
            .checked_mul(4)
            .and_then(|n| {
                config
                    .max_index_json_bytes
                    .checked_mul(4)
                    .and_then(|json| n.checked_add(json))
            })
            .and_then(|n| n.checked_add(16 * 1024 * 1024))
            .ok_or(ImportError::Capacity("index memory configuration"))?;
        if !(65536..=64 * 1024 * 1024).contains(&config.max_index_json_bytes)
            || config.max_memory_bytes > 1024 * 1024 * 1024
            || reservation > config.max_memory_bytes
        {
            return Err(ImportError::Capacity("index memory configuration"));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                admission: Arc::new(Semaphore::new(config.max_jobs)),
                memory: Arc::new(Semaphore::new(config.max_memory_bytes)),
                reservation,
                services,
                config,
                stopping: AtomicBool::new(false),
                jobs: Mutex::new(HashMap::new()),
                next: AtomicU64::new(1),
                changed: Notify::new(),
            }),
        })
    }
    pub fn physical_jobs(&self) -> usize {
        self.inner.jobs.lock().unwrap().len()
    }
    pub async fn build(
        &self,
        request: IndexRequest,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<IndexOutcome, ImportError> {
        self.build_with_dispatcher(request, caller, self.inner.services.dispatcher.clone())
            .await
    }
    /// Select an immutable per-execution publication context without changing
    /// the service default. The dispatcher remains owned through the physical
    /// build/defer task; implementations retain SwapPolicyContext through real
    /// broker replies, including replies that outlive a logical timeout.
    pub async fn build_with_dispatcher(
        &self,
        request: IndexRequest,
        caller: Arc<dyn Send + Sync>,
        dispatcher: Arc<dyn IndexDispatcher>,
    ) -> Result<IndexOutcome, ImportError> {
        if self.inner.stopping.load(Ordering::Acquire) {
            return Err(ImportError::Stopping);
        }
        let permit = self
            .inner
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| ImportError::Busy)?;
        let job = Arc::new(Job {
            id: self.inner.next.fetch_add(1, Ordering::Relaxed),
            inner: Arc::downgrade(&self.inner),
            cancelled: request.cancelled.clone(),
            caller: Some(caller),
            permit: Some(permit),
            memory: Mutex::new(None),
        });
        {
            let mut jobs = self.inner.jobs.lock().unwrap();
            if self.inner.stopping.load(Ordering::Acquire) {
                drop(jobs);
                return Err(ImportError::Stopping);
            }
            jobs.insert(job.id, Arc::downgrade(&job));
        }
        let mut cancel = Cancel {
            flag: request.cancelled.clone(),
            armed: true,
        };
        let inner = self.inner.clone();
        let result = tokio::spawn(async move { inner.build_owned(request, job, dispatcher).await })
            .await
            .map_err(|_| ImportError::Worker)?;
        cancel.armed = false;
        result
    }
    /// Additive component path. Preserve typed stage/commit/release evidence;
    /// operational errors and ambiguous physical replies are not task exceptions.
    pub async fn build_report_with_dispatcher(
        &self,
        request: IndexRequest,
        caller: Arc<dyn Send + Sync>,
        dispatcher: Arc<dyn IndexDispatcher>,
    ) -> Result<IndexExecutionReport, ImportError> {
        if self.inner.stopping.load(Ordering::Acquire) {
            return Err(ImportError::Stopping);
        }
        let permit = self
            .inner
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| ImportError::Busy)?;
        let job = Arc::new(Job {
            id: self.inner.next.fetch_add(1, Ordering::Relaxed),
            inner: Arc::downgrade(&self.inner),
            cancelled: request.cancelled.clone(),
            caller: Some(caller),
            permit: Some(permit),
            memory: Mutex::new(None),
        });
        {
            let mut jobs = self.inner.jobs.lock().unwrap();
            if self.inner.stopping.load(Ordering::Acquire) {
                drop(jobs);
                return Err(ImportError::Stopping);
            }
            jobs.insert(job.id, Arc::downgrade(&job));
        }
        let mut cancel = Cancel {
            flag: request.cancelled.clone(),
            armed: true,
        };
        let inner = self.inner.clone();
        let result =
            tokio::spawn(async move { inner.build_report_owned(request, job, dispatcher).await })
                .await
                .map_err(|_| ImportError::Worker)?;
        cancel.armed = false;
        Ok(result)
    }
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.inner.stopping.store(true, Ordering::Release);
        let jobs = self
            .inner
            .jobs
            .lock()
            .unwrap()
            .values()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        for job in &jobs {
            job.cancelled.store(true, Ordering::Release);
        }
        drop(jobs);
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.physical_jobs() == 0 {
                return true;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return self.physical_jobs() == 0;
            }
        }
    }
}
impl Inner {
    async fn build_owned(
        &self,
        request: IndexRequest,
        job: Arc<Job>,
        dispatcher: Arc<dyn IndexDispatcher>,
    ) -> Result<IndexOutcome, ImportError> {
        let lease = match self
            .services
            .locks
            .acquire("epg_source_file", request.source_id)
            .await
        {
            Ok(lease) => lease,
            Err(TaskLockError::Held) => {
                if request.retry >= 480 {
                    return Ok(IndexOutcome::Exhausted);
                }
                dispatcher
                    .defer(
                        request.source_id,
                        request.retry + 1,
                        5,
                        SwapPolicyContext {
                            cancelled: job.cancelled.clone(),
                            retained: job,
                        },
                    )
                    .await?;
                return Ok(IndexOutcome::Deferred);
            }
            Err(_) => return Err(ImportError::Worker),
        };
        let watch = Watch::new(&lease, job.cancelled.clone());
        let (lease, released) = RetainedLease::new(lease, job.clone());
        let context = SwapPolicyContext {
            cancelled: job.cancelled.clone(),
            retained: Arc::new(Context {
                _watch: watch,
                _lease: lease,
            }),
        };
        let result=async{
            let acquire=self.memory.clone().acquire_many_owned(self.reservation as u32);tokio::pin!(acquire);
            let permit=loop{context.ensure_active()?;tokio::select!{result=&mut acquire=>break result.map_err(|_|ImportError::Stopping)?,_=tokio::time::sleep(Duration::from_millis(10))=>{}}};
            *job.memory.lock().unwrap()=Some(permit);
            self.scan_and_store(request.source_id,context.clone(),None).await
        }.await;
        drop(context);
        if !released.wait(self.config.release_timeout).await {
            return Err(ImportError::Worker);
        }
        result
    }
    async fn build_report_owned(
        &self,
        request: IndexRequest,
        job: Arc<Job>,
        dispatcher: Arc<dyn IndexDispatcher>,
    ) -> IndexExecutionReport {
        let trace = Arc::new(ExecutionTrace::new());
        let lease = match self
            .services
            .locks
            .acquire_retained_detailed("epg_source_file", request.source_id, job.clone())
            .await
        {
            Ok(lease) => lease,
            Err(TaskLockDetailedError::Operational(TaskLockError::Held)) => {
                if request.retry >= 480 {
                    return trace.report(
                        Ok(IndexOutcome::Exhausted),
                        IndexReleaseObservation::NotAcquired,
                    );
                }
                stage(&Some(trace.clone()), IndexStage::ChildPublication);
                let result = dispatcher
                    .defer(
                        request.source_id,
                        request.retry + 1,
                        5,
                        SwapPolicyContext {
                            cancelled: job.cancelled.clone(),
                            retained: job,
                        },
                    )
                    .await;
                return trace.report(
                    result.map(|()| IndexOutcome::Deferred),
                    IndexReleaseObservation::NotAcquired,
                );
            }
            Err(error) => {
                let release = match &error {
                    TaskLockDetailedError::Acquisition { cleanup, .. } => {
                        IndexReleaseObservation::AcquisitionCleanup(cleanup.clone())
                    }
                    _ => IndexReleaseObservation::NotAcquired,
                };
                return IndexExecutionReport {
                    operation: Err(IndexStageFailure {
                        stage: IndexStage::AcquireFileLock,
                        cause: IndexFailureCause::Lock(error),
                    }),
                    commit: IndexCommitObservation::NotWritten,
                    release,
                };
            }
        };
        let watch = Watch::new(&lease, job.cancelled.clone());
        let (lease, released) = RetainedLease::new(lease, job.clone());
        let context = SwapPolicyContext {
            cancelled: job.cancelled.clone(),
            retained: Arc::new(Context {
                _watch: watch,
                _lease: lease,
            }),
        };
        stage(&Some(trace.clone()), IndexStage::MemoryAdmission);
        let operation = async {
            let acquire = self.memory.clone().acquire_many_owned(self.reservation as u32); tokio::pin!(acquire);
            let permit = loop { context.ensure_active()?; tokio::select! { result = &mut acquire => break result.map_err(|_| ImportError::Stopping)?, _ = tokio::time::sleep(Duration::from_millis(10)) => {} } };
            *job.memory.lock().unwrap() = Some(permit);
            self.scan_and_store(request.source_id, context.clone(), Some(trace.clone())).await
        }.await;
        drop(context);
        let release = match released.wait_detailed(self.config.release_timeout).await {
            ReleaseWait::Complete(ReleaseOutcome::Released) => IndexReleaseObservation::Released,
            ReleaseWait::Complete(ReleaseOutcome::NotOwner) => IndexReleaseObservation::NotOwner,
            ReleaseWait::Complete(ReleaseOutcome::Failed(error)) => {
                IndexReleaseObservation::Failed(error)
            }
            ReleaseWait::Complete(ReleaseOutcome::RuntimeUnavailable) => {
                IndexReleaseObservation::RuntimeUnavailable
            }
            ReleaseWait::TimedOut => IndexReleaseObservation::TimedOut,
        };
        trace.report(operation, release)
    }
    async fn scan_and_store(
        &self,
        id: i64,
        context: SwapPolicyContext,
        trace: Option<Arc<ExecutionTrace>>,
    ) -> Result<IndexOutcome, ImportError> {
        context.ensure_active()?;
        stage(&trace, IndexStage::SourceRead);
        // Like original .get(), this neither loads the old multi-MiB index nor filters
        // inactive/dummy source flags. The task owns its file lease before this read.
        let row =
            sqlx::query("SELECT file_path,extracted_file_path FROM epg_epgsource WHERE id=$1")
                .bind(id)
                .fetch_optional(self.services.store.pool())
                .await?;
        let Some(row) = row else {
            return Ok(IndexOutcome::MissingSource);
        };
        let file: Option<String> = row.get("file_path");
        let extracted: Option<String> = row.get("extracted_file_path");
        let selected = extracted
            .filter(|path| !path.is_empty())
            .or_else(|| file.filter(|path| !path.is_empty()));
        let make_cache = selected.is_none();
        let cache = self.config.path_context.as_ref().map_or_else(
            || self.config.cache_dir.clone(),
            |context| context.media_root().join("cached_epg"),
        );
        let selected = selected
            .map(PathBuf::from)
            .unwrap_or_else(|| cache.join(format!("{id}.tmp")));
        let path = self.config.path_context.as_ref().map_or_else(
            || selected.clone(),
            |context| context.resolve(selected.clone()),
        );
        let limits = self.config.limits;
        let worker = context.clone();
        let json_bytes = self.config.max_index_json_bytes;
        let file_trace = trace.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _guard = worker.retained.clone();
            worker.ensure_active()?;
            if make_cache {
                stage(&file_trace, IndexStage::CacheDirectory);
                std::fs::create_dir_all(cache)?;
            }
            stage(&file_trace, IndexStage::FileMetadata);
            let metadata = match std::fs::metadata(&path) {
                Ok(value) => value,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            if !metadata.is_file() {
                if file_trace.is_some() && metadata.is_dir() {
                    stage(&file_trace, IndexStage::FileOpen);
                    // Pinned Linux Source136 raises EISDIR21 at Python open.
                    #[cfg(target_os = "linux")]
                    return Err(std::io::Error::from_raw_os_error(21).into());
                }
                stage(&file_trace, IndexStage::UnsupportedFileKind);
                return Err(ImportError::Capacity("programme-index regular file"));
            }
            stage(&file_trace, IndexStage::FileOpen);
            let file = File::open(path)?;
            stage(&file_trace, IndexStage::Scan);
            let index = scan_file(file, limits, &worker.cancelled)?;
            worker.ensure_active()?;
            stage(&file_trace, IndexStage::IndexEncoding);
            if !index.fits_json_bytes(json_bytes) {
                return Err(ImportError::Capacity("programme-index JSON"));
            }
            Ok(Some(index.source_value()))
        })
        .await
        .map_err(|_| ImportError::Worker)??;
        let Some(data) = result else {
            return Ok(IndexOutcome::MissingFile);
        };
        context.ensure_active()?;
        // A private writer drops its potentially large driver buffers when this
        // operation completes, rather than retaining them in the shared pool.
        stage(&trace, IndexStage::WriterAdmission);
        let mut connection = self.services.store.pool().acquire().await?.detach();
        let result=async{
            let mut tx=connection.begin().await?;if let Err(error)=context.ensure_active(){tx.rollback().await?;return Err(error);}
            // Original update_or_create obtains its row lock before save's
            // auto_now clock sample. Do not sample while waiting for that lock.
            stage(&trace,IndexStage::RowLock);
            let locked=sqlx::query("SELECT source_id FROM epg_epgsourceindex WHERE source_id=$1 FOR UPDATE").bind(id).fetch_optional(&mut *tx).await;
            if let Err(error)=locked{let _=tx.rollback().await;return Err(ImportError::from(error));}
            if let Err(error)=context.ensure_active(){tx.rollback().await?;return Err(error);}
            let now=(self.services.clock)();
            stage(&trace,IndexStage::Write);
            let saved=sqlx::query("INSERT INTO epg_epgsourceindex(source_id,data,updated_at) VALUES($1,$2,$3) ON CONFLICT(source_id) DO UPDATE SET data=EXCLUDED.data,updated_at=EXCLUDED.updated_at").bind(id).bind(sqlx::types::Json(data)).bind(now).execute(&mut *tx).await;
            if let Err(error)=saved{let _=tx.rollback().await;return Err(ImportError::from(error));}
            if let Err(error)=context.ensure_active(){tx.rollback().await?;return Err(error);}
            stage(&trace,IndexStage::Commit);
            if let Some(trace)=&trace { trace.commit.store(1,Ordering::Release); }
            tx.commit().await?;
            if let Some(trace)=&trace { trace.commit.store(2,Ordering::Release); }
            Ok(IndexOutcome::Written)
        }.await;
        let closed = connection.close().await;
        if let Err(error) = closed {
            stage(&trace, IndexStage::ConnectionClose);
            return Err(error.into());
        }
        result
    }
}
