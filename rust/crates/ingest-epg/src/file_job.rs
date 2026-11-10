//! Bounded local-file refresh orchestration, ported from original Dispatcharr.
//! Original authors retain full application/algorithm credit. App dispatch,
//! remote downloads, compressed-file promotion and actual child jobs are gated
//! independently; an unsupported source is never reported as imported.
use crate::{
    cache_policy::{CachedSwapPolicy, PolicyClock},
    channels::{ChannelImporter, ChannelRequest},
    file_lease::RetainedLease,
    orphan_cleanup::{CleanupConfig, OrphanCleanup},
    reporter::{
        guarded_pg_error, FailedImport, GuardedSqlDiagnostic, GuideReporter, ReportAction,
        ReportObservation,
    },
    EpgImporter, ImportError, ImportReporting, ImportRequest, SwapPolicyContext,
    SwapPolicyProvider,
};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_coordination::{TaskLease, TaskLockError, TaskLocks};
use dispatcharr_ingest_core::{
    extract_xmltv_owned, source_integer_literal, xmltv_file_handle_is_complete, UploadLimits,
};
use dispatcharr_output_store::transport::{RedisTransport, TransportConfig};
use dispatcharr_store::Store;
use sqlx::Row;
use std::{
    collections::HashMap,
    fs::File,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};
use tokio::sync::{watch, Notify, OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Debug)]
pub enum ChildTask {
    RefreshDeferred {
        source_id: i64,
        force: bool,
        retry: u32,
        delay_seconds: u32,
    },
    ProgrammeIndex {
        source_id: i64,
    },
    SeriesRules,
    LateProgrammes {
        source_id: i64,
        initial_epg_ids: Vec<i64>,
    },
}
/// Implementations must retain `context.retained` through their actual queue
/// publication reply. A logical timeout must not release source/file leases.
pub trait GuideTaskDispatcher: Send + Sync {
    fn dispatch<'a>(
        &'a self,
        task: ChildTask,
        context: SwapPolicyContext,
    ) -> BoxFuture<'a, Result<(), ImportError>>;
}
#[derive(Clone)]
pub struct FileJobServices {
    pub store: Store,
    pub locks: TaskLocks,
    pub reporter: GuideReporter,
    pub channels: ChannelImporter,
    pub programmes: EpgImporter,
    pub policy: Arc<CachedSwapPolicy>,
    pub dispatcher: Arc<dyn GuideTaskDispatcher>,
    pub clock: PolicyClock,
}
#[derive(Clone, Debug)]
pub struct FileJobConfig {
    pub max_jobs: usize,
    /// Prepared cache root; local ordinary files do not create paths here.
    pub cache_dir: PathBuf,
    pub release_timeout: Duration,
}
pub struct FileJobRequest {
    pub source_id: i64,
    pub force: bool,
    pub defer_retry: u32,
    pub cancelled: Arc<AtomicBool>,
}
#[derive(Debug, PartialEq, Eq)]
pub enum FileJobOutcome {
    Imported,
    Skipped,
    Deferred,
    AlreadyRunning,
    MissingSource(String),
    Failed,
    Unsupported(&'static str),
}
/// A reason known from the exact busy/exhaustion branches. This is not a
/// classification of the coarse Failed outcome or of channel/SQL errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExhaustionReason {
    FileBusy,
    ParserBusy,
}
impl ExhaustionReason {
    fn source_reason(self) -> &'static str {
        match self {
            Self::FileBusy => "EPG file in use (index build)",
            Self::ParserBusy => "per-channel program parses in progress",
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExhaustionReporting {
    StatusSaved,
    CaughtGuardedStatusSql(GuardedSqlDiagnostic),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExhaustionTerminal {
    Verified,
    CaughtGuardedSelectSql(GuardedSqlDiagnostic),
    CaughtGuardedStatusSql(GuardedSqlDiagnostic),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettledExhaustion {
    pub reason: ExhaustionReason,
    pub reporting: ExhaustionReporting,
    pub terminal: ExhaustionTerminal,
}
#[derive(Debug, Eq, PartialEq)]
pub struct FileJobReport {
    pub outcome: FileJobOutcome,
    /// Returned only after physical reporter, terminal cleanup and refresh
    /// Released have settled successfully. Each run owns its local reason.
    pub settled_exhaustion: Option<SettledExhaustion>,
}
#[derive(Clone)]
pub struct FileJobRunner {
    inner: Arc<Inner>,
}
struct Inner {
    services: FileJobServices,
    config: FileJobConfig,
    transport: RedisTransport,
    orphans: OrphanCleanup,
    admission: Arc<Semaphore>,
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
}
impl Drop for Job {
    fn drop(&mut self) {
        drop(self.caller.take());
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
struct LossWatch(watch::Sender<bool>);
impl LossWatch {
    fn new(lease: &TaskLease, cancelled: Arc<AtomicBool>) -> Self {
        let mut lost = lease.loss_signal();
        let (tx, mut done) = watch::channel(false);
        tokio::spawn(async move {
            loop {
                if *lost.borrow() {
                    cancelled.store(true, Ordering::Release);
                    return;
                }
                tokio::select! {_=done.changed()=>return,result=lost.changed()=>if result.is_err(){return;}}
            }
        });
        Self(tx)
    }
}
impl Drop for LossWatch {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}
struct LeaseContext {
    _watch: LossWatch,
    _guard: Arc<dyn Send + Sync>,
}
struct Source {
    id: i64,
    name: String,
    kind: String,
    active: bool,
    url: Option<String>,
    path: Option<String>,
    extracted: Option<String>,
    properties: serde_json::Value,
}
impl FileJobRunner {
    pub async fn connect(
        services: FileJobServices,
        config: FileJobConfig,
        redis: &str,
    ) -> Result<Self, ImportError> {
        if !(1..=4).contains(&config.max_jobs)
            || config.cache_dir.as_os_str().len() > 4096
            || !config.cache_dir.is_absolute()
            || config.release_timeout < Duration::from_millis(1)
            || config.release_timeout > Duration::from_secs(60)
        {
            return Err(ImportError::Capacity("file job configuration"));
        }
        let transport = RedisTransport::connect(
            redis,
            TransportConfig {
                max_operations: 2,
                max_queued_operations: 4,
                command_timeout: Duration::from_secs(2),
            },
        )
        .await
        .map_err(|_| ImportError::Worker)?;
        let orphans = OrphanCleanup::new(
            services.store.pool().clone(),
            services.clock.clone(),
            CleanupConfig {
                max_jobs: config.max_jobs,
                ..CleanupConfig::default()
            },
        )
        .map_err(|_| ImportError::Worker)?;
        Ok(Self {
            inner: Arc::new(Inner {
                admission: Arc::new(Semaphore::new(config.max_jobs)),
                services,
                config,
                transport,
                orphans,
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
    /// The owned supervisor survives waiter cancellation and retains admission
    /// through child parser/SQL/publication and physical owner-checked release.
    pub async fn run(
        &self,
        request: FileJobRequest,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<FileJobOutcome, ImportError> {
        self.run_with_dispatcher(request, caller, self.inner.services.dispatcher.clone())
            .await
    }
    /// Per-execution immutable publication context. Concurrent executions never
    /// mutate the configured default dispatcher or another parent's lineage.
    /// The owned supervisor retains this Arc through physical child replies.
    pub async fn run_with_dispatcher(
        &self,
        request: FileJobRequest,
        caller: Arc<dyn Send + Sync>,
        dispatcher: Arc<dyn GuideTaskDispatcher>,
    ) -> Result<FileJobOutcome, ImportError> {
        self.run_report_with_dispatcher(request, caller, dispatcher)
            .await
            .map(|report| report.outcome)
    }
    /// Additive detailed result. Old run APIs preserve their exact outcome.
    /// No reason survives an operational/report/release failure.
    pub async fn run_report_with_dispatcher(
        &self,
        request: FileJobRequest,
        caller: Arc<dyn Send + Sync>,
        dispatcher: Arc<dyn GuideTaskDispatcher>,
    ) -> Result<FileJobReport, ImportError> {
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
        let result = tokio::spawn(async move {
            let mut settled_exhaustion = None;
            let outcome = inner
                .run_owned(request, job, dispatcher, &mut settled_exhaustion)
                .await?;
            Ok::<_, ImportError>(FileJobReport {
                outcome,
                settled_exhaustion,
            })
        })
        .await
        .map_err(|_| ImportError::Worker)?;
        cancel.armed = false;
        result
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
                return self
                    .inner
                    .orphans
                    .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                    .await;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return self.physical_jobs() == 0
                    && self.inner.orphans.shutdown(Duration::ZERO).await;
            }
        }
    }
}
impl Inner {
    async fn run_owned(
        &self,
        request: FileJobRequest,
        job: Arc<Job>,
        dispatcher: Arc<dyn GuideTaskDispatcher>,
        settled_exhaustion: &mut Option<SettledExhaustion>,
    ) -> Result<FileJobOutcome, ImportError> {
        let lease = match self
            .services
            .locks
            .acquire("refresh_epg_data", request.source_id)
            .await
        {
            Ok(v) => v,
            Err(TaskLockError::Held) => return Ok(FileJobOutcome::AlreadyRunning),
            Err(_) => return Err(ImportError::Worker),
        };
        let watch = LossWatch::new(&lease, job.cancelled.clone());
        let (guard, released) = RetainedLease::new(lease, job.clone());
        let context = SwapPolicyContext {
            cancelled: job.cancelled.clone(),
            retained: Arc::new(LeaseContext {
                _watch: watch,
                _guard: guard,
            }),
        };
        let result = self
            .refresh(
                &request,
                context.clone(),
                dispatcher.as_ref(),
                settled_exhaustion,
            )
            .await;
        // Source performs terminal-state cleanup while refresh lease is held.
        // Cancellation does not suppress this final best-effort error effect.
        let final_context = SwapPolicyContext {
            cancelled: Arc::new(AtomicBool::new(false)),
            retained: context.retained.clone(),
        };
        let terminal = self.terminal(request.source_id, final_context).await;
        drop(context);
        if !released.wait(self.config.release_timeout).await {
            return Err(ImportError::Worker);
        }
        let classified =
            settled_exhaustion.is_some() && matches!(&result, Ok(FileJobOutcome::Failed));
        match terminal {
            Ok(TerminalObservation::Verified) => (),
            Ok(TerminalObservation::StatusFailure(ReportObservation::FailureStatusSqlCaught(
                cause,
            ))) if classified => {
                let Some(diagnostic) = guarded_pg_error(&cause) else {
                    return Err(ImportError::Worker);
                };
                settled_exhaustion.as_mut().unwrap().terminal =
                    ExhaustionTerminal::CaughtGuardedStatusSql(diagnostic);
            }
            Ok(TerminalObservation::StatusFailure(_)) if classified => {
                return Err(ImportError::Worker)
            }
            // Preserve the existing execute() guarded-error behavior for
            // nonclassified runs; no new normal Failed authority is granted.
            Ok(TerminalObservation::StatusFailure(_)) => (),
            Err(TerminalFailure::Read(cause)) if classified => {
                let Some(diagnostic) = guarded_pg_error(&cause) else {
                    return Err(cause.into());
                };
                settled_exhaustion.as_mut().unwrap().terminal =
                    ExhaustionTerminal::CaughtGuardedSelectSql(diagnostic);
            }
            Err(failure) => return Err(failure.into_import()),
        }
        result
    }
    async fn source(&self, id: i64) -> Result<Option<Source>, ImportError> {
        // Bound driver-owned variable strings before decoding; oversized valid
        // configurations are explicit unsupported capacity, never truncated.
        let row=sqlx::query("SELECT id::bigint AS id,is_active,CASE WHEN octet_length(name)<=65536 THEN name END AS name,source_type,CASE WHEN octet_length(url)<=65536 THEN url END AS url,file_path,extracted_file_path,CASE WHEN octet_length(custom_properties::text)<=65536 THEN custom_properties END AS properties,octet_length(name)>65536 OR octet_length(url)>65536 OR octet_length(custom_properties::text)>65536 AS oversized FROM epg_epgsource WHERE id=$1").bind(id).fetch_optional(self.services.store.pool()).await?;
        row.map(|row| {
            if row.get::<Option<bool>, _>("oversized").unwrap_or(false) {
                return Err(ImportError::Capacity("source settings"));
            }
            Ok(Source {
                id: row.get("id"),
                name: row.get("name"),
                kind: row.get("source_type"),
                active: row.get("is_active"),
                url: row.get("url"),
                path: row.get("file_path"),
                extracted: row.get("extracted_file_path"),
                properties: row
                    .get::<Option<serde_json::Value>, _>("properties")
                    .unwrap_or(serde_json::Value::Null),
            })
        })
        .transpose()
    }
    async fn refresh(
        &self,
        request: &FileJobRequest,
        context: SwapPolicyContext,
        dispatcher: &dyn GuideTaskDispatcher,
        settled_exhaustion: &mut Option<SettledExhaustion>,
    ) -> Result<FileJobOutcome, ImportError> {
        context.ensure_active()?;
        let Some(source) = self.source(request.source_id).await? else {
            // Source ignores the helper's caught failure boolean and returns
            // its cleanup string, including definite partial commits. Unknown
            // or ambiguous native SQL remains operational, never fake success.
            let _report = self
                .orphans
                .cleanup(request.source_id, context.retained.clone())
                .await
                .map_err(|_| ImportError::Worker)?;
            return Ok(FileJobOutcome::MissingSource(format!(
                "EPG source {} does not exist, task cleaned up",
                request.source_id
            )));
        };
        if !source.active || source.kind == "dummy" {
            return Ok(FileJobOutcome::Skipped);
        }
        if source.kind != "xmltv" {
            return Ok(FileJobOutcome::Unsupported("non-XMLTV source"));
        }
        let now = (self.services.clock)();
        sqlx::query("INSERT INTO epg_epgsourceindex(source_id,data,updated_at) VALUES($1,NULL,$2) ON CONFLICT(source_id) DO UPDATE SET data=NULL,updated_at=EXCLUDED.updated_at").bind(source.id).bind(now).execute(self.services.store.pool()).await?;
        context.ensure_active()?;
        let (counter,_): (Option<String>,_)=self.transport.command("if redis.call('STRLEN',KEYS[1])>4096 then return redis.error_reply('counter capacity') end return redis.call('GET',KEYS[1])",&[format!("epg_source_tvg_parse_{}",source.id)],&[],context.retained.clone()).await.map_err(|_|ImportError::Worker)?;
        if let Some(value) = counter {
            let Some(number) = source_integer_literal(&value) else {
                let error = format!("invalid literal for int() with base 10: '{value}'");
                self.failure(
                    &source,
                    format!("Error refreshing EPG data: {error}"),
                    error,
                    false,
                    context,
                )
                .await?;
                return Ok(FileJobOutcome::Failed);
            };
            if !number.starts_with('-') && number != "0" {
                return self
                    .defer(
                        &source,
                        request,
                        ExhaustionReason::ParserBusy,
                        context,
                        dispatcher,
                        settled_exhaustion,
                    )
                    .await;
            }
        }
        let lease = match self
            .services
            .locks
            .acquire("epg_source_file", source.id)
            .await
        {
            Ok(v) => v,
            Err(TaskLockError::Held) => {
                return self
                    .defer(
                        &source,
                        request,
                        ExhaustionReason::FileBusy,
                        context,
                        dispatcher,
                        settled_exhaustion,
                    )
                    .await
            }
            Err(_) => return Err(ImportError::Worker),
        };
        let watch = LossWatch::new(&lease, context.cancelled.clone());
        let (file_guard, released) = RetainedLease::new(lease, context.retained.clone());
        let file_context = SwapPolicyContext {
            cancelled: context.cancelled.clone(),
            retained: Arc::new(LeaseContext {
                _watch: watch,
                _guard: file_guard,
            }),
        };
        let result = self
            .local_file(&source, file_context.clone(), dispatcher)
            .await;
        drop(file_context);
        if !released.wait(self.config.release_timeout).await {
            return Err(ImportError::Worker);
        }
        match result? {
            FileJobOutcome::Imported => {}
            other => return Ok(other),
        }
        context.ensure_active()?;
        dispatcher
            .dispatch(
                ChildTask::ProgrammeIndex {
                    source_id: source.id,
                },
                context.clone(),
            )
            .await?;
        sqlx::query("UPDATE epg_epgsource SET updated_at=$2 WHERE id=$1")
            .bind(source.id)
            .bind((self.services.clock)())
            .execute(self.services.store.pool())
            .await?;
        let _ = dispatcher.dispatch(ChildTask::SeriesRules, context).await;
        Ok(FileJobOutcome::Imported)
    }
    async fn local_file(
        &self,
        source: &Source,
        context: SwapPolicyContext,
        dispatcher: &dyn GuideTaskDispatcher,
    ) -> Result<FileJobOutcome, ImportError> {
        if source.url.as_deref().is_some_and(|v| !v.is_empty()) {
            return Ok(FileJobOutcome::Unsupported("remote XMLTV download"));
        }
        let path = source
            .path
            .as_ref()
            .filter(|v| !v.is_empty())
            .map(PathBuf::from);
        let mut check_path = source
            .extracted
            .as_ref()
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .or_else(|| path.clone());
        let paths = path.clone();
        let checks = check_path.clone();
        let retained = context.retained.clone();
        let cancelled = context.cancelled.clone();
        let (exists, mut incomplete) = tokio::task::spawn_blocking(move || {
            let _retained = retained;
            if cancelled.load(Ordering::Acquire) {
                return Err(ImportError::Cancelled);
            }
            let exists = paths.as_ref().is_some_and(|v| v.exists());
            let incomplete = checks.as_ref().is_some_and(|p| {
                p.as_os_str().to_string_lossy().ends_with(".xml")
                    && p.exists()
                    && File::open(p)
                        .ok()
                        .is_none_or(|mut f| !xmltv_file_handle_is_complete(&mut f))
            });
            Ok((exists, incomplete))
        })
        .await
        .map_err(|_| ImportError::Worker)??;
        context.ensure_active()?;
        if !exists {
            let message = "No URL provided and no valid local file exists".to_string();
            self.failure(source, message.clone(), message, true, context)
                .await?;
            return Ok(FileJobOutcome::Failed);
        }
        let path = path.unwrap();
        if [".gz", ".zip", ".xz"]
            .iter()
            .any(|suffix| path.as_os_str().to_string_lossy().ends_with(suffix))
        {
            let destination = self.config.cache_dir.join(format!("{}.xml", source.id));
            let incoming = self.config.cache_dir.join(format!(
                ".incoming-{}-{}-{}.xml",
                std::process::id(),
                source.id,
                self.next.fetch_add(1, Ordering::Relaxed)
            ));
            let input = path.clone();
            let target = destination.clone();
            let retained = context.retained.clone();
            let cancel = context.cancelled.clone();
            let promoted = tokio::task::spawn_blocking(move || {
                let _retained = retained;
                if cancel.load(Ordering::Acquire) {
                    return Err(ImportError::Cancelled);
                }
                std::fs::create_dir_all(target.parent().unwrap())?;
                // Unique incoming output belongs to this job. Never pass the
                // previous good guide to the destructive low-level extractor.
                let owned = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&incoming)?;
                drop(owned);
                let extracted =
                    extract_xmltv_owned(&input, &incoming, false, UploadLimits::default(), &cancel);
                let complete = extracted.is_ok()
                    && File::open(&incoming)
                        .ok()
                        .is_some_and(|mut file| xmltv_file_handle_is_complete(&mut file));
                if cancel.load(Ordering::Acquire) {
                    let _ = std::fs::remove_file(&incoming);
                    return Err(ImportError::Cancelled);
                }
                if !complete {
                    let _ = std::fs::remove_file(&incoming);
                    return Ok(false);
                }
                if let Err(error) = std::fs::rename(&incoming, &target) {
                    let _ = std::fs::remove_file(&incoming);
                    return Err(error.into());
                }
                Ok(true)
            })
            .await
            .map_err(|_| ImportError::Worker)??;
            if !promoted {
                return Ok(FileJobOutcome::Unsupported(
                    "invalid/oversized compressed guide promotion",
                ));
            }
            context.ensure_active()?;
            sqlx::query("UPDATE epg_epgsource SET extracted_file_path=$2 WHERE id=$1")
                .bind(source.id)
                .bind(
                    destination
                        .to_str()
                        .ok_or(ImportError::Capacity("cache path"))?,
                )
                .execute(self.services.store.pool())
                .await?;
            check_path = Some(destination);
            incomplete = false;
        }
        if incomplete {
            let message="EPG file appears incomplete (missing closing </tv>). Keeping previous data when available; will retry on next refresh.".to_string();
            self.failure(source, message.clone(), message, true, context)
                .await?;
            return Ok(FileJobOutcome::Failed);
        }
        self.services
            .reporter
            .execute(
                ReportAction::LocalDownloadComplete {
                    source_id: source.id,
                },
                context.clone(),
            )
            .await?;
        let path = check_path.unwrap();
        let channel = self
            .services
            .channels
            .import_channels(
                ChannelRequest {
                    source_id: source.id,
                    source_name: source.name.clone(),
                    path: path.clone(),
                    cache_file: self.config.cache_dir.join(format!("{}.tmp", source.id)),
                    source_has_url: false,
                    source_properties: source.properties.clone(),
                    cancelled: context.cancelled.clone(),
                },
                context.retained.clone(),
                Some(self.services.reporter.clone()),
            )
            .await;
        if let Err(error) = channel {
            return match error {
                crate::channels::ChannelError::Import(ImportError::Cancelled) => {
                    Err(ImportError::Cancelled)
                }
                _ => Ok(FileJobOutcome::Failed),
            };
        }
        let result = self
            .services
            .programmes
            .import_file_reported(
                ImportRequest {
                    source_id: source.id,
                    path,
                    now: (self.services.clock)(),
                    policy: dispatcharr_store::source_epg_history_policy(&serde_json::json!({}))
                        .map_err(|_| ImportError::Worker)?,
                    cancelled: context.cancelled.clone(),
                },
                context.retained.clone(),
                Some(self.services.policy.clone() as Arc<dyn SwapPolicyProvider>),
                ImportReporting {
                    reporter: self.services.reporter.clone(),
                    source_name: source.name.clone(),
                    source_has_url: false,
                },
            )
            .await;
        let report = match result {
            Ok(report) => report,
            Err(ImportError::Incomplete) => return Ok(FileJobOutcome::Failed),
            Err(error) => return Err(error),
        };
        if !report.no_mapped_channels {
            dispatcher
                .dispatch(
                    ChildTask::LateProgrammes {
                        source_id: source.id,
                        initial_epg_ids: report.initial_mapped_epg_ids,
                    },
                    context,
                )
                .await?;
        }
        Ok(FileJobOutcome::Imported)
    }
    async fn failure(
        &self,
        source: &Source,
        message: String,
        error: String,
        download: bool,
        context: SwapPolicyContext,
    ) -> Result<(), ImportError> {
        let value = FailedImport {
            source_id: source.id,
            source_name: source.name.clone(),
            last_message: message,
            publication_error: error,
        };
        self.services
            .reporter
            .execute(
                if download {
                    ReportAction::DownloadFailed(value)
                } else {
                    ReportAction::RefreshFailed(value)
                },
                context,
            )
            .await
    }
    async fn defer(
        &self,
        source: &Source,
        request: &FileJobRequest,
        reason: ExhaustionReason,
        context: SwapPolicyContext,
        dispatcher: &dyn GuideTaskDispatcher,
        settled_exhaustion: &mut Option<SettledExhaustion>,
    ) -> Result<FileJobOutcome, ImportError> {
        if request.defer_retry >= 480 {
            let msg = format!(
                "EPG refresh blocked for too long ({}); retry later",
                reason.source_reason()
            );
            let observation = self
                .services
                .reporter
                .execute_observed(
                    ReportAction::RefreshFailed(FailedImport {
                        source_id: source.id,
                        source_name: source.name.clone(),
                        last_message: msg.clone(),
                        publication_error: msg,
                    }),
                    context,
                )
                .await?;
            // Source29 proves normal null even when the guarded UPDATE raises
            // P0001. Other discarded/unknown causes are NOT inferred normal.
            let reporting = match observation {
                ReportObservation::FailureStatusSaved => Some(ExhaustionReporting::StatusSaved),
                ReportObservation::FailureStatusSqlCaught(cause) => {
                    guarded_pg_error(&cause).map(ExhaustionReporting::CaughtGuardedStatusSql)
                }
                _ => None,
            };
            *settled_exhaustion = reporting.map(|reporting| SettledExhaustion {
                reason,
                reporting,
                terminal: ExhaustionTerminal::Verified,
            });
            return Ok(FileJobOutcome::Failed);
        }
        dispatcher
            .dispatch(
                ChildTask::RefreshDeferred {
                    source_id: source.id,
                    force: request.force,
                    retry: request.defer_retry + 1,
                    delay_seconds: 15,
                },
                context,
            )
            .await?;
        Ok(FileJobOutcome::Deferred)
    }
    async fn terminal(
        &self,
        id: i64,
        context: SwapPolicyContext,
    ) -> Result<TerminalObservation, TerminalFailure> {
        // Source reads status/name inside its guard, then checks status. Keep
        // pool acquisition separate: a startup/reset/capacity failure is not an
        // observed server exception from this guarded SELECT.
        let mut connection = self
            .services
            .store
            .pool()
            .acquire()
            .await
            .map_err(TerminalFailure::Acquire)?;
        let row=match sqlx::query("SELECT status,CASE WHEN octet_length(name)<=65536 THEN name END AS name,octet_length(name)>65536 AS oversized FROM epg_epgsource WHERE id=$1")
            .bind(id).fetch_optional(&mut *connection).await {
                Ok(row)=>row,
                Err(cause)=>{
                    if connection.close().await.is_err(){return Err(TerminalFailure::Other(ImportError::Worker));}
                    return Err(TerminalFailure::Read(cause));
                }
            };
        drop(connection);
        if let Some(row) = row {
            let status: String = row.get("status");
            if !matches!(status.as_str(), "fetching" | "parsing") {
                return Ok(TerminalObservation::Verified);
            }
            if row.get::<bool, _>("oversized") {
                return Err(TerminalFailure::Other(ImportError::Capacity(
                    "terminal source name",
                )));
            }
            let msg = "Refresh did not complete successfully".to_string();
            let observation = self
                .services
                .reporter
                .execute_observed(
                    ReportAction::RefreshFailed(FailedImport {
                        source_id: id,
                        source_name: row.get("name"),
                        last_message: msg.clone(),
                        publication_error: msg,
                    }),
                    context,
                )
                .await
                .map_err(TerminalFailure::Other)?;
            if !matches!(observation, ReportObservation::FailureStatusSaved) {
                return Ok(TerminalObservation::StatusFailure(observation));
            }
        }
        Ok(TerminalObservation::Verified)
    }
}

enum TerminalObservation {
    Verified,
    StatusFailure(ReportObservation),
}

// A guarded server read is distinct from native pool acquisition and reporting.
enum TerminalFailure {
    Acquire(sqlx::Error),
    Read(sqlx::Error),
    Other(ImportError),
}
impl TerminalFailure {
    fn into_import(self) -> ImportError {
        match self {
            Self::Acquire(cause) | Self::Read(cause) => cause.into(),
            Self::Other(cause) => cause,
        }
    }
}
