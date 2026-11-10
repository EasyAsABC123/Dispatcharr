//! Bounded XMLTV file-to-PostgreSQL orchestration using original Dispatcharr
//! projection and staged-swap algorithms. Original authors retain full credit.
//! Application scheduling/status/cache/event wiring remains a separate gate.
pub mod auto_logos;
pub mod cache_policy;
pub mod channel_core;
mod channel_sql;
pub mod channels;
pub mod current_file;
pub mod current_lookup;
pub mod file_job;
mod file_lease;
pub mod index_service;
pub mod programme_index;
pub mod reporter;
use chrono::{DateTime, Utc};
use dispatcharr_ingest_core::{
    project_xmltv_programme, xmltv_file_handle_is_complete, XmltvProgramme, XmltvReader,
};
use dispatcharr_store::{EpgHistoryPolicy, GuideSwapCounts, Store, StoreError};
use dispatcharr_xmltv_parser::{parse_xmltv_reader, XmltvError, XmltvStats};
use std::{
    collections::{HashMap, HashSet},
    fs::{File, Metadata},
    future::Future,
    io,
    mem::size_of,
    os::unix::fs::MetadataExt,
    path::PathBuf,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};
use tokio::sync::{mpsc, Notify, OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Debug)]
pub struct ImportConfig {
    pub max_jobs: usize,
    pub queue_batches: usize,
    pub batch_rows: usize,
    pub batch_bytes: usize,
    pub max_file_bytes: u64,
    pub max_programmes: u64,
    pub insert_batch_rows: usize,
}
impl Default for ImportConfig {
    fn default() -> Self {
        Self {
            max_jobs: 2,
            queue_batches: 2,
            batch_rows: 1000,
            batch_bytes: 4 * 1024 * 1024,
            max_file_bytes: 4 * 1024 * 1024 * 1024 - 65536,
            max_programmes: 5_000_000,
            insert_batch_rows: 2500,
        }
    }
}
impl ImportConfig {
    fn validate(&self) -> Result<(), ImportError> {
        if !(1..=4).contains(&self.max_jobs)
            || !(1..=4).contains(&self.queue_batches)
            || !(1..=2500).contains(&self.batch_rows)
            || !(65536..=8 * 1024 * 1024).contains(&self.batch_bytes)
            || self.batch_rows * size_of::<XmltvProgramme>() + 4096 >= self.batch_bytes
            || self.max_file_bytes == 0
            || self.max_file_bytes > 4 * 1024 * 1024 * 1024 - 65536
            || self.max_programmes == 0
            || self.max_programmes > 5_000_000
            || !(1..=5000).contains(&self.insert_batch_rows)
        {
            return Err(ImportError::Capacity("invalid import configuration"));
        }
        Ok(())
    }
}
#[derive(Debug)]
pub enum ImportError {
    Busy,
    Stopping,
    Cancelled,
    Incomplete,
    FileChanged,
    Capacity(&'static str),
    Io(io::Error),
    Store(StoreError),
    Parser,
    Worker,
}
impl From<StoreError> for ImportError {
    fn from(value: StoreError) -> Self {
        Self::Store(value)
    }
}
impl From<sqlx::Error> for ImportError {
    fn from(value: sqlx::Error) -> Self {
        Self::Store(value.into())
    }
}
impl From<io::Error> for ImportError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
impl std::fmt::Display for ImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ImportError {}

#[derive(Clone)]
pub struct ImportReporting {
    pub reporter: reporter::GuideReporter,
    pub source_name: String,
    /// Missing files with a URL still require download orchestration. This
    /// adapter only translates Source's explicit no-URL branch when false.
    pub source_has_url: bool,
}

pub struct ImportRequest {
    pub source_id: i64,
    pub path: PathBuf,
    pub now: DateTime<Utc>,
    pub policy: EpgHistoryPolicy,
    pub cancelled: Arc<AtomicBool>,
}
/// Evaluated only after complete parsing and actual staging replies. The fixed
/// request remains available for deterministic component replay.
#[derive(Clone, Copy, Debug)]
pub struct SwapPolicy {
    pub now: DateTime<Utc>,
    pub history: EpgHistoryPolicy,
}
#[derive(Clone)]
pub struct SwapPolicyContext {
    pub cancelled: Arc<AtomicBool>,
    /// Clone this into any supervised I/O task that can outlive its waiter.
    /// Logical cancellation must not release the physical job/lock guard.
    pub retained: Arc<dyn Send + Sync>,
}
impl SwapPolicyContext {
    pub fn ensure_active(&self) -> Result<(), ImportError> {
        cancelled(&self.cancelled)
    }
}
pub type SwapPolicyFuture<'a> =
    Pin<Box<dyn Future<Output = Result<SwapPolicy, ImportError>> + Send + 'a>>;
pub trait SwapPolicyProvider: Send + Sync {
    fn resolve<'a>(&'a self, context: SwapPolicyContext) -> SwapPolicyFuture<'a>;
}
#[derive(Debug, Default)]
pub struct ImportReport {
    pub parsed_programmes: u64,
    pub skipped_unmapped: u64,
    pub invalid_programmes: u64,
    pub channels_with_programmes: usize,
    pub parser: XmltvStats,
    pub swap: GuideSwapCounts,
    pub no_mapped_channels: bool,
    /// Actual initial mapping used by this parse, for Source late-mapping dispatch.
    /// Returned only by the reported import path; bounded by staging limits.
    pub initial_mapped_epg_ids: Vec<i64>,
}
#[derive(Clone)]
pub struct EpgImporter {
    inner: Arc<Inner>,
}
struct Inner {
    store: Store,
    config: ImportConfig,
    admission: Arc<Semaphore>,
    stopping: AtomicBool,
    jobs: Mutex<HashMap<u64, Weak<PhysicalJob>>>,
    next: AtomicU64,
    changed: Notify,
}
struct PhysicalJob {
    id: u64,
    inner: Weak<Inner>,
    cancelled: Arc<AtomicBool>,
    _admission: Option<OwnedSemaphorePermit>,
    _retained: Option<Arc<dyn Send + Sync>>,
}
impl Drop for PhysicalJob {
    fn drop(&mut self) {
        // Release our actual retained context/slot before reporting no jobs.
        drop(self._retained.take());
        drop(self._admission.take());
        if let Some(inner) = self.inner.upgrade() {
            inner.jobs.lock().unwrap().remove(&self.id);
            inner.changed.notify_waiters();
        }
    }
}
struct CancelOnDrop {
    cancelled: Arc<AtomicBool>,
    armed: bool,
}
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.cancelled.store(true, Ordering::Release);
        }
    }
}
impl EpgImporter {
    pub fn new(store: Store, config: ImportConfig) -> Result<Self, ImportError> {
        config.validate()?;
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                admission: Arc::new(Semaphore::new(config.max_jobs)),
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
    /// Caller/awaiter cancellation requests cancellation without dropping physical
    /// parser/SQL work, admission or the caller's retained job/lock guard.
    pub async fn import_file(
        &self,
        request: ImportRequest,
        retained: Arc<dyn Send + Sync>,
    ) -> Result<ImportReport, ImportError> {
        self.import_with_provider(request, retained, None, None)
            .await
    }
    /// Refresh Source history/cache policy and sample the cutoff clock after
    /// parser completion, rather than binding them at the start of a long file.
    pub async fn import_file_with_swap_policy(
        &self,
        request: ImportRequest,
        retained: Arc<dyn Send + Sync>,
        provider: Arc<dyn SwapPolicyProvider>,
    ) -> Result<ImportReport, ImportError> {
        self.import_with_provider(request, retained, Some(provider), None)
            .await
    }
    /// Add source-characterized status/progress effects to the owned importer.
    /// Download/extraction, SQL exception spelling, integrations and late mapping
    /// task dispatch remain separate orchestration gates; this stays app-unwired.
    pub async fn import_file_reported(
        &self,
        request: ImportRequest,
        retained: Arc<dyn Send + Sync>,
        provider: Option<Arc<dyn SwapPolicyProvider>>,
        reporting: ImportReporting,
    ) -> Result<ImportReport, ImportError> {
        if reporting.source_name.capacity() > 4096 {
            return Err(ImportError::Capacity("source name"));
        }
        self.import_with_provider(request, retained, provider, Some(reporting))
            .await
    }
    async fn import_with_provider(
        &self,
        request: ImportRequest,
        retained: Arc<dyn Send + Sync>,
        provider: Option<Arc<dyn SwapPolicyProvider>>,
        reporting: Option<ImportReporting>,
    ) -> Result<ImportReport, ImportError> {
        if self.inner.stopping.load(Ordering::Acquire) {
            return Err(ImportError::Stopping);
        }
        let permit = self
            .inner
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| ImportError::Busy)?;
        let state = Arc::new(PhysicalJob {
            id: self.inner.next.fetch_add(1, Ordering::Relaxed),
            inner: Arc::downgrade(&self.inner),
            cancelled: request.cancelled.clone(),
            _admission: Some(permit),
            _retained: Some(retained),
        });
        {
            let mut jobs = self.inner.jobs.lock().unwrap();
            if self.inner.stopping.load(Ordering::Acquire) {
                drop(jobs);
                return Err(ImportError::Stopping);
            }
            jobs.insert(state.id, Arc::downgrade(&state));
        }
        let mut cancellation = CancelOnDrop {
            cancelled: state.cancelled.clone(),
            armed: true,
        };
        let inner = self.inner.clone();
        let result = tokio::spawn(async move {
            let source_id=request.source_id;
            let result=import_owned(inner,request,state.clone(),provider,reporting.clone()).await;
            if let (Some(reporting),Err(error))=(reporting,result.as_ref()) {
                let failure=match error {
                    ImportError::Incomplete=>Some(("EPG file appears incomplete (missing closing </tv>). Keeping previous data when available; will retry on next refresh.","EPG file appears incomplete (missing closing </tv>). Keeping previous data when available; will retry on next refresh.")),
                    ImportError::Io(value) if value.kind()==io::ErrorKind::NotFound&&!reporting.source_has_url=>Some(("No URL provided, cannot fetch EPG data","No URL provided")),
                    _=>None,
                };
                if let Some((message,publication_error))=failure {
                    let _=reporting.reporter.execute(reporter::ReportAction::Failed(reporter::FailedImport{source_id,source_name:reporting.source_name,last_message:message.into(),publication_error:publication_error.into()}),job_context(&state)).await;
                }
            }
            result
        }).await.map_err(|_|ImportError::Worker)?;
        cancellation.armed = false;
        result
    }
    /// A false result means physical work still retains its guard. Never abort
    /// the blocking parser or owned SQL command merely to report clean shutdown.
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        let active = {
            let jobs = self.inner.jobs.lock().unwrap();
            self.inner.stopping.store(true, Ordering::Release);
            jobs.values().filter_map(Weak::upgrade).collect::<Vec<_>>()
        };
        for job in active {
            job.cancelled.store(true, Ordering::Release);
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let changed = self.inner.changed.notified();
            if self.physical_jobs() == 0 {
                return true;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return self.physical_jobs() == 0;
            }
        }
    }
}

enum Batch {
    Validated,
    Rows {
        rows: Vec<XmltvProgramme>,
        progress: Option<u64>,
    },
}
fn job_context(state: &Arc<PhysicalJob>) -> SwapPolicyContext {
    SwapPolicyContext {
        cancelled: state.cancelled.clone(),
        retained: state.clone(),
    }
}
#[derive(Default)]
struct Parsed {
    report: ImportReport,
}
fn cancelled(flag: &AtomicBool) -> Result<(), ImportError> {
    if flag.load(Ordering::Acquire) {
        Err(ImportError::Cancelled)
    } else {
        Ok(())
    }
}
fn identity(metadata: &Metadata) -> (u64, u64, u64, i64, i64, i64, i64) {
    (
        metadata.dev(),
        metadata.ino(),
        metadata.len(),
        metadata.mtime(),
        metadata.mtime_nsec(),
        metadata.ctime(),
        metadata.ctime_nsec(),
    )
}
fn row_charge(row: &XmltvProgramme) -> Result<usize, ImportError> {
    // String capacities survive projection. A conservative factor charges JSON
    // nodes/maps/numbers as well as encoded content; reject a row before queueing.
    let custom = row
        .custom_properties
        .as_ref()
        .map(serde_json::to_vec)
        .transpose()
        .map_err(|_| ImportError::Parser)?;
    let mut bytes = 4096usize;
    for size in [
        row.title.capacity(),
        row.tvg_id.capacity(),
        row.description.as_ref().map_or(0, String::capacity),
        row.sub_title.as_ref().map_or(0, String::capacity),
        custom
            .as_ref()
            .map_or(0, |value| value.capacity().saturating_mul(64)),
    ] {
        bytes = bytes
            .checked_add(size)
            .ok_or(ImportError::Capacity("programme row"))?;
    }
    Ok(bytes)
}
fn send(
    tx: &mpsc::Sender<Batch>,
    rows: Vec<XmltvProgramme>,
    flag: &AtomicBool,
) -> Result<(), ImportError> {
    cancelled(flag)?;
    tx.blocking_send(Batch::Rows {
        rows,
        progress: None,
    })
    .map_err(|_| ImportError::Cancelled)
}
fn parse_file(
    path: PathBuf,
    mapping: HashMap<String, i64>,
    config: ImportConfig,
    flag: Arc<AtomicBool>,
    tx: mpsc::Sender<Batch>,
) -> Result<Parsed, ImportError> {
    cancelled(&flag)?;
    let mut file = File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > config.max_file_bytes {
        return Err(ImportError::Capacity("input file"));
    }
    let original = identity(&metadata);
    if !xmltv_file_handle_is_complete(&mut file) {
        return Err(ImportError::Incomplete);
    }
    tx.blocking_send(Batch::Validated)
        .map_err(|_| ImportError::Cancelled)?;
    let mut monitor = file.try_clone()?;
    let reader = XmltvReader::from_file(file)?;
    let mut parsed = Parsed::default();
    let mut channels = HashSet::new();
    let baseline = config.batch_rows * size_of::<XmltvProgramme>() + 4096;
    let mut rows = Vec::with_capacity(config.batch_rows);
    let mut bytes = baseline;
    let stats = parse_xmltv_reader(reader, &flag, |node| {
        cancelled(&flag)?;
        if node.tag != "programme" {
            return Ok(());
        }
        let tvg = node.attrs.get("channel");
        let Some(epg) = tvg.and_then(|key| mapping.get(key)) else {
            parsed.report.skipped_unmapped += 1;
            return Ok(());
        };
        let row = match project_xmltv_programme(&node, *epg, 255) {
            Ok(row) => row,
            Err(_) => {
                parsed.report.invalid_programmes += 1;
                return Ok(());
            }
        };
        let charge = row_charge(&row)?;
        if charge.saturating_add(baseline) > config.batch_bytes {
            return Err(ImportError::Capacity("programme row"));
        }
        if rows.len() == config.batch_rows || bytes.saturating_add(charge) > config.batch_bytes {
            send(
                &tx,
                std::mem::replace(&mut rows, Vec::with_capacity(config.batch_rows)),
                &flag,
            )?;
            bytes = baseline;
        }
        parsed.report.parsed_programmes += 1;
        if parsed.report.parsed_programmes > config.max_programmes {
            return Err(ImportError::Capacity("programme count"));
        }
        channels.insert(*epg);
        rows.push(row);
        bytes += charge;
        if parsed.report.parsed_programmes.is_multiple_of(5000) {
            cancelled(&flag)?;
            tx.blocking_send(Batch::Rows {
                rows: std::mem::replace(&mut rows, Vec::with_capacity(config.batch_rows)),
                progress: Some(parsed.report.parsed_programmes),
            })
            .map_err(|_| ImportError::Cancelled)?;
            bytes = baseline;
        }
        Ok(())
    });
    parsed.report.parser = match stats {
        Ok(stats) => stats,
        Err(XmltvError::Sink(error)) => return Err(error),
        Err(XmltvError::Cancelled) => return Err(ImportError::Cancelled),
        Err(XmltvError::Io(error)) => return Err(ImportError::Io(error)),
        Err(XmltvError::Capacity) => return Err(ImportError::Capacity("XML parser")),
        Err(_) => return Err(ImportError::Parser),
    };
    cancelled(&flag)?;
    if identity(&monitor.metadata()?) != original {
        return Err(ImportError::FileChanged);
    }
    if !xmltv_file_handle_is_complete(&mut monitor) {
        return Err(ImportError::Incomplete);
    }
    if !rows.is_empty() {
        send(&tx, rows, &flag)?;
    }
    parsed.report.channels_with_programmes = channels.len();
    Ok(parsed)
}
async fn import_owned(
    inner: Arc<Inner>,
    request: ImportRequest,
    state: Arc<PhysicalJob>,
    provider: Option<Arc<dyn SwapPolicyProvider>>,
    reporting: Option<ImportReporting>,
) -> Result<ImportReport, ImportError> {
    cancelled(&state.cancelled)?;
    if let Some(reporting) = reporting.as_ref() {
        reporting
            .reporter
            .execute(
                reporter::ReportAction::Progress {
                    source_id: request.source_id,
                    progress: 0,
                },
                job_context(&state),
            )
            .await?;
    }
    let stage = inner
        .store
        .begin_guide_staging(request.source_id, state.clone())
        .await?;
    let (stage, mapping) = stage.mapping_snapshot().await?;
    let (stage, total_epg_entries) = if reporting.is_some() {
        stage.source_entry_count().await?
    } else {
        (stage, 0)
    };
    let initial_mapped_epg_ids = if reporting.is_some() {
        mapping.epg_ids.clone()
    } else {
        Vec::new()
    };
    let mapped_tvg_ids = mapping.by_tvg_id.len();
    cancelled(&state.cancelled)?;
    if mapping.epg_ids.is_empty() {
        if let Some(reporting) = reporting.as_ref() {
            reporting
                .reporter
                .execute(
                    reporter::ReportAction::NoMappings {
                        source_id: request.source_id,
                        total_epg_entries: total_epg_entries as u64,
                    },
                    job_context(&state),
                )
                .await?;
        }
        return Ok(ImportReport {
            no_mapped_channels: true,
            ..ImportReport::default()
        });
    }
    let (tx, mut rx) = mpsc::channel(inner.config.queue_batches);
    let config = inner.config.clone();
    let parser_state = state.clone();
    let parser = tokio::task::spawn_blocking(move || {
        let _guard = parser_state.clone();
        parse_file(
            request.path,
            mapping.by_tvg_id,
            config,
            parser_state.cancelled.clone(),
            tx,
        )
    });
    let mut staging = Some(stage);
    let mut failure = None;
    while let Some(batch) = rx.recv().await {
        if let Err(error) = cancelled(&state.cancelled) {
            failure = Some(error);
            break;
        }
        let result = async {
            match batch {
                Batch::Validated => {
                    if let Some(reporting) = reporting.as_ref() {
                        reporting
                            .reporter
                            .execute(
                                reporter::ReportAction::Progress {
                                    source_id: request.source_id,
                                    progress: 10,
                                },
                                job_context(&state),
                            )
                            .await?;
                    }
                }
                Batch::Rows { rows, progress } => {
                    if !rows.is_empty() {
                        staging = Some(staging.take().unwrap().push(rows).await?);
                    }
                    if let (Some(reporting), Some(processed)) = (reporting.as_ref(), progress) {
                        reporting
                            .reporter
                            .execute(
                                reporter::ReportAction::Staging {
                                    source_id: request.source_id,
                                    processed,
                                    mapped_tvg_ids,
                                },
                                job_context(&state),
                            )
                            .await?;
                    }
                }
            }
            Ok::<_, ImportError>(())
        }
        .await;
        if let Err(error) = result {
            failure = Some(error);
            break;
        }
    }
    if failure.is_some() {
        state.cancelled.store(true, Ordering::Release);
    }
    // Closing releases a parser blocked on a full bounded queue, then join its
    // actual blocking task before releasing admission or retained job context.
    rx.close();
    drop(rx);
    let parsed = parser.await.map_err(|_| ImportError::Worker)?;
    if let Some(error) = failure {
        return Err(error);
    }
    let mut parsed = parsed?;
    cancelled(&state.cancelled)?;
    if let Some(reporting) = reporting.as_ref() {
        reporting
            .reporter
            .execute(
                reporter::ReportAction::Progress {
                    source_id: request.source_id,
                    progress: 90,
                },
                job_context(&state),
            )
            .await?;
    }
    let policy = if let Some(provider) = provider.filter(|_| parsed.report.parsed_programmes > 0) {
        provider
            .resolve(SwapPolicyContext {
                cancelled: state.cancelled.clone(),
                retained: state.clone(),
            })
            .await?
    } else {
        SwapPolicy {
            now: request.now,
            history: request.policy,
        }
    };
    // An owned provider remains awaited during cancellation, retaining admission
    // until its physical I/O completes. A cancelled result never reaches swap.
    cancelled(&state.cancelled)?;
    parsed.report.swap = staging
        .unwrap()
        .swap_cancellable(
            mapping.epg_ids,
            policy.now,
            policy.history,
            inner.config.insert_batch_rows,
            state.cancelled.clone(),
        )
        .await?;
    if let Some(reporting) = reporting {
        reporting
            .reporter
            .execute(
                reporter::ReportAction::Committed(reporter::CompletedImport {
                    source_id: request.source_id,
                    source_name: reporting.source_name,
                    accepted: parsed.report.parsed_programmes,
                    channels_with_programmes: parsed.report.channels_with_programmes,
                    skipped_unmapped: parsed.report.skipped_unmapped,
                    total_epg_entries,
                    mapped_tvg_ids,
                }),
                job_context(&state),
            )
            .await?;
    }
    parsed.report.initial_mapped_epg_ids = initial_mapped_epg_ids;
    Ok(parsed.report)
}

#[cfg(test)]
mod tests;

// Source orphan79/native125 helper; no application/default consumer authority.
pub mod orphan_cleanup;
