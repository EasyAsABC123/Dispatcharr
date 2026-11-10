//! Bounded literal XMLTV channel pass, ported from original Dispatcharr tasks.
//! Full credit belongs to original authors. Refresh/dispatch wiring is separate.
use crate::{
    channel_core::{self, ChannelRow},
    channel_sql::ChannelSession,
    reporter::{FailedImport, GuideReporter, ReportAction},
    ImportError, SwapPolicyContext,
};
use dispatcharr_ingest_core::XmltvReader;
use dispatcharr_store::Store;
use dispatcharr_xmltv_parser::{parse_xmltv_reader, XmltvError};
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
use tokio::sync::{mpsc, Notify, OwnedSemaphorePermit, Semaphore};
#[derive(Clone, Debug)]
pub struct ChannelConfig {
    pub max_jobs: usize,
    pub queue_frames: usize,
    pub queue_bytes: usize,
    pub max_keys: usize,
    pub state_bytes: usize,
    pub file_bytes: u64,
}
impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            max_jobs: 2,
            queue_frames: 32,
            queue_bytes: 8 * 1024 * 1024,
            max_keys: 100_000,
            state_bytes: 64 * 1024 * 1024,
            file_bytes: 4 * 1024 * 1024 * 1024 - 65536,
        }
    }
}
impl ChannelConfig {
    fn validate(&self) -> Result<(), ImportError> {
        if !(1..=4).contains(&self.max_jobs)
            || !(1..=128).contains(&self.queue_frames)
            || !(65536..=64 * 1024 * 1024).contains(&self.queue_bytes)
            || !(1..=1_000_000).contains(&self.max_keys)
            || !(1024 * 1024..=256 * 1024 * 1024).contains(&self.state_bytes)
            || self.file_bytes == 0
            || self.file_bytes > 4 * 1024 * 1024 * 1024 - 65536
        {
            return Err(ImportError::Capacity("channel configuration"));
        }
        Ok(())
    }
}
pub struct ChannelRequest {
    pub source_id: i64,
    pub source_name: String,
    pub path: PathBuf,
    /// Prepared by the source/path resolver (including cache-parent creation).
    pub cache_file: PathBuf,
    pub source_has_url: bool,
    pub cancelled: Arc<AtomicBool>,
}
#[derive(Debug, Default)]
pub struct ChannelReport {
    pub processed: u64,
    pub initial_total: u64,
}
#[derive(Debug)]
pub enum ChannelError {
    Import(ImportError),
    NeedsFetch,
    NoUrl,
    Scan(ImportError),
    Final(ImportError),
}
impl From<ImportError> for ChannelError {
    fn from(value: ImportError) -> Self {
        Self::Import(value)
    }
}
struct Inner {
    store: Store,
    config: ChannelConfig,
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
    frame_bytes: Arc<Semaphore>,
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
#[derive(Clone)]
pub struct ChannelImporter {
    inner: Arc<Inner>,
}
impl ChannelImporter {
    pub fn new(store: Store, config: ChannelConfig) -> Result<Self, ImportError> {
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
    /// Scalar retained frame credits, including queued and blocked SQL frames.
    pub fn retained_frame_bytes(&self) -> usize {
        let jobs = {
            self.inner
                .jobs
                .lock()
                .unwrap()
                .values()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>()
        };
        jobs.iter()
            .map(|job| self.inner.config.queue_bytes - job.frame_bytes.available_permits())
            .sum()
    }
    pub async fn import_channels(
        &self,
        request: ChannelRequest,
        retained: Arc<dyn Send + Sync>,
        reporter: Option<GuideReporter>,
    ) -> Result<ChannelReport, ChannelError> {
        if request.source_name.capacity() > 4096 {
            return Err(ImportError::Capacity("source name").into());
        }
        if self.inner.stopping.load(Ordering::Acquire) {
            return Err(ImportError::Stopping.into());
        }
        let permit = self
            .inner
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| ImportError::Busy)?;
        let state = Arc::new(Job {
            id: self.inner.next.fetch_add(1, Ordering::Relaxed),
            inner: Arc::downgrade(&self.inner),
            cancelled: request.cancelled.clone(),
            frame_bytes: Arc::new(Semaphore::new(self.inner.config.queue_bytes)),
            caller: Some(retained),
            permit: Some(permit),
        });
        {
            let mut jobs = self.inner.jobs.lock().unwrap();
            if self.inner.stopping.load(Ordering::Acquire) {
                drop(jobs);
                return Err(ImportError::Stopping.into());
            }
            jobs.insert(state.id, Arc::downgrade(&state));
        }
        let mut cancel = Cancel {
            flag: state.cancelled.clone(),
            armed: true,
        };
        let inner = self.inner.clone();
        let result = tokio::spawn(async move {
            let id = request.source_id;
            let name = request.source_name.clone();
            let result = run(inner, request, state.clone(), reporter.as_ref()).await;
            if let (Some(reporter), Err(error)) = (reporter, result.as_ref()) {
                let failure = match error {
                    ChannelError::NoUrl => Some((
                        "No URL provided, cannot fetch EPG data".into(),
                        "No URL provided".into(),
                    )),
                    ChannelError::Scan(error) => {
                        let message = error_text(error);
                        Some((format!("Error parsing XML file: {message}"), message))
                    }
                    ChannelError::Final(error) => {
                        let message = error_text(error);
                        Some((format!("Error parsing EPG file: {message}"), message))
                    }
                    _ => None,
                };
                if let Some((last_message, publication_error)) = failure {
                    let _ = reporter
                        .execute(
                            ReportAction::ChannelFailed(FailedImport {
                                source_id: id,
                                source_name: name,
                                last_message,
                                publication_error,
                            }),
                            terminal_context(&state),
                        )
                        .await;
                }
            }
            result
        })
        .await
        .map_err(|_| ImportError::Worker)?;
        cancel.armed = false;
        result
    }
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
            let notified = self.inner.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.physical_jobs() == 0 {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.physical_jobs() == 0;
            }
        }
    }
}
fn context(state: &Arc<Job>) -> SwapPolicyContext {
    SwapPolicyContext {
        cancelled: state.cancelled.clone(),
        retained: state.clone(),
    }
}
fn terminal_context(state: &Arc<Job>) -> SwapPolicyContext {
    SwapPolicyContext {
        cancelled: Arc::new(AtomicBool::new(false)),
        retained: state.clone(),
    }
}
fn active(state: &Job) -> Result<(), ImportError> {
    if state.cancelled.load(Ordering::Acquire) {
        Err(ImportError::Cancelled)
    } else {
        Ok(())
    }
}
async fn publish(
    reporter: Option<&GuideReporter>,
    state: &Arc<Job>,
    action: ReportAction,
) -> Result<(), ImportError> {
    if let Some(reporter) = reporter {
        reporter.execute(action, context(state)).await?;
    }
    Ok(())
}
fn tick(
    id: i64,
    value: u8,
    processed: Option<u64>,
    total: Option<u64>,
    initial: Option<u64>,
) -> ReportAction {
    ReportAction::ChannelProgress {
        source_id: id,
        progress: value,
        processed,
        total,
        total_channels: initial,
    }
}
struct Frame {
    row: Option<ChannelRow>,
    _bytes: OwnedSemaphorePermit,
}
fn parse(
    path: PathBuf,
    inner: &Inner,
    state: Arc<Job>,
    tx: mpsc::Sender<Frame>,
    bytes: Arc<Semaphore>,
) -> Result<(), ImportError> {
    active(&state)?;
    let file = File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > inner.config.file_bytes {
        return Err(ImportError::Capacity("channel file"));
    }
    // The original helper intentionally recovers a missing footer. The fetch
    // layer performs its separate extension-dependent completeness check.
    let reader = XmltvReader::from_file(file)?;
    let result = parse_xmltv_reader(reader, &state.cancelled, |node| {
        active(&state)?;
        if node.tag != "channel" {
            return Ok(());
        }
        let row = channel_core::project_channel(&node);
        let charge = 256 + row.as_ref().map_or(0, ChannelRow::allocated_fields);
        if charge > inner.config.queue_bytes {
            return Err(ImportError::Capacity("channel frame"));
        }
        let permit = tokio::runtime::Handle::current()
            .block_on(bytes.clone().acquire_many_owned(charge as u32))
            .map_err(|_| ImportError::Cancelled)?;
        active(&state)?;
        tx.blocking_send(Frame {
            row,
            _bytes: permit,
        })
        .map_err(|_| ImportError::Cancelled)
    });
    match result {
        Ok(_) => Ok(()),
        Err(XmltvError::Sink(error)) => Err(error),
        Err(XmltvError::Io(error)) => Err(ImportError::Io(error)),
        Err(XmltvError::Cancelled) => Err(ImportError::Cancelled),
        Err(XmltvError::Capacity) => Err(ImportError::Capacity("channel parser")),
        Err(_) => Err(ImportError::Parser),
    }
}
async fn run(
    inner: Arc<Inner>,
    request: ChannelRequest,
    state: Arc<Job>,
    reporter: Option<&GuideReporter>,
) -> Result<ChannelReport, ChannelError> {
    active(&state)?;
    publish(
        reporter,
        &state,
        tick(request.source_id, 0, None, None, None),
    )
    .await?;
    active(&state)?;
    let path = request.path.clone();
    let stat_state = state.clone();
    let exists = tokio::task::spawn_blocking(move || {
        let _retained = stat_state;
        path.exists()
    })
    .await
    .map_err(|_| ImportError::Worker)?;
    if !exists {
        sqlx::query("UPDATE epg_epgsource SET file_path=$2 WHERE id=$1")
            .bind(request.source_id)
            .bind(request.cache_file.to_string_lossy().as_ref())
            .execute(inner.store.pool())
            .await
            .map_err(ImportError::from)?;
        return Err(if request.source_has_url {
            ChannelError::NeedsFetch
        } else {
            ChannelError::NoUrl
        });
    }
    let mut session = ChannelSession::open(
        &inner.store,
        request.source_id,
        state.clone(),
        inner.config.max_keys,
        inner.config.state_bytes,
    )
    .await
    .map_err(ChannelError::Final)?;
    publish(
        reporter,
        &state,
        tick(request.source_id, 10, None, None, None),
    )
    .await?;
    active(&state)?;
    // Original count failure uses an estimate of 500 rows, then continues parsing.
    let initial = session.count().await.unwrap_or(500);
    let had = initial > 0;
    let mut estimate = initial;
    publish(
        reporter,
        &state,
        tick(request.source_id, 25, None, None, Some(initial)),
    )
    .await?;
    active(&state)?;
    let (tx, mut rx) = mpsc::channel(inner.config.queue_frames);
    let bytes = state.frame_bytes.clone();
    let parse_inner = inner.clone();
    let parse_state = state.clone();
    let parser = tokio::task::spawn_blocking(move || {
        parse(request.path, &parse_inner, parse_state, tx, bytes)
    });
    let mut processed = 0u64;
    let mut failure = None;
    while let Some(frame) = rx.recv().await {
        let result = async {
            active(&state)?;
            processed += 1;
            let skip = if let Some(row) = frame.row {
                session.channel(row).await?
            } else {
                false
            };
            if skip {
                return Ok(());
            }
            session.flush_full().await?;
            if processed.is_multiple_of(1000) {
                session.clear_cache();
            }
            if channel_core::should_publish(processed, initial, had) {
                let (value, total) = channel_core::progress(processed, estimate, had);
                estimate = total;
                publish(
                    reporter,
                    &state,
                    tick(
                        request.source_id,
                        value,
                        Some(processed),
                        Some(estimate),
                        None,
                    ),
                )
                .await?;
            }
            Ok::<_, ImportError>(())
        }
        .await;
        if let Err(error) = result {
            failure = Some(error);
            state.cancelled.store(true, Ordering::Release);
            break;
        }
    }
    rx.close();
    drop(rx);
    let parsed = parser.await.map_err(|_| ImportError::Worker)?;
    if let Some(error) = failure {
        return Err(ChannelError::Scan(error));
    }
    parsed.map_err(ChannelError::Scan)?;
    active(&state)?;
    if processed > 0 {
        let (value, total) = channel_core::progress(processed, estimate, had);
        publish(
            reporter,
            &state,
            tick(request.source_id, value, Some(processed), Some(total), None),
        )
        .await?;
    }
    session.finish().await.map_err(ChannelError::Final)?;
    publish(
        reporter,
        &state,
        ReportAction::ChannelsComplete {
            source_id: request.source_id,
            processed,
        },
    )
    .await?;
    Ok(ChannelReport {
        processed,
        initial_total: initial,
    })
}
fn error_text(error: &ImportError) -> String {
    if let ImportError::Store(dispatcharr_store::StoreError::Database(value)) = error {
        if let Some(database) = value.as_database_error() {
            return database.message().into();
        }
    }
    error.to_string()
}
