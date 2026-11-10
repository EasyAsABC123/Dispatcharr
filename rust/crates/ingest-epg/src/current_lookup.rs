//! Post96 draft: owned fresh-index orchestration, not app-wired or accepted.
//! Original Dispatcharr authors retain all application and algorithm credit.
use crate::{
    current_file::{self, CurrentFileError, CurrentFileLimits, CurrentFileResult},
    ImportError, SwapPolicyContext,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_store::{CurrentFileLookup, Store, StoreError};
pub use dispatcharr_store::{CurrentFileOutcome, CurrentFileRequest};
use sqlx::{Connection, Row};
use std::{
    collections::HashMap,
    io::Write,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

/// Implementations must transfer the context through physical broker work.
/// A process-only callback cannot authorize an application Parsing response.
pub trait MissingIndexDispatcher: Send + Sync {
    fn dispatch(
        &self,
        source_id: i64,
        context: SwapPolicyContext,
    ) -> BoxFuture<'_, Result<(), ImportError>>;
}
#[derive(Clone)]
pub struct CurrentLookupServices {
    pub store: Store,
    pub dispatcher: Arc<dyn MissingIndexDispatcher>,
    pub clock: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
}
/// Explicit original-process path context. Stored strings remain unchanged;
/// joins preserve OS symlink/dot traversal and neither expand nor canonicalize.
/// Application/volume migration must supply a separately verified context.
#[derive(Clone, Debug)]
pub struct SourcePathContext {
    source_cwd: std::path::PathBuf,
    media_root: std::path::PathBuf,
}
impl SourcePathContext {
    pub fn new(
        source_cwd: std::path::PathBuf,
        media_root: std::path::PathBuf,
    ) -> Result<Self, ImportError> {
        if [&source_cwd, &media_root]
            .iter()
            .any(|path| !path.is_absolute() || path.as_os_str().len() > 4096)
        {
            return Err(ImportError::Capacity("source path context"));
        }
        Ok(Self {
            source_cwd,
            media_root,
        })
    }
    pub fn source_cwd(&self) -> &std::path::Path {
        &self.source_cwd
    }
    pub fn media_root(&self) -> &std::path::Path {
        &self.media_root
    }
    fn resolve(&self, path: std::path::PathBuf) -> std::path::PathBuf {
        if path.is_absolute() {
            path
        } else {
            self.source_cwd.join(path)
        }
    }
    fn cache_file(&self, source_id: i64) -> std::path::PathBuf {
        self.media_root
            .join("cached_epg")
            .join(format!("{source_id}.tmp"))
    }
}
#[derive(Clone, Debug)]
pub struct CurrentLookupConfig {
    /// None preserves the existing absolute-only component API.
    pub path_context: Option<SourcePathContext>,
    pub max_jobs: usize,
    pub memory_bytes: usize,
    pub response_bytes: usize,
    pub limits: CurrentFileLimits,
}
impl Default for CurrentLookupConfig {
    fn default() -> Self {
        Self {
            path_context: None,
            max_jobs: 2,
            memory_bytes: 192 * 1024 * 1024,
            response_bytes: 4 * 1024 * 1024,
            limits: CurrentFileLimits::default(),
        }
    }
}
#[derive(Clone)]
pub struct CurrentLookup {
    inner: Arc<Inner>,
}
struct Inner {
    services: CurrentLookupServices,
    config: CurrentLookupConfig,
    admission: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    reservation: usize,
    stopping: AtomicBool,
    next: AtomicU64,
    jobs: Mutex<HashMap<u64, Weak<Job>>>,
    changed: Notify,
}
struct Memory {
    permit: Mutex<OwnedSemaphorePermit>,
}
struct Job {
    id: u64,
    inner: Weak<Inner>,
    cancelled: Arc<AtomicBool>,
    _caller: Arc<dyn Send + Sync>,
    _admission: OwnedSemaphorePermit,
    memory: Mutex<Option<Arc<Memory>>>,
}
impl Drop for Job {
    fn drop(&mut self) {
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
struct ResponseOwner {
    data: Vec<u8>,
    _memory: OwnedSemaphorePermit,
}
impl AsRef<[u8]> for ResponseOwner {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
struct LimitedWriter {
    data: Vec<u8>,
    limit: usize,
}
impl Write for LimitedWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if data.len() > self.limit.saturating_sub(self.data.len()) {
            return Err(std::io::Error::other("current response capacity"));
        }
        let required = self.data.len() + data.len();
        if required > self.data.capacity() {
            let target = self
                .data
                .capacity()
                .saturating_mul(2)
                .max(4096)
                .max(required)
                .min(self.limit);
            self.data
                .try_reserve_exact(target - self.data.len())
                .map_err(|_| std::io::Error::other("current response allocation"))?;
        }
        self.data.extend_from_slice(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl CurrentLookup {
    pub fn new(
        services: CurrentLookupServices,
        config: CurrentLookupConfig,
    ) -> Result<Self, ImportError> {
        let reservation = config
            .limits
            .buffer_bytes
            .checked_add(64 * 1024 * 1024)
            .and_then(|v| {
                config
                    .response_bytes
                    .checked_mul(2)
                    .and_then(|r| v.checked_add(r))
            })
            .ok_or(ImportError::Capacity("current lookup memory"))?;
        if !(1..=4).contains(&config.max_jobs)
            || !(65536..=16 * 1024 * 1024).contains(&config.response_bytes)
            || reservation > config.memory_bytes
            || config.memory_bytes > 1024 * 1024 * 1024
            || !(2 * 1024 * 1024..=16 * 1024 * 1024).contains(&config.limits.buffer_bytes)
            || !(1..=4 * 1024 * 1024 * 1024).contains(&config.limits.read_bytes)
            || !(1..=100).contains(&config.limits.max_offsets)
            || config.limits.forward_timeout.is_zero()
            || config.limits.forward_timeout > Duration::from_secs(10)
        {
            return Err(ImportError::Capacity("current lookup configuration"));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                services,
                admission: Arc::new(Semaphore::new(config.max_jobs)),
                memory: Arc::new(Semaphore::new(config.memory_bytes)),
                reservation,
                config,
                stopping: AtomicBool::new(false),
                next: AtomicU64::new(1),
                jobs: Mutex::new(HashMap::new()),
                changed: Notify::new(),
            }),
        })
    }
    pub fn retained_memory_bytes(&self) -> usize {
        self.inner.config.memory_bytes - self.inner.memory.available_permits()
    }
    pub fn physical_jobs(&self) -> usize {
        self.inner.jobs.lock().unwrap().len()
    }
    pub async fn lookup(
        &self,
        request: CurrentFileRequest,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<CurrentFileOutcome, CurrentFileError> {
        if self.inner.stopping.load(Ordering::Acquire) {
            return Err(ImportError::Stopping.into());
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
            cancelled: Arc::new(AtomicBool::new(false)),
            _caller: caller,
            _admission: permit,
            memory: Mutex::new(None),
        });
        {
            let mut jobs = self.inner.jobs.lock().unwrap();
            if self.inner.stopping.load(Ordering::Acquire) {
                return Err(ImportError::Stopping.into());
            }
            jobs.insert(job.id, Arc::downgrade(&job));
        }
        let mut cancel = Cancel {
            flag: job.cancelled.clone(),
            armed: true,
        };
        let inner = self.inner.clone();
        let result = tokio::spawn(async move { inner.lookup_owned(request, job).await })
            .await
            .map_err(|_| ImportError::Worker)?;
        cancel.armed = false;
        result
    }
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.inner.stopping.store(true, Ordering::Release);
        for job in self
            .inner
            .jobs
            .lock()
            .unwrap()
            .values()
            .filter_map(Weak::upgrade)
        {
            job.cancelled.store(true, Ordering::Release);
        }
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
impl CurrentFileLookup for CurrentLookup {
    fn lookup(
        &self,
        request: CurrentFileRequest,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<CurrentFileOutcome, StoreError>> {
        Box::pin(async move {
            CurrentLookup::lookup(self,request,caller).await.map_err(|error|match error {
            CurrentFileError::Runtime(ImportError::Store(error))=>error,
            CurrentFileError::Runtime(ImportError::Parser)=>StoreError::Unsupported("Unsupported stored programme index shape; Source malformed-index failure parity is not implemented."),
            CurrentFileError::Time(_)=>StoreError::StatusBody(500,serde_json::json!({"detail":"Current programme timestamp invalid."})),
            CurrentFileError::Runtime(ImportError::Io(_))=>StoreError::StatusBody(500,serde_json::json!({"detail":"Current programme file unavailable."})),
            _=>StoreError::StatusBody(503,serde_json::json!({"detail":"Current programme file capacity unavailable."})),
        })
        })
    }
}
enum FreshIndex {
    Absent,
    Missing,
    Offsets { values: Vec<u64>, interleaved: bool },
}
impl Inner {
    async fn lookup_owned(
        &self,
        request: CurrentFileRequest,
        job: Arc<Job>,
    ) -> Result<CurrentFileOutcome, CurrentFileError> {
        let context = SwapPolicyContext {
            cancelled: job.cancelled.clone(),
            retained: job.clone(),
        };
        context.ensure_active()?;
        if matches!(request.source_type.as_str(), "dummy" | "schedules_direct")
            || request.tvg_id.is_empty()
        {
            return Ok(CurrentFileOutcome::Missing);
        }
        if request.tvg_id.len() > 4096 {
            return Err(ImportError::Capacity("current TVG identifier").into());
        }
        let path = request
            .extracted_file_path
            .as_ref()
            .filter(|p| !p.as_os_str().is_empty())
            .or_else(|| {
                request
                    .file_path
                    .as_ref()
                    .filter(|p| !p.as_os_str().is_empty())
            })
            .cloned()
            .unwrap_or_else(|| {
                self.config.path_context.as_ref().map_or_else(
                    || request.cache_file_path.clone(),
                    |context| context.cache_file(request.source_id),
                )
            });
        let path = self
            .config
            .path_context
            .as_ref()
            .map_or_else(|| path.clone(), |context| context.resolve(path.clone()));
        if !path.is_absolute() || path.as_os_str().len() > 4096 {
            return Err(ImportError::Capacity("current file path").into());
        }
        // Retained earlier DTOs may belong to the same ordered HTTP request.
        // Fail capacity rather than wait for that request to release bytes it
        // cannot release until this callback finishes.
        let permit = self
            .memory
            .clone()
            .try_acquire_many_owned(self.reservation as u32)
            .map_err(|_| ImportError::Busy)?;
        let memory = Arc::new(Memory {
            permit: Mutex::new(permit),
        });
        *job.memory.lock().unwrap() = Some(memory.clone());
        let worker = context.clone();
        let check_path = path.clone();
        let make_cache = request
            .file_path
            .as_ref()
            .is_none_or(|p| p.as_os_str().is_empty())
            && request
                .extracted_file_path
                .as_ref()
                .is_none_or(|p| p.as_os_str().is_empty());
        let exists = tokio::task::spawn_blocking(move || {
            let _guard = worker.retained.clone();
            worker.ensure_active()?;
            if make_cache {
                if let Some(parent) = check_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
            }
            Ok::<_, ImportError>(check_path.exists())
        })
        .await
        .map_err(|_| ImportError::Worker)??;
        context.ensure_active()?;
        if !exists {
            return Ok(CurrentFileOutcome::Missing);
        }
        // Source samples now after its file guards, before reading the index.
        let now = (self.services.clock)();
        let index = self
            .fresh_index(request.source_id, &request.tvg_id, &context)
            .await?;
        match index {
            FreshIndex::Absent => {
                self.services
                    .dispatcher
                    .dispatch(request.source_id, context.clone())
                    .await?;
                context.ensure_active()?;
                Ok(CurrentFileOutcome::Parsing)
            }
            FreshIndex::Missing => Ok(CurrentFileOutcome::Missing),
            FreshIndex::Offsets {
                values,
                interleaved,
            } => {
                let worker = context.clone();
                let limits = self.config.limits;
                let response_bytes = self.config.response_bytes;
                let tvg = request.tvg_id;
                let result = tokio::task::spawn_blocking(move || {
                    let _guard = worker.retained.clone();
                    worker.ensure_active()?;
                    let result = current_file::read_current_with_opener(
                        || std::fs::File::open(&path),
                        &tvg,
                        &values,
                        interleaved,
                        now,
                        limits,
                        &worker.cancelled,
                    )?;
                    worker.ensure_active()?;
                    match result {
                        CurrentFileResult::Missing | CurrentFileResult::Timeout => {
                            Ok(CurrentFileOutcome::Missing)
                        }
                        CurrentFileResult::Found(programme) => {
                            let mut writer = LimitedWriter {
                                data: Vec::new(),
                                limit: response_bytes,
                            };
                            serde_json::to_writer(&mut writer, &programme.source_value())
                                .map_err(|_| ImportError::Capacity("current response bytes"))?;
                            worker.ensure_active()?;
                            // Transfer only the retained allocation, not the
                            // whole parser working reservation. Otherwise an
                            // ordered multi-EPG response can block itself while
                            // retaining earlier small DTOs awaiting assembly.
                            let retained = memory
                                .permit
                                .lock()
                                .unwrap()
                                .split(writer.data.capacity())
                                .ok_or(ImportError::Capacity("current response memory"))?;
                            Ok(CurrentFileOutcome::Found(Bytes::from_owner(
                                ResponseOwner {
                                    data: writer.data,
                                    _memory: retained,
                                },
                            )))
                        }
                    }
                })
                .await
                .map_err(|_| ImportError::Worker)?;
                result
            }
        }
    }
    async fn fresh_index(
        &self,
        source: i64,
        tvg: &str,
        context: &SwapPolicyContext,
    ) -> Result<FreshIndex, CurrentFileError> {
        context.ensure_active()?;
        // Project one target instead of decoding an unbounded full index. The
        // old full JSON never enters a pooled connection's retained buffers.
        let mut connection = self
            .services
            .store
            .pool()
            .acquire()
            .await
            .map_err(ImportError::from)?
            .detach();
        let result=async{
            context.ensure_active()?;
            let row=sqlx::query("SELECT jsonb_typeof(data) AS root_type,jsonb_typeof(data->'channels') AS channel_type,(data->'channels') ? $2 AS present,jsonb_typeof(data->'channels'->$2) AS offset_type,octet_length((data->'channels'->$2)::text) AS offset_bytes,CASE WHEN octet_length((data->'channels'->$2)::text)<=4096 THEN (data->'channels'->$2)::text END AS offsets,jsonb_typeof(data->'interleaved_channels') AS interleaved_type,(data->'interleaved_channels') ? $2 AS interleaved FROM epg_epgsourceindex WHERE source_id=$1").bind(source).bind(tvg).fetch_optional(&mut connection).await.map_err(ImportError::from)?;
            context.ensure_active()?;let Some(row)=row else{return Ok(FreshIndex::Absent);};
            let root:Option<String>=row.get("root_type");if root.as_deref().is_none_or(|v|v=="null"){return Ok(FreshIndex::Absent);}
            let channels:Option<String>=row.get("channel_type");if root.as_deref()!=Some("object")||channels.as_deref().is_some_and(|v|v!="object"){return Err(ImportError::Parser.into());}
            if !row.get::<Option<bool>,_>("present").unwrap_or(false){return Ok(FreshIndex::Missing);}
            if row.get::<Option<String>,_>("offset_type").as_deref()!=Some("array"){return Err(ImportError::Parser.into());}
            let text:Option<String>=row.get("offsets");let text=text.ok_or(ImportError::Capacity("current index target bytes"))?;
            let values:Vec<u64>=serde_json::from_str(&text).map_err(|_|ImportError::Parser)?;
            if values.len()>self.config.limits.max_offsets{return Err(ImportError::Capacity("current index offsets").into());}
            let interleaved_type:Option<String>=row.get("interleaved_type");if interleaved_type.as_deref().is_some_and(|v|v!="array"&&v!="null"){return Err(ImportError::Parser.into());}
            Ok(FreshIndex::Offsets{values,interleaved:row.get::<Option<bool>,_>("interleaved").unwrap_or(false)})
        }.await;
        // No pooled transaction survives into file work; physical close is
        // awaited even if the HTTP requester has already canceled.
        connection.close().await.map_err(ImportError::from)?;
        result
    }
}

#[cfg(test)]
mod draft_tests {
    use super::*;
    #[test]
    fn bounded_writer_rejects_before_growing_past_limit() {
        let mut writer = LimitedWriter {
            data: Vec::new(),
            limit: 8,
        };
        writer.write_all(b"12345678").unwrap();
        assert_eq!(writer.data.len(), 8);
        assert_eq!(writer.data.capacity(), 8);
        assert!(writer.write_all(b"9").is_err());
        assert_eq!(writer.data, b"12345678");
    }
    #[test]
    fn response_clones_retain_capacity_but_not_parser_working_credits() {
        let semaphore = Arc::new(Semaphore::new(1024));
        let mut working = semaphore.clone().try_acquire_many_owned(512).unwrap();
        let mut data = Vec::with_capacity(64);
        data.extend_from_slice(b"ok");
        let retained = working.split(data.capacity()).unwrap();
        let body = Bytes::from_owner(ResponseOwner {
            data,
            _memory: retained,
        });
        drop(working);
        assert_eq!(semaphore.available_permits(), 960);
        assert_eq!(body.len(), 2);
        let clone = body.clone();
        drop(body);
        assert_eq!(semaphore.available_permits(), 960);
        assert_eq!(&clone[..], b"ok");
        drop(clone);
        assert_eq!(semaphore.available_permits(), 1024);
    }
}

#[cfg(test)]
mod source_path_tests {
    use super::SourcePathContext;
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    #[test]
    fn context_keeps_literal_source_paths_and_default_cache_layout() {
        let context = SourcePathContext::new("/app".into(), "/app/media".into()).unwrap();
        assert_eq!(
            context.resolve("media/$HOME/~literal +%.xml".into()),
            PathBuf::from("/app/media/$HOME/~literal +%.xml")
        );
        assert_eq!(
            context.resolve("/data/../guide.xml".into()),
            PathBuf::from("/data/../guide.xml")
        );
        assert_eq!(
            context.cache_file(17),
            PathBuf::from("/app/media/cached_epg/17.tmp")
        );
        assert!(SourcePathContext::new("relative".into(), "/app/media".into()).is_err());
        assert!(SourcePathContext::new("/app".into(), "media".into()).is_err());
    }

    #[test]
    fn symlink_parent_is_resolved_by_filesystem_not_lexical_normalization() {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "owned-current-path-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(directory.join("target/sub")).unwrap();
        std::fs::write(directory.join("guide.xml"), "lexical-parent").unwrap();
        std::fs::write(directory.join("target/guide.xml"), "real-parent").unwrap();
        std::os::unix::fs::symlink(directory.join("target/sub"), directory.join("link")).unwrap();
        let context = SourcePathContext::new(directory.clone(), directory.join("media")).unwrap();
        assert_eq!(
            std::fs::read_to_string(context.resolve("link/../guide.xml".into())).unwrap(),
            "real-parent"
        );
        std::fs::remove_dir_all(directory).unwrap();
    }
}
