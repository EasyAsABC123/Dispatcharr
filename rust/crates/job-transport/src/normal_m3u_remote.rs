//! Remote STD M3U normal worker over the same Main M3U slot and owned services.
//! Original Dispatcharr authors retain application/algorithm/protocol credit.
use crate::{
    m3u_result_file::GroupsResultFile,
    milestone_dispatch::Disposition as D,
    normal_revocation::Revoked,
    normal_xc_live::Message,
    scanner_m3u_protocol::Kind,
    transport::{Jobs, M3uPorts},
};
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_coordination::{TaskLockDetailedError, TaskLockError, TaskLocks};
use dispatcharr_ingest_core::{CacheReadLimits, Entry, ValidatedCache};
use dispatcharr_store::{xc_live_tasks::Prepared, AuthService, Store, StoreError};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, Semaphore};
type Owner = Arc<dyn Send + Sync>;
struct Inner {
    sd: std::sync::OnceLock<crate::normal_sd_guide::Capability>,
    flows: crate::auto_sync_flow::ActiveFlows,
    jobs: Jobs,
    ports: M3uPorts,
    store: Store,
    auth: AuthService,
    events: Arc<dyn UpdateEventSink>,
    locks: TaskLocks,
    revoked: Revoked,
    slots: Arc<Semaphore>,
    cache: PathBuf,
    auto_jobs: Option<Arc<dyn dispatcharr_store::recording_http::RecordingJobs>>,
    closed: AtomicBool,
    uncertain: Mutex<Vec<Owner>>,
}
#[derive(Clone)]
pub(crate) struct Worker {
    inner: Arc<Inner>,
}
struct Guard {
    inner: Arc<Inner>,
    owner: Owner,
    armed: bool,
}
impl Drop for Guard {
    fn drop(&mut self) {
        if self.armed {
            let mut owners = self
                .inner
                .uncertain
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if !owners.iter().any(|o| Arc::ptr_eq(o, &self.owner)) {
                owners.push(self.owner.clone())
            }
        }
    }
}
enum Outcome {
    Value(Value),
    File(GroupsResultFile),
}
impl Worker {
    pub(crate) fn bind_sd(
        &self,
        capability: crate::normal_sd_guide::Capability,
    ) -> Result<(), crate::milestone_dispatch::Disposition> {
        if !capability.owns(&self.inner.jobs) {
            return Err(crate::milestone_dispatch::Disposition::UnsupportedDomain);
        }
        self.inner
            .sd
            .set(capability)
            .map_err(|_| crate::milestone_dispatch::Disposition::UnsupportedDomain)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        jobs: Jobs,
        ports: M3uPorts,
        store: Store,
        auth: AuthService,
        events: Arc<dyn UpdateEventSink>,
        locks: TaskLocks,
        revoked: Revoked,
        slots: Arc<Semaphore>,
        cache: PathBuf,
        auto_jobs: Option<Arc<dyn dispatcharr_store::recording_http::RecordingJobs>>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                sd: std::sync::OnceLock::new(),
                flows: crate::auto_sync_flow::ActiveFlows::default(),
                jobs,
                ports,
                store,
                auth,
                events,
                locks,
                revoked,
                slots,
                cache,
                auto_jobs,
                closed: AtomicBool::new(false),
                uncertain: Mutex::new(Vec::with_capacity(1)),
            }),
        }
    }
    pub(crate) fn stop(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }
    pub(crate) fn begin_drain(&self, budget: Duration) {
        self.inner.flows.begin_drain(budget);
    }
    pub(crate) async fn shutdown(&self, timeout: Duration) -> bool {
        self.stop();
        self.begin_drain(timeout);
        tokio::time::timeout(timeout, self.inner.slots.clone().acquire_owned())
            .await
            .is_ok_and(|r| r.is_ok())
    }
    pub(crate) async fn run(
        &self,
        expected: Option<Arc<[u8]>>,
        caller: Owner,
    ) -> Result<Option<D>, D> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(D::Operational("Remote M3U intake stopped".into()));
        }
        let slot = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| D::Capacity)?;
        let owner: Owner = Arc::new((slot, caller, self.inner.clone()));
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let mut guard = Guard {
                inner: inner.clone(),
                owner: owner.clone(),
                armed: true,
            };
            let result = async {
                let candidate = match inner
                    .jobs
                    .peek_xc_live(expected.as_deref(), owner.clone())
                    .await
                {
                    Ok(Some(v)) => v,
                    Ok(None) => return Ok(Some(D::Empty)),
                    Err(crate::transport::Error::ClaimRace) => return Ok(Some(D::HeadChanged)),
                    Err(crate::transport::Error::UnsupportedTask) => {
                        return Err(D::UnsupportedHead)
                    }
                    Err(_) => return Err(fault("head read")),
                };
                let admission = if inner.auto_jobs.is_some() {
                    dispatcharr_store::xc_live_tasks::prepare_std_remote_with_auto(
                        inner.store.clone(),
                        inner.auth.clone(),
                        candidate.message.account,
                        owner.clone(),
                        inner.events.clone(),
                    )
                    .await
                } else {
                    dispatcharr_store::xc_live_tasks::prepare_std_remote(
                        inner.store.clone(),
                        inner.auth.clone(),
                        candidate.message.account,
                        owner.clone(),
                        inner.events.clone(),
                    )
                    .await
                };
                let prepared = match admission {
                    Ok(Some(p)) => Arc::new(p),
                    Ok(None) => return Ok(None),
                    Err(StoreError::StatusBody(503, _)) => return Ok(Some(D::Capacity)),
                    Err(StoreError::Unsupported(_)) => return Err(D::UnsupportedDomain),
                    Err(_) => return Err(fault("SQL admission")),
                };
                if prepared.missing_input() {
                    if candidate.message.kind != Kind::Single {
                        prepared
                            .close()
                            .await
                            .map_err(|_| fault("missing input Groups admission close"))?;
                        return Err(D::UnsupportedDomain);
                    }
                    if let Err(error) = admit_missing_input_cache(
                        inner.cache.clone(),
                        candidate.message.account,
                        owner.clone(),
                    )
                    .await
                    {
                        prepared
                            .close()
                            .await
                            .map_err(|_| fault("missing input cache admission close"))?;
                        return Err(error);
                    }
                }
                if inner.closed.load(Ordering::Acquire) {
                    prepared.close().await.map_err(|_| fault("SQL close"))?;
                    return Err(fault("admission stopped"));
                }
                let message = match inner.jobs.claim_xc_live(candidate, owner.clone()).await {
                    Ok(v) => v,
                    Err(crate::transport::Error::ClaimRace) => {
                        prepared.close().await.map_err(|_| fault("SQL close"))?;
                        return Ok(Some(D::HeadChanged));
                    }
                    Err(_) => {
                        let _ = prepared.close().await;
                        return Err(fault("claim outcome"));
                    }
                };
                if inner
                    .revoked
                    .contains(&message.id.to_string())
                    .map_err(|_| fault("revoked state"))?
                {
                    prepared
                        .close()
                        .await
                        .map_err(|_| fault("revoked SQL close"))?;
                    if !inner
                        .jobs
                        .write_xc_live_revoked(&message, owner.clone())
                        .await
                        .map_err(|_| fault("revoked backend"))?
                    {
                        return Err(fault("revoked backend refused"));
                    }
                    return Ok(Some(D::Revoked(message.id)));
                }
                let key = if message.kind == Kind::Single {
                    "refresh_single_m3u_account"
                } else {
                    "refresh_m3u_account_groups"
                };
                let lease = match inner
                    .locks
                    .acquire_retained_detailed(key, message.account, owner.clone())
                    .await
                {
                    Ok(v) => Some(v),
                    Err(TaskLockDetailedError::Operational(TaskLockError::Held)) => None,
                    Err(_) => {
                        let _ = prepared.close().await;
                        return Err(fault("task lock"));
                    }
                };
                let flow = if message.kind == Kind::Single && inner.auto_jobs.is_some() {
                    let stop = inner.clone();
                    Some(
                        inner.flows.track(
                            crate::auto_sync_flow::Flow::new(
                                inner.jobs.clone(),
                                inner.auth.producer_identity_source(),
                                inner.cache.clone(),
                                message.id,
                                message.parent,
                                message.root,
                                owner.clone(),
                                Arc::new(move || stop.closed.load(Ordering::Acquire)),
                            )
                            .with_sd(inner.sd.get().cloned()),
                        ),
                    )
                } else {
                    None
                };
                let body = if lease.is_none() {
                    Ok(Outcome::Value(if message.kind == Kind::Groups {
                        json!([
                            format!("Task already running for account_id={}.", message.account),
                            null
                        ])
                    } else {
                        json!(format!(
                            "Task already running for account_id={}.",
                            message.account
                        ))
                    }))
                } else {
                    execute(&inner, &prepared, &message, owner.clone(), flow.clone()).await
                };
                if let Some(flow) = &flow {
                    flow.observe(if body.is_ok() {
                        "import_body_complete"
                    } else {
                        "import_body_error"
                    });
                }
                let regex_closed = if let Some(flow) = &flow {
                    flow.retire(Duration::from_secs(5)).await.is_ok()
                } else {
                    true
                };
                if let Some(flow) = &flow {
                    flow.observe("sql_close_begin");
                }
                let closed = prepared.close().await;
                if let Some(flow) = &flow {
                    flow.observe(if closed.is_ok() {
                        "sql_close_complete"
                    } else {
                        "sql_close_error"
                    });
                }
                let released = if let Some(lease) = lease {
                    lease.release_owned_detailed().await.is_ok_and(|v| v)
                } else {
                    true
                };
                if closed.is_err() || !released || !regex_closed {
                    return Err(fault("physical SQL/token drain"));
                }
                let ack = match body? {
                    Outcome::Value(value) => {
                        if let Some(flow) = &flow {
                            flow.complete(value).await
                        } else {
                            inner
                                .jobs
                                .write_xc_live(&message, value, Vec::new(), owner.clone())
                                .await
                        }
                    }
                    Outcome::File(file) => {
                        inner
                            .jobs
                            .write_m3u_full_result(
                                message.id,
                                message.parent,
                                Vec::new(),
                                file,
                                owner.clone(),
                            )
                            .await
                    }
                }
                .map_err(|_| fault("result publication"))?;
                Ok(Some(if ack {
                    D::Completed(message.id)
                } else {
                    D::CompletedWithoutId
                }))
            }
            .await;
            if result.is_ok() {
                guard.armed = false
            }
            result
        })
        .await
        .map_err(|_| fault("supervisor lost"))?
    }
}
fn fault(stage: &str) -> D {
    D::Operational(format!("Remote M3U {stage} unsettled"))
}
fn store_error(error: StoreError, task: uuid::Uuid) -> D {
    static ERROR_RECORDS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    // One fixed-category record per failing operation; never print SQL messages,
    // arbitrary Debug values, credentials, URLs or request/result payloads.
    if std::env::var_os("RUST_AUTO_STAGE_DIAGNOSTICS").as_deref() == Some(std::ffi::OsStr::new("1"))
        && ERROR_RECORDS.fetch_add(1, Ordering::Relaxed) < 256
    {
        let code = match &error {
            StoreError::Database(e) => e
                .as_database_error()
                .and_then(|e| e.code())
                .filter(|c| c.len() == 5 && c.bytes().all(|b| b.is_ascii_alphanumeric()))
                .map(|c| c.into_owned()),
            _ => None,
        };
        let variant = match &error {
            StoreError::Database(_) => "Database",
            StoreError::Unauthorized(_) => "Unauthorized",
            StoreError::AuthenticationBody(_) => "AuthenticationBody",
            StoreError::Forbidden => "Forbidden",
            StoreError::PermissionDenied => "PermissionDenied",
            StoreError::UiPermissionFault => "UiPermissionFault",
            StoreError::UnhandledRestFault => "UnhandledRestFault",
            StoreError::NotFound => "NotFound",
            StoreError::Unsupported(_) => "Unsupported",
            StoreError::RateLimited(_) => "RateLimited",
            StoreError::Throttled { .. } => "Throttled",
            StoreError::XcDenied { .. } => "XcDenied",
            StoreError::InvalidInput(_) => "InvalidInput",
            StoreError::StatusBody(..) => "StatusBody",
        };
        eprintln!(
            "rust_auto_error task={task} variant={variant} sqlstate={}",
            code.as_deref().unwrap_or("none")
        );
    }
    match error {
        StoreError::Unsupported(_) => D::UnsupportedDomain,
        _ => fault("SQL/event effects"),
    }
}
async fn execute(
    inner: &Inner,
    prepared: &Arc<Prepared>,
    message: &Message,
    owner: Owner,
    flow: Option<Arc<crate::auto_sync_flow::Flow>>,
) -> Result<Outcome, D> {
    let outcome = execute_body(inner, prepared, message, owner.clone(), flow).await;
    if message.kind == Kind::Single {
        let path = inner.cache.join(format!("{}.json", message.account));
        let retired = tokio::task::spawn_blocking(move || {
            let _owner = owner;
            // Original's finally catches all actual OSError outcomes. A known
            // deletion refusal is not an unsettled worker or fabricated delete.
            std::fs::remove_file(path)
        })
        .await;
        if retired.is_err() {
            return Err(fault("parsed cache retirement"));
        }
    }
    outcome
}
async fn execute_body(
    inner: &Inner,
    prepared: &Arc<Prepared>,
    message: &Message,
    owner: Owner,
    flow: Option<Arc<crate::auto_sync_flow::Flow>>,
) -> Result<Outcome, D> {
    let start = chrono::Utc::now();
    let elapsed = Instant::now();
    let parsed = inner.cache.join(format!("{}.json", message.account));
    if message.kind == Kind::Single {
        prepared
            .begin_single()
            .await
            .map_err(|error| store_error(error, message.id))?;
    }
    let cached = if message.kind == Kind::Single {
        probe(parsed.clone(), owner.clone()).await?
    } else {
        None
    };
    let entries = if let Some(entries) = cached.filter(|n| *n > 0) {
        entries
    } else {
        let group_lease = if message.kind == Kind::Single {
            match inner
                .locks
                .acquire_retained_detailed(
                    "refresh_m3u_account_groups",
                    message.account,
                    owner.clone(),
                )
                .await
            {
                Ok(v) => Some(v),
                Err(TaskLockDetailedError::Operational(TaskLockError::Held)) => {
                    return Err(D::UnsupportedDomain)
                }
                Err(_) => return Err(fault("nested groups lock")),
            }
        } else {
            None
        };
        // The Source STD missing-input branch returns a caught fetch failure;
        // no HTTP request, parser, fake document or downloader is involved.
        let transfer = if prepared.missing_input() {
            Err(dispatcharr_store::remote_m3u_io::Error::Caught(
                "No M3U source available (missing URL and file)".to_owned(),
            ))
        } else {
            dispatcharr_store::remote_m3u_io::transfer(
                prepared.remote_input(inner.cache.clone(), message.use_cache),
                prepared.clone(),
                owner.clone(),
            )
            .await
        };
        let downloaded = match transfer {
            Ok(v) => v,
            Err(error) => {
                use dispatcharr_store::remote_m3u_io::Error as TransferError;
                let (message_text, groups_result) = match error {
                    TransferError::Caught(message) => (message, None),
                    TransferError::DownloadFailed {
                        message,
                        groups_result,
                    } => (message, Some(groups_result)),
                    _ => {
                        if let Some(lease) = group_lease {
                            let _ = lease.release_owned_detailed().await;
                        }
                        return Err(fault("HTTP/file transfer"));
                    }
                };
                prepared
                    .caught_remote_error(&message_text, "downloading")
                    .await
                    .map_err(|error| store_error(error, message.id))?;
                if let Some(lease) = group_lease {
                    if !lease.release_owned_detailed().await.is_ok_and(|v| v) {
                        return Err(fault("groups token release"));
                    }
                }
                return Ok(Outcome::Value(if message.kind == Kind::Single {
                    json!("Failed to update m3u account - download failed or other error")
                } else {
                    json!([groups_result.unwrap_or(message_text), null])
                }));
            }
        };
        prepared
            .import_std_groups(
                downloaded.catalog.groups,
                start,
                message.kind == Kind::Single || message.full,
            )
            .await
            .map_err(|error| store_error(error, message.id))?;
        if let Some(lease) = group_lease {
            if !lease.release_owned_detailed().await.is_ok_and(|v| v) {
                return Err(fault("groups token release"));
            }
        }
        downloaded.catalog.entries
    };
    if message.kind == Kind::Groups {
        let path = parsed;
        let directory = inner.cache.clone();
        let retained = owner;
        let file = tokio::task::spawn_blocking(move || {
            GroupsResultFile::from_actual_cache(&path, &directory, retained)
        })
        .await
        .map_err(|_| fault("result file supervisor"))?
        .map_err(|_| fault("result file"))?;
        return Ok(Outcome::File(file));
    }
    if entries == 0 {
        return Ok(Outcome::Value(
            prepared
                .empty_std()
                .await
                .map_err(|error| store_error(error, message.id))?
                .result,
        ));
    }
    prepared
        .parsing_only()
        .await
        .map_err(|error| store_error(error, message.id))?;
    let groups = prepared
        .std_groups()
        .await
        .map_err(|error| store_error(error, message.id))?;
    let (sender, mut receiver) = mpsc::channel(1);
    let retained = owner.clone();
    let actor = tokio::task::spawn_blocking(move || produce_batches(parsed, sender, retained));
    let mut total = dispatcharr_store::ImportCounts::default();
    let mut failed = 0usize;
    let mut import_error = None;
    while let Some(batch) = receiver.recv().await {
        match prepared.std_batch(batch, &groups, &mut total).await {
            Ok(true) => {}
            Ok(false) => failed += 1,
            Err(error) => {
                // A SQL error must not drop the parser's JoinHandle and then
                // retire its still-open cache. Closing the receiver interrupts
                // its bounded blocking_send; join before releasing either FD.
                import_error = Some(store_error(error, message.id));
                receiver.close();
                break;
            }
        }
    }
    drop(receiver);
    let parsed = actor.await.map_err(|_| fault("parser supervisor"))?;
    if let Some(error) = import_error {
        // The producer can report the expected closed consumer here. Its actual
        // physical return is proven above; preserve the original SQL outcome.
        return Err(error);
    }
    parsed.map_err(|_| fault("cache parse"))?;
    let completed = if failed > 0 {
        prepared.finish_std_failure(failed).await
    } else if let (Some(jobs), Some(flow)) = (&inner.auto_jobs, &flow) {
        prepared
            .finish_import_with_auto(total, start, elapsed, flow.services(jobs.clone()))
            .await
    } else {
        prepared.finish_import(total, start, elapsed).await
    }
    .map_err(|error| store_error(error, message.id))?;
    if completed.invalidate {
        inner
            .ports
            .invalidate(owner)
            .await
            .map_err(|_| fault("output cache invalidation"))?;
    }
    Ok(Outcome::Value(completed.result))
}
async fn probe(path: PathBuf, owner: Owner) -> Result<Option<u64>, D> {
    tokio::task::spawn_blocking(move || {
        let _owner = owner;
        match std::fs::symlink_metadata(&path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(_) => Err(fault("cache metadata")),
            Ok(meta) if !meta.is_file() || meta.file_type().is_symlink() => {
                Err(D::UnsupportedDomain)
            }
            Ok(_) => match open_cache(&path) {
                Ok(cache) => Ok(Some(cache.entries)),
                Err(dispatcharr_ingest_core::CacheReadError::Json(error))
                    if error.is_syntax() || error.is_eof() =>
                {
                    // Source retains a malformed JSON cache as .corrupted and
                    // then downloads again. Actual rename refusal is caught.
                    let mut backup = path.as_os_str().to_os_string();
                    backup.push(".corrupted");
                    let _ = std::fs::rename(&path, PathBuf::from(backup));
                    Ok(None)
                }
                Err(dispatcharr_ingest_core::CacheReadError::Io(_)) => Ok(None),
                // The bounded decoder's capacity/schema domain is not a
                // provider-empty result and must not trigger destructive import.
                Err(_) => Err(D::UnsupportedDomain),
            },
        }
    })
    .await
    .map_err(|_| fault("cache probe supervisor"))?
}
fn produce_batches(
    path: PathBuf,
    sender: mpsc::Sender<Vec<Entry>>,
    owner: Owner,
) -> Result<(), ()> {
    let _owner = owner;
    let mut cache = open_cache(&path).map_err(|_| ())?;
    let mut rows = Vec::with_capacity(1000);
    let mut bytes = 0usize;
    cache
        .for_each(|entry| {
            let encoded = serde_json::to_vec(&entry).map_err(|_| "entry byte accounting")?;
            bytes = bytes
                .checked_add(encoded.len())
                .filter(|n| *n <= dispatcharr_store::MAX_IMPORT_BATCH_BYTES)
                .ok_or("logical batch capacity")?;
            rows.push(entry);
            if rows.len() == 1000 {
                sender
                    .blocking_send(std::mem::take(&mut rows))
                    .map_err(|_| "closed batch consumer")?;
                bytes = 0;
            }
            Ok(())
        })
        .map_err(|_| ())?;
    if !rows.is_empty() {
        sender.blocking_send(rows).map_err(|_| ())?;
    }
    Ok(())
}

fn open_cache(
    path: &std::path::Path,
) -> Result<ValidatedCache, dispatcharr_ingest_core::CacheReadError> {
    use dispatcharr_ingest_core::CacheReadError;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(CacheReadError::Capacity);
    }
    ValidatedCache::from_file(file, CacheReadLimits::default())
}

/// Shared with aggregate admission: Source missing-input error slice is cold
/// parsed-cache Single only. Observe the actual configured directory/file under
/// the same owner; no file is created, removed or renamed during admission.
pub(crate) async fn admit_missing_input_cache(
    directory: std::path::PathBuf,
    account: i64,
    owner: Owner,
) -> Result<(), D> {
    let supported=tokio::task::spawn_blocking(move || {
        let _owner=owner;
        if !std::fs::metadata(&directory).is_ok_and(|metadata|metadata.is_dir()){return false;}
        matches!(std::fs::symlink_metadata(directory.join(format!("{account}.json"))),Err(error) if error.kind()==std::io::ErrorKind::NotFound)
    }).await.map_err(|_|fault("missing input cache observation unsettled"))?;
    if supported {
        Ok(())
    } else {
        Err(D::UnsupportedDomain)
    }
}
