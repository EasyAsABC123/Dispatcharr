//! Additive normal DVR handler; captured constructors remain unchanged.
//! Original Dispatcharr authors retain algorithm/application credit.
//! Automatic postprocessing extension remains private pending Source characterization.
use crate::{
    normal_named_recording::NormalNamedRecordingPublications,
    normal_recording_publication::{NormalRecordingPublications, NormalRecordingPublisher},
    normal_revocation::Revoked,
    recording_active, recording_cleanup, recording_events, recording_files, recording_normal,
    recording_normal_capture, recording_normal_policy, recording_normal_task, recording_process,
    recording_settings, recording_stop_handoff,
    transport::{Config, Jobs},
};
pub use crate::{recording_normal_task::TaskKey, recording_stop_handoff::LocalRecordingStop};
use chrono::Utc;
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_store::{
    recording_capture::{CaptureStore, NormalEvent},
    Store,
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, OnceLock,
};
use std::time::Duration;
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

#[derive(Debug)]
pub enum Error {
    Closed,
    Capacity,
    NotDue,
    Unsupported(&'static str),
    Supervisor,
    Unsettled,
    Store(dispatcharr_store::recording_capture::Error),
    Settings(recording_settings::Error),
    Active(recording_active::Error),
    Files(recording_files::Error),
    Media(recording_process::Error),
    Events(recording_events::Error),
    Transport(crate::transport::Error),
    Publication(crate::normal_recording_publication::Error),
    Cleanup(Box<dispatcharr_store::recording_beat_cleanup::Error>),
}
pub struct Assembly {
    pub store: Store,
    pub broker: String,
    pub result: String,
    pub cache: String,
    pub events: Arc<dyn UpdateEventSink>,
    pub explicit_internal_ts_base: String,
    pub configured_plugin_directory: String,
    pub media_uid: u32,
    pub publisher: NormalRecordingPublisher,
    pub comskip: crate::normal_comskip::Prepared,
    pub revoked: Revoked,
    pub control: Arc<crate::normal_control::Control>,
    pub publications: NormalRecordingPublications,
    pub named_publications: NormalNamedRecordingPublications,
}
pub enum Run {
    Empty,
    UnsupportedHead,
    Revoked {
        task: recording_normal_task::TaskKey,
        metadata_written: bool,
    },
    CompletedNoop {
        task: recording_normal_task::TaskKey,
        metadata_written: bool,
    },
    Completed {
        task: recording_normal_task::TaskKey,
        metadata_written: bool,
        bytes_written: u64,
        file_size: Option<u64>,
        processes: Vec<Value>,
        active_observations: Vec<Value>,
    },
}
struct Owner {
    _caller: Arc<dyn Send + Sync>,
    _slot: OwnedSemaphorePermit,
    trace: Arc<Mutex<Vec<Value>>>,
    expected: Option<Arc<[u8]>>,
}
struct Receipt {
    done: Option<oneshot::Sender<()>>,
    _owner: Arc<dyn Send + Sync>,
}
impl Drop for Receipt {
    fn drop(&mut self) {
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
    }
}
type UncertainOwner = (Arc<Owner>, Arc<Inner>, Arc<recording_process::Transcript>);
struct Inner {
    jobs: Jobs,
    store: Arc<CaptureStore>,
    settings: recording_settings::Settings,
    active: recording_active::Operations,
    events: Arc<dyn UpdateEventSink>,
    explicit_origin: String,
    plugin_directory: std::path::PathBuf,
    plugin_domain: Arc<plugin_domain::PluginDomain>,
    media_uid: u32,
    publisher: NormalRecordingPublisher,
    comskip: crate::normal_comskip::Prepared,
    revoked: Revoked,
    control: Arc<crate::normal_control::Control>,
    publications: NormalRecordingPublications,
    named_publications: NormalNamedRecordingPublications,
    head_advance: Arc<tokio::sync::Notify>,
    cleanup: recording_cleanup::Component,
    slot: Arc<Semaphore>,
    closed: AtomicBool,
    shutdown: Arc<AtomicBool>,
    local_stop: OnceLock<recording_stop_handoff::Registry>,
    // At most20 admitted tasks retain their own unknown post-claim scopes. No
    // generic error->terminal metadata, lease cleanup, requeue or automatic retry.
    uncertain: Mutex<Vec<UncertainOwner>>,
    transcript: Mutex<Option<Arc<recording_process::Transcript>>>,
    active_trace: Mutex<Vec<Value>>,
}
#[derive(Clone)]
pub struct NormalDvr {
    inner: Arc<Inner>,
}
pub(crate) async fn transport<T, F, Fut>(
    owner: Arc<dyn Send + Sync>,
    operation: F,
) -> Result<T, Error>
where
    F: FnOnce(Arc<dyn Send + Sync>) -> Fut,
    Fut: std::future::Future<Output = Result<T, crate::transport::Error>>,
{
    let (tx, rx) = oneshot::channel();
    let receipt = Arc::new(Receipt {
        done: Some(tx),
        _owner: owner,
    });
    let result = operation(receipt.clone()).await;
    drop(receipt);
    rx.await.map_err(|_| Error::Unsettled)?;
    result.map_err(Error::Transport)
}
#[path = "recording_dvr_plugin_domain.rs"]
mod plugin_domain;

impl NormalDvr {
    pub async fn connect(assembly: Assembly, owner: Arc<dyn Send + Sync>) -> Result<Self, Error> {
        tokio::spawn(async move {
            let _owner = owner.clone();
            // Actual Source configuration, not an observation-fixture path.
            // Nonempty plugin domains still require real hook execution ports.
            let plugin_domain = Arc::new(plugin_domain::PluginDomain::prepare(
                &assembly.configured_plugin_directory,
            )?);
            let plugin_directory = plugin_domain.path().to_owned();

            recording_process::ObservedLocalOrigin::from_normal_internal_base(
                &assembly.explicit_internal_ts_base,
            )
            .map_err(Error::Media)?;
            let jobs =
                Jobs::connect_recording_dvr(&assembly.broker, &assembly.result, Config::default())
                    .await
                    .map_err(Error::Transport)?;
            let active =
                recording_active::Operations::connect_normal(assembly.cache.clone(), owner)
                    .await
                    .map_err(Error::Active)?;
            let store = Arc::new(CaptureStore::normal(assembly.store.pool().clone()));
            let cleanup = recording_cleanup::Component::normal(assembly.store.pool().clone());
            if !assembly.comskip.uses_store(&assembly.store) {
                return Err(Error::Unsupported(
                    "Comskip and normal DVR must share Store",
                ));
            }
            let settings = recording_settings::Settings::new(assembly.store, assembly.cache);
            Ok(Self {
                inner: Arc::new(Inner {
                    jobs,
                    store,
                    settings,
                    active,
                    events: assembly.events,
                    explicit_origin: assembly.explicit_internal_ts_base,
                    plugin_directory,
                    plugin_domain,
                    media_uid: assembly.media_uid,
                    publisher: assembly.publisher,
                    comskip: assembly.comskip,
                    revoked: assembly.revoked,
                    control: assembly.control,
                    publications: assembly.publications,
                    named_publications: assembly.named_publications,
                    head_advance: Arc::new(tokio::sync::Notify::new()),
                    cleanup,
                    slot: Arc::new(Semaphore::new(20)),
                    closed: AtomicBool::new(false),
                    shutdown: Arc::new(AtomicBool::new(false)),
                    local_stop: OnceLock::new(),
                    uncertain: Mutex::new(Vec::with_capacity(20)),
                    transcript: Mutex::new(None),
                    active_trace: Mutex::new(Vec::new()),
                }),
            })
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    pub async fn run_once(&self, caller: Arc<dyn Send + Sync>) -> Result<Run, Error> {
        self.run_expected(caller, None).await
    }
    async fn run_expected(
        &self,
        caller: Arc<dyn Send + Sync>,
        expected: Option<Arc<[u8]>>,
    ) -> Result<Run, Error> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let slot = self
            .inner
            .slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let owner = Arc::new(Owner {
            _caller: caller,
            _slot: slot,
            trace: Arc::new(Mutex::new(Vec::with_capacity(16))),
            expected,
        });
        let inner = self.inner.clone();
        // Observer loss never cancels queued work or its original physical owner.
        tokio::spawn(async move {
            let claimed = AtomicBool::new(false);
            let transcript = Arc::new(recording_process::Transcript::new_normal());
            *inner.transcript.lock().map_err(|_| Error::Unsettled)? = Some(transcript.clone());
            inner
                .active_trace
                .lock()
                .map_err(|_| Error::Unsettled)?
                .clear();
            let result = execute(inner.clone(), owner.clone(), &claimed, transcript.clone()).await;
            if result.is_err() && claimed.load(Ordering::Acquire) {
                inner.closed.store(true, Ordering::Release);
                let mut uncertain = match inner.uncertain.lock() {
                    Ok(value) => value,
                    Err(error) => error.into_inner(),
                };
                uncertain.push((owner, inner.clone(), transcript));
            }
            result
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    /// Explicit worker intent only. No task revocation/stop-result authority is
    /// inferred. Unknown post-claim scopes remain pinned, not fake drained.
    pub fn request_shutdown(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.shutdown.store(true, Ordering::Release);
        if let Some(registry) = self.inner.local_stop.get() {
            registry.request_shutdown();
        }
    }
    /// Bind only before starting the in-process AIO Recording worker and HTTP
    /// router which share this instance. Remote workers cannot register here.
    pub fn enable_local_stop_handoff(&self) {
        self.inner
            .local_stop
            .get_or_init(recording_stop_handoff::Registry::default);
    }
    pub fn has_local_stop_handoff(&self) -> bool {
        self.inner.local_stop.get().is_some()
    }
    pub fn begin_local_recording_stop(
        &self,
        channel: uuid::Uuid,
        recording: i64,
    ) -> Option<LocalRecordingStop> {
        self.inner.local_stop.get()?.stop(channel, recording)
    }
    pub fn recording_stop_snapshot_is_current(&self, request: &LocalRecordingStop) -> bool {
        self.inner
            .local_stop
            .get()
            .is_some_and(|registry| registry.still_current(request))
    }
    pub fn require_recording_domain(&self) -> Result<(), Error> {
        self.inner.plugin_domain.require_supported()
    }
    pub fn bind_recording_connect(
        &self,
        auth: dispatcharr_store::AuthService,
        runtime: dispatcharr_store::PluginRuntime,
    ) -> Result<(), Error> {
        self.inner
            .store
            .bind_recording_connect(auth, runtime)
            .map_err(Error::Store)
    }
    pub fn plugin_domain_capability(&self) -> &'static str {
        self.inner.plugin_domain.last_observed_capability()
    }
    pub fn available_slots(&self) -> usize {
        self.inner.slot.available_permits()
    }
    pub async fn drain(&self, timeout: Duration) -> Result<(), Error> {
        // Graceful close stops intake, not the normal recording's observer/body.
        self.inner.closed.store(true, Ordering::Release);
        let deadline = tokio::time::Instant::now() + timeout;
        while self.available_slots() != 20 {
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Unsettled);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        self.inner
            .jobs
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .map_err(Error::Transport)
    }
    pub fn diagnostics(&self) -> Value {
        let process=self.retained_transcript().map(|value|{let(rows,poisoned,pending)=value.snapshot();json!({"rows":rows.iter().map(|row|row.receipt()).collect::<Vec<_>>(),"poisoned":poisoned,"unresolved_wait_owner":pending})});
        let active = match self.inner.active_trace.lock() {
            Ok(value) => value.clone(),
            Err(error) => error.into_inner().clone(),
        };
        json!({"closed":self.inner.closed.load(Ordering::Acquire),"available_slots":self.available_slots(),"processes":process,"active_observations":active})
    }
    pub(crate) fn retained_transcript(&self) -> Option<Arc<recording_process::Transcript>> {
        match self.inner.transcript.lock() {
            Ok(value) => value.clone(),
            Err(error) => error.into_inner().clone(),
        }
    }
}
fn policy_error(error: recording_normal_policy::Error) -> Error {
    match error {
        recording_normal_policy::Error::Unsupported(message) => Error::Unsupported(message),
        recording_normal_policy::Error::PythonFormat => {
            Error::Unsupported("normal Python formatting error")
        }
    }
}
fn trace(owner: &Owner, kind: &str, observation: Value) -> Result<(), Error> {
    let mut log = owner.trace.lock().map_err(|_| Error::Unsettled)?;
    // Diagnostic residency stays constant across arbitrary recording length.
    if log.len() == 16 {
        log.remove(0);
    }
    log.push(json!({"kind":kind,"observation":observation}));
    Ok(())
}
fn due(message: &recording_normal_task::Message) -> Result<(), Error> {
    let now = Utc::now();
    if message.start() <= now {
        Ok(())
    } else {
        Err(Error::NotDue)
    }
}
#[allow(clippy::too_many_arguments)]
async fn emit_recording_event(
    inner: &Arc<Inner>,
    task: &recording_normal_task::Task,
    kind: NormalEvent,
    channel: uuid::Uuid,
    name: String,
    details: Value,
    maximum: i64,
    owner: Arc<Owner>,
) -> Result<(), Error> {
    // Recheck the actual pinned empty directory at each event boundary too.
    inner.plugin_domain.require_supported()?;
    if inner.store.recording_connect_bound() {
        let witness = task.event_context(owner.clone())?;
        inner
            .store
            .clone()
            .insert_claimed_runtime_event(
                kind,
                channel,
                name,
                details,
                maximum,
                witness.context(),
                witness.trail(),
                owner,
            )
            .await
            .map_err(Error::Store)?;
    } else {
        inner
            .store
            .clone()
            .insert_runtime_event(kind, channel, name, details, maximum, owner)
            .await
            .map_err(Error::Store)?;
    }
    Ok(())
}
/// Source finalization exceptions continue to automatic postprocessing. The
/// later explicit deleted-row exists() return still suppresses publication.
async fn automatic_comskip_children(
    inner: &Arc<Inner>,
    message: &recording_normal_task::Message,
    owner: Arc<Owner>,
) -> Result<Vec<uuid::Uuid>, Error> {
    let recording = message.recording_id();
    let mut children = Vec::new();
    match inner.settings.normal_effect(false, owner.clone()).await {
        Ok(settings) => {
            if settings.get("comskip_enabled") == Some(&Value::Bool(true)) {
                match inner
                    .comskip
                    .publish_after_recording(
                        recording,
                        message.raw_key(),
                        message.root_key(),
                        owner.clone(),
                    )
                    .await
                {
                    Ok(child) => children.push(child),
                    Err(_) => trace(
                        &owner,
                        "caught-comskip-publication",
                        json!({"published_reply_observed":false,"retried":false}),
                    )?,
                }
            }
        }
        Err(recording_settings::Error::Unsettled) => return Err(Error::Unsettled),
        Err(_) => trace(&owner, "caught-comskip-settings", json!({"retried":false}))?,
    }
    Ok(children)
}
async fn execute(
    inner: Arc<Inner>,
    owner: Arc<Owner>,
    claimed: &AtomicBool,
    transcript: Arc<recording_process::Transcript>,
) -> Result<Run, Error> {
    // Inspect only the authenticated broker bytes before the domain guard.
    // Full normal-task adoption may read SQL/publication state and must follow it.
    let expected = match owner.expected.clone() {
        Some(raw) => Some(raw),
        None => match inner
            .publisher
            .peek_existing(owner.clone())
            .await
            .map_err(Error::Publication)?
        {
            Some(raw) => Some(raw),
            None => return Ok(Run::Empty),
        },
    };
    inner.plugin_domain.require_supported()?;
    let mut task = match recording_normal_task::peek(
        inner.jobs.clone(),
        &inner.publisher,
        &inner.publications,
        &inner.named_publications,
        expected,
        owner.clone(),
    )
    .await
    {
        Ok(Some(task)) => task,
        Ok(None) => return Ok(Run::Empty),
        Err(Error::Transport(crate::transport::Error::UnsupportedTask)) => {
            return Ok(Run::UnsupportedHead)
        }
        Err(error) => return Err(error),
    };
    let message = task.message.clone();
    // Source Celery consumes a revoked task and writes REVOKED before any
    // recording SQL/media effects. Exact queue CAS remains the only claim.
    if inner
        .revoked
        .contains(&message.raw_key())
        .map_err(|_| Error::Unsettled)?
    {
        claimed.store(true, Ordering::Release);
        if let Err(error) = task.claim(inner.jobs.clone(), owner.clone()).await {
            if matches!(error, Error::Transport(crate::transport::Error::ClaimRace)) {
                claimed.store(false, Ordering::Release);
            }
            return Err(error);
        }
        inner.head_advance.notify_one();
        let metadata_written = message
            .clone()
            .write_revoked(inner.jobs.clone(), owner)
            .await?;
        task.complete(&inner.publications, &inner.named_publications)?;
        return Ok(Run::Revoked {
            task: message.key(),
            metadata_written,
        });
    }
    let per_trace = owner.trace.clone();
    let snapshot = inner
        .store
        .clone()
        .normal_snapshot(message.recording_id(), message.channel_id(), owner.clone())
        .await
        .map_err(Error::Store)?;
    let snapshot = match snapshot {
        Some(snapshot) => snapshot,
        None => {
            claimed.store(true, Ordering::Release);
            if let Err(error) = task.claim(inner.jobs.clone(), owner.clone()).await {
                if matches!(error, Error::Transport(crate::transport::Error::ClaimRace)) {
                    claimed.store(false, Ordering::Release);
                }
                return Err(error);
            }
            inner.head_advance.notify_one();
            let metadata_written = message.clone().write(inner.jobs.clone(), owner).await?;
            task.complete(&inner.publications, &inner.named_publications)?;
            return Ok(Run::CompletedNoop {
                task: message.key(),
                metadata_written,
            });
        }
    };
    due(&message)?;
    let groups = inner
        .settings
        .admission(owner.clone())
        .await
        .map_err(Error::Settings)?;
    let default_profile = groups
        .stream
        .get("default_stream_profile")
        .filter(|v| !v.is_null())
        .map(|v| {
            v.as_i64()
                .or_else(|| v.as_str()?.parse().ok())
                .ok_or(Error::Unsupported("Python ORM default profile conversion"))
        })
        .transpose()?;
    if inner
        .store
        .clone()
        .normal_requires_provider_ua(message.channel_id(), default_profile, owner.clone())
        .await
        .map_err(Error::Store)?
    {
        return Err(Error::Unsupported(
            "Redirect provider User-Agent adapter required",
        ));
    }
    let policy_snapshot = snapshot.clone();
    let policy_groups = recording_normal::EffectiveGroups {
        dvr: groups.dvr.clone(),
        system: groups.system.clone(),
        stream: groups.stream.clone(),
    };
    let owned = owner.clone();
    let uid = inner.media_uid;
    let plugin_directory = inner.plugin_directory.clone();
    let argument_start = message.start();
    let argument_end = message.end();
    let files = tokio::task::spawn_blocking(move || {
        let _owner = owned.clone();
        let existing =
            recording_normal_policy::resume_paths(&policy_snapshot).map_err(policy_error)?;
        let reuse = existing.is_some();
        let paths = match existing {
            Some(paths) => paths,
            None => recording_normal_policy::paths_for_execution(
                &policy_snapshot,
                &policy_groups,
                argument_start,
                argument_end,
            )
            .map_err(policy_error)?,
        };
        recording_files::Prepared::inspect_normal(paths, uid, owned, reuse, plugin_directory)
            .map_err(Error::Files)
    })
    .await
    .map_err(|_| Error::Supervisor)??;
    let paths = files.normal_paths().clone();
    let before = inner
        .active
        .observe(message.recording_id(), owner.clone())
        .await
        .map_err(Error::Active)?;
    trace(&owner, "before-active", before.receipt(json!(before.value)))?;
    due(&message)?;
    if inner.closed.load(Ordering::Acquire) {
        return Err(Error::Closed);
    }
    // Exact raw candidate from the actual queue, validated by all domain gates.
    // Mark ambiguous before dispatch; only confirmed claim-race can clear it.
    claimed.store(true, Ordering::Release);
    if let Err(error) = task.claim(inner.jobs.clone(), owner.clone()).await {
        if matches!(error, Error::Transport(crate::transport::Error::ClaimRace)) {
            claimed.store(false, Ordering::Release);
        }
        return Err(error);
    }
    inner.head_advance.notify_one();
    let confirmed = match inner
        .active
        .set_once_normal(message.recording_id(), owner.clone())
        .await
        .map_err(Error::Active)?
    {
        recording_active::NormalAcquire::Acquired(value) => value,
        recording_active::NormalAcquire::HeldElsewhere(observed) => {
            trace(
                &owner,
                "existing-active-owner",
                observed.receipt(json!(observed.value)),
            )?;
            let metadata_written = message.clone().write(inner.jobs.clone(), owner).await?;
            task.complete(&inner.publications, &inner.named_publications)?;
            return Ok(Run::CompletedNoop {
                task: message.key(),
                metadata_written,
            });
        }
    };
    let lease = confirmed.observation;
    let mut confirmation = confirmed.confirmation;
    trace(&owner, "acquired", lease.receipt(json!(lease.value)))?;
    if !lease.value {
        return Err(Error::Unsupported("post-admission other active owner"));
    }
    inner
        .cleanup
        .delete_by_name(
            format!("dvr-recording-{}", message.recording_id()),
            owner.clone(),
        )
        .await
        .map_err(|error| Error::Cleanup(Box::new(error)))?;
    // Source re-reads Channel after periodic cleanup. Bounded snapshot checks
    // also refuse a changed manual/metadata context before publishing started.
    let current = inner
        .store
        .clone()
        .normal_snapshot(message.recording_id(), message.channel_id(), owner.clone())
        .await
        .map_err(Error::Store)?;
    let Some(current) = current else {
        let metadata_written = message.clone().write(inner.jobs.clone(), owner).await?;
        task.complete(&inner.publications, &inner.named_publications)?;
        return Ok(Run::CompletedNoop {
            task: message.key(),
            metadata_written,
        });
    };
    // The queued task was claimed and acquired the recording lease. Keep its
    // private generation registered before prime or before any attempt can spawn.
    // HTTP task_id can be a Beat schedule alias rather than this actual task ID.
    let stop_registration = inner
        .local_stop
        .get()
        .map(|registry| {
            registry
                .register(current.channel_uuid, message.recording_id())
                .ok_or(Error::Unsettled)
        })
        .transpose()?;
    recording_events::send(
        inner.events.clone(),
        recording_events::started(&current.channel_name),
        owner.clone(),
    )
    .await
    .map_err(Error::Events)?;
    let max_events = recording_normal_policy::max_events(&groups).map_err(policy_error)?;
    emit_recording_event(
        &inner,
        &task,
        NormalEvent::Started,
        current.channel_uuid,
        current.channel_name.clone(),
        json!({"recording_id":message.recording_id()}),
        max_events,
        owner.clone(),
    )
    .await?;
    if inner
        .settings
        .normal_effect(true, owner.clone())
        .await
        .map_err(Error::Settings)?
        != groups.system
    {
        return Err(Error::Unsupported("system group changed"));
    }
    let started = Utc::now();
    if inner
        .settings
        .normal_effect(false, owner.clone())
        .await
        .map_err(Error::Settings)?
        != groups.dvr
    {
        return Err(Error::Unsupported("DVR group changed"));
    }
    let parent = tokio::task::spawn_blocking(move || files.create_parent())
        .await
        .map_err(|_| Error::Supervisor)?
        .map_err(Error::Files)?;
    let patch = recording_normal::prime_patch(message.recording_id(), &paths, started)
        .map_err(|_| Error::Unsupported("prime metadata"))?;
    use dispatcharr_store::recording_capture::{Error as CaptureError, PrimeWriteOutcome};
    let prime_rejected = match inner
        .store
        .clone()
        .prime_normal_properties(message.recording_id(), patch, owner.clone())
        .await
    {
        Ok(PrimeWriteOutcome::Applied(_)) => false,
        Err(
            error @ CaptureError::Unsupported(
                "prime status changed" | "recording changed during fresh merge",
            ),
        ) if stop_registration.is_some() => {
            let recording = message.recording_id();
            let state = inner
                .store
                .clone()
                .normal_state(recording, owner.clone())
                .await
                .map_err(Error::Store)?;
            if !state.is_some_and(|state| state.stopped) {
                return Err(Error::Store(error));
            }
            // Durable STOP won the prime CAS. No HLS directory or child has
            // been started, and only the original, unexpired literal receipt
            // may retire this task's active scope.
            let before_capture = confirmation
                .take()
                .ok_or(Error::Active(recording_active::Error::Uncertain))?;
            let removed = inner
                .active
                .remove_captured61(recording, before_capture, owner.clone())
                .await
                .map_err(Error::Active)?;
            trace(
                &owner,
                "stopped-before-prime",
                removed.receipt(json!(removed.value)),
            )?;
            let metadata_written = message.clone().write(inner.jobs.clone(), owner).await?;
            task.complete(&inner.publications, &inner.named_publications)?;
            return Ok(Run::CompletedNoop {
                task: message.key(),
                metadata_written,
            });
        }
        Err(error) => return Err(Error::Store(error)),
        Ok(PrimeWriteOutcome::Rejected(rejection)) => {
            trace(
                &owner,
                "settled-prime-rejection",
                json!({"code":rejection.code(),"protocol_fate":"server rejection followed by actual Ready and close"}),
            )?;
            true
        }
    };
    if !prime_rejected {
        recording_events::send(
            inner.events.clone(),
            recording_events::updated(message.recording_id()),
            owner.clone(),
        )
        .await
        .map_err(Error::Events)?;
    }
    let files = Arc::new(
        tokio::task::spawn_blocking(move || parent.start_hls())
            .await
            .map_err(|_| Error::Supervisor)?
            .map_err(Error::Files)?,
    );
    // Source3 actually attempted StreamProfile.get(id=None), caught it, then
    // selected the DVR UA. Positive default/Redirect/provider-UA is not ported.
    if inner
        .settings
        .normal_stream_effect(owner.clone())
        .await
        .map_err(Error::Settings)?
        != groups.stream
    {
        return Err(Error::Unsupported(
            "stream settings changed before captured UA fallback",
        ));
    }
    let recording = message.recording_id();
    let capture = recording_normal_capture::capture(recording_normal_capture::Context {
        store: inner.store.clone(),
        active: inner.active.clone(),
        owner: owner.clone(),
        trace: per_trace.clone(),
        recording,
        channel: current.channel_uuid,
        paths: paths.clone(),
        origin: inner.explicit_origin.clone(),
        output_profile_id: crate::recording_output_profile::capture_id(
            groups.dvr.get("output_profile_id"),
        )
        .map_err(Error::Unsupported)?,
        files: files.clone(),
        end: message.end(),
        shutdown: inner.shutdown.clone(),
        transcript: transcript.clone(),
        confirmation: confirmation.take(),
        stop_generation: stop_registration
            .as_ref()
            .map(|guard| guard.generation().clone()),
    })
    .await?;
    if capture.reason == recording_process::NormalStop::Shutdown && Utc::now() < capture.end {
        inner
            .store
            .clone()
            .finalize_runtime(
                recording,
                dispatcharr_store::recording_capture::RuntimeFinalize {
                    ended: Utc::now(),
                    bytes_written: 0,
                    remux_success: false,
                    interrupted_reason: None,
                    server_shutdown: true,
                    stream_info: None,
                },
                owner.clone(),
            )
            .await
            .map_err(Error::Store)?;
        let metadata_written = message.clone().write(inner.jobs.clone(), owner).await?;
        task.complete(&inner.publications, &inner.named_publications)?;
        return Ok(Run::CompletedNoop {
            task: message.key(),
            metadata_written,
        });
    }
    let final_state = inner
        .store
        .clone()
        .normal_state(recording, owner.clone())
        .await
        .map_err(Error::Store)?;
    if final_state.is_none() || capture.reason == recording_process::NormalStop::Deleted {
        let cleanup = files.clone();
        let held = owner.clone();
        tokio::task::spawn_blocking(move || {
            let _owner = held;
            cleanup.cancel_normal()
        })
        .await
        .map_err(|_| Error::Supervisor)?
        .map_err(Error::Files)?;
        let metadata_written = message.clone().write(inner.jobs.clone(), owner).await?;
        task.complete(&inner.publications, &inner.named_publications)?;
        return Ok(Run::CompletedNoop {
            task: message.key(),
            metadata_written,
        });
    }
    let observed_files = files.clone();
    let held = owner.clone();
    let (bytes_written, playlist_exists) = tokio::task::spawn_blocking(move || {
        let _owner = held;
        let bytes = observed_files.normal_segment_bytes()?;
        let exists = observed_files.normal_playlist_exists()?;
        if exists {
            observed_files.ensure_endlist()?;
        }
        Ok::<_, recording_files::Error>((bytes, exists))
    })
    .await
    .map_err(|_| Error::Supervisor)?
    .map_err(Error::Files)?;
    let remux_success = if playlist_exists {
        let held = owner.clone();
        let log = transcript.clone();
        let media_paths = paths.clone();
        tokio::task::spawn_blocking(move || {
            recording_process::remux_normal(media_paths, recording, held, log)
        })
        .await
        .map_err(|_| Error::Supervisor)?
        .map_err(Error::Media)?
    } else {
        false
    };
    // A stop/delete can arrive while remux owns the files. Source reads the
    // actual row again before final metadata/events; do not revive a deleted row.
    let final_state = inner
        .store
        .clone()
        .normal_state(recording, owner.clone())
        .await
        .map_err(Error::Store)?;
    let Some(final_state) = final_state else {
        let cleanup = files.clone();
        let held = owner.clone();
        tokio::task::spawn_blocking(move || {
            let _owner = held;
            cleanup.cancel_normal()
        })
        .await
        .map_err(|_| Error::Supervisor)?
        .map_err(Error::Files)?;
        let metadata_written = message.clone().write(inner.jobs.clone(), owner).await?;
        task.complete(&inner.publications, &inner.named_publications)?;
        return Ok(Run::CompletedNoop {
            task: message.key(),
            metadata_written,
        });
    };
    if remux_success {
        inner
            .store
            .clone()
            .publish_normal_permanent_urls(recording, owner.clone())
            .await
            .map_err(Error::Store)?;
        let viewer_deadline = tokio::time::Instant::now() + Duration::from_secs(14400);
        loop {
            let observed = inner
                .active
                .normal_viewer_absent(recording, owner.clone())
                .await
                .map_err(Error::Active)?;
            trace(&owner, "normal-HLS-viewer", observed.receipt())?;
            if observed.absent || tokio::time::Instant::now() > viewer_deadline {
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        let cleanup = files.clone();
        let held = owner.clone();
        tokio::task::spawn_blocking(move || {
            let _owner = held;
            cleanup.remove_hls()
        })
        .await
        .map_err(|_| Error::Supervisor)?
        .map_err(Error::Files)?;
        inner
            .store
            .clone()
            .clear_runtime_hls(recording, owner.clone())
            .await
            .map_err(Error::Store)?;
    }
    let interrupted_reason = if final_state.stopped {
        None
    } else if !capture.stream_confirmed {
        Some(
            capture
                .interrupted_reason
                .unwrap_or_else(|| "no_stream_data: ffmpeg_failed".into()),
        )
    } else {
        capture.interrupted_reason
    };
    let stream_info = match inner
        .active
        .normal_stream_info(current.channel_uuid, owner.clone())
        .await
    {
        Ok(value) => value,
        Err(recording_active::Error::Backend(error)) => {
            trace(
                &owner,
                "caught-stream-metadata-read",
                json!({"error":error.to_string().chars().take(2048).collect::<String>()}),
            )?;
            None
        }
        Err(error) => return Err(Error::Active(error)),
    };
    let finalized = inner
        .store
        .clone()
        .finalize_runtime(
            recording,
            dispatcharr_store::recording_capture::RuntimeFinalize {
                ended: Utc::now(),
                bytes_written,
                remux_success,
                interrupted_reason,
                server_shutdown: false,
                stream_info,
            },
            owner.clone(),
        )
        .await
        .map_err(Error::Store)?;
    let properties = match finalized {
        dispatcharr_store::recording_capture::RuntimeFinalizeOutcome::Applied(value) => value,
        dispatcharr_store::recording_capture::RuntimeFinalizeOutcome::Caught { intended, code } => {
            trace(
                &owner,
                "settled-final-save-rejection",
                json!({"code":code,"sql_applied":false}),
            )?;
            intended
        }
        dispatcharr_store::recording_capture::RuntimeFinalizeOutcome::Missing => {
            // This SELECT-none corresponds to Source refresh_from_db failing
            // before its later exists() guards; the caught exception still
            // reaches automatic dispatch and active-lock release.
            let children = automatic_comskip_children(&inner, &message, owner.clone()).await?;
            let removed = inner
                .active
                .remove_normal_retired(recording, owner.clone())
                .await
                .map_err(Error::Active)?;
            trace(&owner, "removed", removed.receipt(json!(removed.value)))?;
            drop(files);
            let metadata_written = message
                .clone()
                .write_with_children(inner.jobs.clone(), children, owner)
                .await?;
            task.complete(&inner.publications, &inner.named_publications)?;
            return Ok(Run::CompletedNoop {
                task: message.key(),
                metadata_written,
            });
        }
    };
    let observed_files = files.clone();
    let held = owner.clone();
    let file_size = tokio::task::spawn_blocking(move || {
        let _owner = held;
        observed_files.normal_final_size()
    })
    .await
    .map_err(|_| Error::Supervisor)?
    .map_err(Error::Files)?;
    let failure = if !remux_success {
        Some("remux_failed")
    } else if file_size.is_none() {
        Some("missing_file")
    } else if file_size == Some(0) {
        Some("empty_file")
    } else {
        None
    };
    let status = properties
        .get("status")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .unwrap_or("unknown");
    let file_path = paths
        .final_path
        .to_str()
        .ok_or(Error::Unsupported("audit path UTF8"))?;
    let filename = properties
        .get("file_name")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .unwrap_or(&paths.filename);
    let details = json!({"recording_id":recording,"outcome":if failure.is_none(){"success"}else{"failed"},
        "has_file":failure.is_none(),"failure_reason":failure,"status":status,
        "interrupted_reason":properties.get("interrupted_reason").and_then(Value::as_str).filter(|v|!v.is_empty()),
        "file_path":file_path,"file_name":filename,"file_url":properties.get("file_url").and_then(Value::as_str),"file_size":file_size,
        "remux_success":remux_success,"bytes_written":properties.get("bytes_written").filter(|v|v.is_i64()||v.is_u64()),
        "start_time":recording_normal::source_iso(final_state.start),"end_time":recording_normal::source_iso(final_state.end)});
    // Actual row absence after the final save suppresses end publication too.
    if inner
        .store
        .clone()
        .normal_state(recording, owner.clone())
        .await
        .map_err(Error::Store)?
        .is_some()
    {
        emit_recording_event(
            &inner,
            &task,
            NormalEvent::Ended,
            current.channel_uuid,
            current.channel_name.clone(),
            details,
            max_events,
            owner.clone(),
        )
        .await?;
        recording_events::send(
            inner.events.clone(),
            recording_events::ended(&current.channel_name),
            owner.clone(),
        )
        .await
        .map_err(Error::Events)?;
    } else {
        let metadata_written = message.clone().write(inner.jobs.clone(), owner).await?;
        task.complete(&inner.publications, &inner.named_publications)?;
        return Ok(Run::CompletedNoop {
            task: message.key(),
            metadata_written,
        });
    }
    let postprocess_children = automatic_comskip_children(&inner, &message, owner.clone()).await?;
    let removed = inner
        .active
        .remove_normal_retired(recording, owner.clone())
        .await
        .map_err(Error::Active)?;
    trace(&owner, "removed", removed.receipt(json!(removed.value)))?;
    let (records, poisoned, pending) = transcript.snapshot();
    if poisoned
        || pending
        || records.iter().any(|row| {
            row.emergency
                || row.wait_error.is_some()
                || row.reader_error.is_some()
                || row.kill_error.is_some()
                || row.settled_utc.is_none()
        })
    {
        return Err(Error::Unsettled);
    }
    // Known spawn failure and nonzero process exits are retained diagnostics,
    // not fabricated ownership loss. All actual children/stderr are settled.
    drop(files);
    let metadata_written = message
        .clone()
        .write_with_children(inner.jobs.clone(), postprocess_children, owner)
        .await?;
    task.complete(&inner.publications, &inner.named_publications)?;
    let active_observations = per_trace.lock().map_err(|_| Error::Unsettled)?.clone();
    Ok(Run::Completed {
        task: message.key(),
        metadata_written,
        bytes_written,
        file_size,
        processes: records.iter().map(|row| row.receipt()).collect(),
        active_observations,
    })
}

impl crate::milestone_dispatch::Lifecycle for NormalDvr {
    fn shutdown(
        &self,
        timeout: Duration,
    ) -> dispatcharr_contracts::BoxFuture<'_, Result<(), crate::milestone_dispatch::Disposition>>
    {
        Box::pin(async move {
            self.request_shutdown();
            self.drain(timeout)
                .await
                .map_err(|e| crate::milestone_dispatch::Disposition::Operational(format!("{e:?}")))
        })
    }
}
impl crate::milestone_dispatch::Handler for NormalDvr {
    fn head_advance_notifier(&self) -> Option<Arc<tokio::sync::Notify>> {
        Some(self.inner.head_advance.clone())
    }
    fn maximum_concurrency(&self) -> usize {
        20
    }
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn run_selected(
        &self,
        expected: Arc<[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<
        '_,
        Result<crate::milestone_dispatch::Disposition, crate::milestone_dispatch::Disposition>,
    > {
        Box::pin(async move { self.disposition(owner, Some(expected)).await })
    }

    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<
        '_,
        Result<crate::milestone_dispatch::Disposition, crate::milestone_dispatch::Disposition>,
    > {
        Box::pin(async move { self.disposition(owner, None).await })
    }
}
impl NormalDvr {
    async fn disposition(
        &self,
        owner: Arc<dyn Send + Sync>,
        expected: Option<Arc<[u8]>>,
    ) -> Result<crate::milestone_dispatch::Disposition, crate::milestone_dispatch::Disposition>
    {
        use crate::milestone_dispatch::Disposition as D;
        async move {
            match self.run_expected(owner, expected).await {
                Ok(Run::Empty) => Ok(D::Empty),
                Ok(Run::UnsupportedHead) => Err(D::UnsupportedHead),
                Ok(Run::Revoked { task, .. }) => Ok(match task {
                    recording_normal_task::TaskKey::Uuid(id) => D::Revoked(id),
                    recording_normal_task::TaskKey::Named(key) => D::RevokedNamed(key),
                }),

                Ok(Run::Completed { task, .. } | Run::CompletedNoop { task, .. }) => {
                    Ok(match task {
                        recording_normal_task::TaskKey::Uuid(id) => D::Completed(id),
                        recording_normal_task::TaskKey::Named(key) => D::CompletedNamed(key),
                    })
                }
                Err(Error::Unsupported(message)) => {
                    let unsettled = self
                        .inner
                        .uncertain
                        .lock()
                        .map_err(|_| D::Operational("DVR ownership mutex poisoned".into()))?
                        .is_empty();
                    let unsettled = !unsettled;
                    if unsettled {
                        Err(D::Operational(message.into()))
                    } else {
                        Err(D::UnsupportedDomain)
                    }
                }
                Err(
                    Error::Capacity
                    | Error::Publication(crate::normal_recording_publication::Error::Capacity),
                ) => Ok(D::Capacity),
                Err(Error::NotDue) => Ok(D::NotDue),
                Err(Error::Transport(crate::transport::Error::ClaimRace)) => Ok(D::HeadChanged),
                Err(Error::Store(dispatcharr_store::recording_capture::Error::Connect(_))) => Err(
                    D::Operational("Recording Connect scope remains unsettled".into()),
                ),
                Err(Error::Store(dispatcharr_store::recording_capture::Error::Scoped(_))) => Err(
                    D::Operational("Recording hook/SQL scope remains unsettled".into()),
                ),
                Err(error) => Err(D::Operational(format!("{error:?}"))),
            }
        }
        .await
    }
}

/// Startup recovery uses the same retained filesystem/process/store ports. The
/// context is sealed by actual recovery SQL; no recording identity is invented.
impl crate::normal_recording_recovery::RecoveryMedia for NormalDvr {
    fn ensure_domain_supported(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<
        '_,
        Result<(), crate::normal_recording_recovery::RecoveryMediaError>,
    > {
        Box::pin(async move {
            self.inner
                .plugin_domain
                .require_supported()
                .map_err(|error| match error {
                    Error::Unsupported(_) => {
                        crate::normal_recording_recovery::RecoveryMediaError::Unsupported
                    }
                    _ => crate::normal_recording_recovery::RecoveryMediaError::Unsettled,
                })?;
            self.inner
                .store
                .clone()
                .require_unbound_event_domain(NormalEvent::Ended, owner)
                .await
                .map_err(|error| match error {
                    dispatcharr_store::recording_capture::Error::Unsupported(_) => {
                        crate::normal_recording_recovery::RecoveryMediaError::Unsupported
                    }
                    _ => crate::normal_recording_recovery::RecoveryMediaError::Unsettled,
                })
        })
    }
    fn finalize_expired(
        &self,
        context: crate::normal_recording_recovery::ExpiredRecording,
        caller: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<
        '_,
        Result<(), crate::normal_recording_recovery::RecoveryMediaError>,
    > {
        Box::pin(async move {
            use crate::normal_recording_recovery::RecoveryMediaError as R;
            self.inner
                .plugin_domain
                .require_supported()
                .map_err(|error| match error {
                    Error::Unsupported(_) => R::Unsupported,
                    _ => R::Unsettled,
                })?;
            self.inner
                .store
                .clone()
                .require_unbound_event_domain(NormalEvent::Ended, caller.clone())
                .await
                .map_err(|error| match error {
                    dispatcharr_store::recording_capture::Error::Unsupported(_) => R::Unsupported,
                    _ => R::Unsettled,
                })?;
            if self.inner.closed.load(Ordering::Acquire) {
                return Err(R::Unsettled);
            }
            let slot = self
                .inner
                .slot
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| R::Unsettled)?;
            let owner = Arc::new(Owner {
                _caller: caller,
                _slot: slot,
                trace: Arc::new(Mutex::new(Vec::with_capacity(16))),
                expected: None,
            });
            let inner = self.inner.clone();
            tokio::spawn(async move {
                let transcript = Arc::new(recording_process::Transcript::new_normal());
                let result =
                    finalize_expired(inner.clone(), context, owner.clone(), transcript.clone())
                        .await;
                if result.is_err() {
                    // Unknown ownership cannot be discarded merely because the
                    // outer Source recovery task catches per-row exceptions.
                    inner.closed.store(true, Ordering::Release);
                    inner
                        .uncertain
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push((owner, inner.clone(), transcript));
                }
                result.map_err(|e| match e {
                    Error::Unsupported(_) => R::Unsupported,
                    _ => R::Unsettled,
                })
            })
            .await
            .map_err(|_| R::Unsettled)?
        })
    }
    fn revoke_old_task(
        &self,
        context: crate::normal_recording_recovery::ActiveRecording,
        owner: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<
        '_,
        Result<(), crate::normal_recording_recovery::RecoveryMediaError>,
    > {
        let inner = self.inner.clone();
        Box::pin(async move {
            use crate::normal_recording_recovery::RecoveryMediaError as R;
            tokio::spawn(async move {
                inner
                    .plugin_domain
                    .require_supported()
                    .map_err(|error| match error {
                        Error::Unsupported(_) => R::Unsupported,
                        _ => R::Unsettled,
                    })?;
                let Some(key) = context.old_task_key().filter(|s| !s.is_empty()) else {
                    return Ok(());
                };
                let deleted = inner
                    .cleanup
                    .delete_by_name(key.to_owned(), owner.clone())
                    .await
                    .map_err(|_| R::Unsettled)?;
                if !deleted {
                    // PUBLISH reply is settled publication, never a claim that
                    // all workers have already applied the revoke command.
                    inner
                        .control
                        .revoke_recording_task(key, owner)
                        .await
                        .map_err(|_| R::Unsettled)?;
                }
                Ok(())
            })
            .await
            .map_err(|_| R::Unsettled)?
        })
    }
}
async fn finalize_expired(
    inner: Arc<Inner>,
    context: crate::normal_recording_recovery::ExpiredRecording,
    owner: Arc<Owner>,
    transcript: Arc<recording_process::Transcript>,
) -> Result<(), Error> {
    inner.plugin_domain.require_supported()?;
    let recording = context.recording_id();
    let props = context.properties().clone();
    let uid = inner.media_uid;
    let plugin_directory = inner.plugin_directory.clone();
    let held = owner.clone();
    let files = tokio::task::spawn_blocking(move || {
        let Some(paths) =
            recording_normal_policy::resume_properties(&props).map_err(policy_error)?
        else {
            return Ok::<_, Error>(None);
        };
        let prepared =
            recording_files::Prepared::inspect_normal(paths, uid, held, true, plugin_directory)
                .map_err(Error::Files)?;
        let working = prepared
            .create_parent()
            .map_err(Error::Files)?
            .start_hls()
            .map_err(Error::Files)?;
        if !working.normal_playlist_exists().map_err(Error::Files)? {
            return Ok(None);
        }
        working.ensure_endlist().map_err(Error::Files)?;
        Ok(Some(Arc::new(working)))
    })
    .await
    .map_err(|_| Error::Supervisor)??;
    let mut remux_success = false;
    if let Some(files) = &files {
        let paths = files.paths().clone();
        let held = owner.clone();
        let log = transcript.clone();
        remux_success = tokio::task::spawn_blocking(move || {
            recording_process::remux_normal(paths, recording, held, log)
        })
        .await
        .map_err(|_| Error::Supervisor)?
        .map_err(Error::Media)?;
        if remux_success {
            // Expired Source recovery removes HLS directly; its live-viewer
            // wait belongs to the running recorder, not this recovery branch.
            let cleanup = files.clone();
            let held = owner.clone();
            tokio::task::spawn_blocking(move || {
                let _owner = held;
                cleanup.remove_hls()
            })
            .await
            .map_err(|_| Error::Supervisor)?
            .map_err(Error::Files)?;
        }
    }
    let finalized = inner
        .store
        .clone()
        .finalize_expired_runtime(recording, remux_success, owner.clone())
        .await
        .map_err(Error::Store)?;
    let properties = match finalized {
        dispatcharr_store::recording_capture::RuntimeFinalizeOutcome::Missing => return Ok(()),
        dispatcharr_store::recording_capture::RuntimeFinalizeOutcome::Applied(p) => p,
        dispatcharr_store::recording_capture::RuntimeFinalizeOutcome::Caught { intended, code } => {
            trace(
                &owner,
                "expired-settled-save-rejection",
                json!({"code":code,"sql_applied":false}),
            )?;
            intended
        }
    };
    let file_path = properties
        .get("file_path")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    let file_size = if let Some(files) = files {
        let held = owner.clone();
        tokio::task::spawn_blocking(move || {
            let _owner = held;
            files.normal_final_size()
        })
        .await
        .map_err(|_| Error::Supervisor)?
        .map_err(Error::Files)?
    } else {
        // No file-root capability means no arbitrary path stat/read authority.
        // Source's supported normal paths are rooted in the owned library.
        if let Some(path) = file_path {
            let path = std::path::PathBuf::from(path);
            let held = owner.clone();
            let uid = inner.media_uid;
            tokio::task::spawn_blocking(move || {
                recording_files::normal_recorded_file_size(path, uid, held)
            })
            .await
            .map_err(|_| Error::Supervisor)?
            .map_err(Error::Files)?
        } else {
            None
        }
    };
    let failure = if !remux_success {
        Some("remux_failed")
    } else if file_size.is_none() {
        Some("missing_file")
    } else if file_size == Some(0) {
        Some("empty_file")
    } else {
        None
    };
    let file_name = properties
        .get("file_name")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            file_path.and_then(|s| {
                std::path::Path::new(s)
                    .file_name()?
                    .to_str()
                    .map(str::to_owned)
            })
        });
    let groups = inner
        .settings
        .admission(owner.clone())
        .await
        .map_err(Error::Settings)?;
    let max_events = recording_normal_policy::max_events(&groups).map_err(policy_error)?;
    let details = json!({"recording_id":recording,"outcome":if failure.is_none(){"success"}else{"failed"},"has_file":failure.is_none(),
        "failure_reason":failure,"status":properties.get("status").and_then(Value::as_str).filter(|s|!s.is_empty()).unwrap_or("unknown"),
        "interrupted_reason":properties.get("interrupted_reason").and_then(Value::as_str).filter(|s|!s.is_empty()),
        "file_path":file_path,"file_name":file_name,"file_url":properties.get("file_url").and_then(Value::as_str),"file_size":file_size,"remux_success":remux_success,
        "bytes_written":properties.get("bytes_written").filter(|v|v.is_i64()||v.is_u64()),
        "start_time":recording_normal::source_iso(context.recording_start()),"end_time":recording_normal::source_iso(context.recording_end())});
    inner
        .store
        .clone()
        .insert_runtime_event(
            NormalEvent::Ended,
            context.channel_uuid(),
            context.channel_name().to_owned(),
            details,
            max_events,
            owner,
        )
        .await
        .map_err(Error::Store)?;
    Ok(())
}
