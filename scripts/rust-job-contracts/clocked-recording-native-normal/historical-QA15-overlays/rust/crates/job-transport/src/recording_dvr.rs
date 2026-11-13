//! PRIVATE UNCOMPILED actual normal-DVR composition. Original Dispatcharr and
//! dependency authors retain credit. No Main/default consumer/registry attached.
//! Source scope: actual single Source3 normal case; no failure/default graph claim.
use crate::{
    clocked_recording_envelope::ObservedProducerEnvelope,
    recording_active, recording_cleanup, recording_events, recording_files, recording_normal,
    recording_process, recording_settings,
    transport::{Config, Jobs},
};
use chrono::Utc;
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_store::{
    recording_capture::{CaptureStore, NormalEvent, PropertyWrite, RecordingIdentity},
    Store,
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

#[derive(Debug)]
pub enum Error {
    Closed,
    Capacity,
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
}
pub enum Run {
    Empty,
    UnsupportedHead,
    Completed {
        task: uuid::Uuid,
        metadata_written: bool,
        bytes_written: u64,
        file_size: u64,
        processes: Vec<Value>,
        active_observations: Vec<Value>,
    },
}
struct Owner {
    _caller: Arc<dyn Send + Sync>,
    _slot: OwnedSemaphorePermit,
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
    media_uid: u32,
    cleanup: recording_cleanup::Component,
    slot: Arc<Semaphore>,
    closed: AtomicBool,
    shutdown: Arc<AtomicBool>,
    // At most one unknown post-claim operation pins its original scope. No
    // generic error->terminal metadata, lease cleanup, requeue or automatic retry.
    uncertain: Mutex<Option<UncertainOwner>>,
    transcript: Mutex<Option<Arc<recording_process::Transcript>>>,
    active_trace: Mutex<Vec<Value>>,
}
pub struct Dvr {
    inner: Arc<Inner>,
}
async fn transport<T, F, Fut>(owner: Arc<dyn Send + Sync>, operation: F) -> Result<T, Error>
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
impl Dvr {
    pub async fn connect(assembly: Assembly, owner: Arc<dyn Send + Sync>) -> Result<Self, Error> {
        tokio::spawn(async move {
            let _owner = owner.clone();
            if assembly.configured_plugin_directory != "/data/plugins" {
                return Err(Error::Unsupported("captured configured plugin directory"));
            }
            recording_process::ObservedLocalOrigin::from_explicit_setting(
                &assembly.explicit_internal_ts_base,
            )
            .map_err(Error::Media)?;
            let jobs =
                Jobs::connect_recording_dvr(&assembly.broker, &assembly.result, Config::default())
                    .await
                    .map_err(Error::Transport)?;
            let active = recording_active::Operations::connect(assembly.cache.clone(), owner)
                .await
                .map_err(Error::Active)?;
            let store = Arc::new(CaptureStore::source3_local(assembly.store.pool().clone()));
            let cleanup =
                recording_cleanup::Component::source3_normal(assembly.store.pool().clone());
            let settings = recording_settings::Settings::new(assembly.store, assembly.cache);
            Ok(Self {
                inner: Arc::new(Inner {
                    jobs,
                    store,
                    settings,
                    active,
                    events: assembly.events,
                    explicit_origin: assembly.explicit_internal_ts_base,
                    media_uid: assembly.media_uid,
                    cleanup,
                    slot: Arc::new(Semaphore::new(1)),
                    closed: AtomicBool::new(false),
                    shutdown: Arc::new(AtomicBool::new(false)),
                    uncertain: Mutex::new(None),
                    transcript: Mutex::new(None),
                    active_trace: Mutex::new(Vec::new()),
                }),
            })
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    pub async fn run_once(&self, caller: Arc<dyn Send + Sync>) -> Result<Run, Error> {
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
        });
        let inner = self.inner.clone();
        // Observer loss never cancels queued work or its original physical owner.
        tokio::spawn(async move {
            let claimed = AtomicBool::new(false);
            let transcript = Arc::new(recording_process::Transcript::new());
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
                *uncertain = Some((owner, inner.clone(), transcript));
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
    }
    pub fn available_slots(&self) -> usize {
        self.inner.slot.available_permits()
    }
    pub async fn drain(&self, timeout: Duration) -> Result<(), Error> {
        // Graceful close stops intake, not the normal recording's observer/body.
        self.inner.closed.store(true, Ordering::Release);
        let deadline = tokio::time::Instant::now() + timeout;
        while self.available_slots() != 1 {
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
fn trace(inner: &Inner, kind: &str, observation: Value) -> Result<(), Error> {
    let mut log = inner.active_trace.lock().map_err(|_| Error::Unsettled)?;
    if log.len() >= 16 {
        return Err(Error::Unsettled);
    }
    log.push(json!({"kind":kind,"observation":observation}));
    Ok(())
}
fn due(message: &ObservedProducerEnvelope) -> Result<(), Error> {
    let now = Utc::now();
    if message.end() - message.start() != chrono::Duration::seconds(30)
        || now < message.start()
        || now >= message.end()
    {
        return Err(Error::Unsupported("captured current due30s domain"));
    }
    Ok(())
}
async fn execute(
    inner: Arc<Inner>,
    owner: Arc<Owner>,
    claimed: &AtomicBool,
    transcript: Arc<recording_process::Transcript>,
) -> Result<Run, Error> {
    let jobs = inner.jobs.clone();
    let candidate = match transport(owner.clone(), move |guard| async move {
        jobs.peek_recording_dvr(guard).await
    })
    .await
    {
        Ok(Some(value)) => value,
        Ok(None) => return Ok(Run::Empty),
        Err(Error::Transport(crate::transport::Error::UnsupportedTask)) => {
            return Ok(Run::UnsupportedHead)
        }
        Err(error) => return Err(error),
    };
    let message = candidate.message.clone();
    due(&message)?;
    let expected = RecordingIdentity {
        recording: message.recording_id(),
        channel: message.channel_id(),
        start: message.start(),
        end: message.end(),
    };
    let snapshot = inner
        .store
        .clone()
        .snapshot(expected.clone(), owner.clone())
        .await
        .map_err(Error::Store)?;
    let cleanup = inner
        .cleanup
        .prepare(expected.clone(), owner.clone())
        .await
        .map_err(|error| Error::Cleanup(Box::new(error)))?;
    let groups = inner
        .settings
        .admission(owner.clone())
        .await
        .map_err(Error::Settings)?;
    let paths = recording_normal::paths(
        message.recording_id(),
        &snapshot.properties,
        &groups,
        snapshot.audit_rows,
    )
    .map_err(|_| Error::Unsupported("normal metadata/settings domain"))?;
    let owned = owner.clone();
    let planned = paths.clone();
    let uid = inner.media_uid;
    let files = tokio::task::spawn_blocking(move || {
        recording_files::Prepared::inspect(planned, uid, owned)
    })
    .await
    .map_err(|_| Error::Supervisor)?
    .map_err(Error::Files)?;
    let before = inner
        .active
        .observe(message.recording_id(), owner.clone())
        .await
        .map_err(Error::Active)?;
    trace(&inner, "before-active", before.receipt(json!(before.value)))?;
    if before.value != (0, -2) {
        return Err(Error::Unsupported("preexisting DVR active key"));
    }
    let auxiliary = inner
        .active
        .auxiliary_absent(message.recording_id(), snapshot.channel_uuid, owner.clone())
        .await
        .map_err(Error::Active)?;
    trace(&inner, "before-auxiliary", auxiliary.receipt())?;
    if !auxiliary.absent {
        return Err(Error::Unsupported(
            "initial Source3 has no viewers/proxy metadata",
        ));
    }
    due(&message)?;
    if inner.closed.load(Ordering::Acquire) {
        return Err(Error::Closed);
    }
    // Exact raw candidate from the actual queue, validated by all domain gates.
    // Mark ambiguous before dispatch; only confirmed claim-race can clear it.
    claimed.store(true, Ordering::Release);
    let jobs = inner.jobs.clone();
    if let Err(error) = transport(owner.clone(), move |guard| async move {
        jobs.claim_recording_dvr(candidate, guard).await
    })
    .await
    {
        if matches!(error, Error::Transport(crate::transport::Error::ClaimRace)) {
            claimed.store(false, Ordering::Release);
        }
        return Err(error);
    }
    let lease = inner
        .active
        .set_once(message.recording_id(), owner.clone())
        .await
        .map_err(Error::Active)?;
    trace(&inner, "acquired", lease.receipt(json!(lease.value)))?;
    if !lease.value {
        return Err(Error::Unsupported("post-admission other active owner"));
    }
    inner
        .cleanup
        .execute(cleanup, owner.clone())
        .await
        .map_err(|error| Error::Cleanup(Box::new(error)))?;
    // Source re-reads Channel after periodic cleanup. Bounded snapshot checks
    // also refuse a changed manual/metadata context before publishing started.
    let current = inner
        .store
        .clone()
        .snapshot(expected, owner.clone())
        .await
        .map_err(Error::Store)?;
    if current.channel_uuid != snapshot.channel_uuid
        || current.channel_name != snapshot.channel_name
        || current.properties != snapshot.properties
    {
        return Err(Error::Unsupported("post-admission recording changed"));
    }
    recording_events::send(
        inner.events.clone(),
        recording_events::started(&current.channel_name),
        owner.clone(),
    )
    .await
    .map_err(Error::Events)?;
    let max_events = groups
        .system
        .get("max_system_events")
        .and_then(Value::as_u64)
        .unwrap_or(100);
    inner
        .store
        .clone()
        .insert_normal_event(
            NormalEvent::Started,
            current.channel_uuid,
            current.channel_name.clone(),
            json!({"recording_id":message.recording_id()}),
            max_events,
            owner.clone(),
        )
        .await
        .map_err(Error::Store)?;
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
    inner
        .store
        .clone()
        .write_properties(
            message.recording_id(),
            PropertyWrite::Prime(patch),
            owner.clone(),
        )
        .await
        .map_err(Error::Store)?;
    recording_events::send(
        inner.events.clone(),
        recording_events::updated(message.recording_id()),
        owner.clone(),
    )
    .await
    .map_err(Error::Events)?;
    let files = tokio::task::spawn_blocking(move || parent.start_hls())
        .await
        .map_err(|_| Error::Supervisor)?
        .map_err(Error::Files)?;
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
    let origin =
        recording_process::ObservedLocalOrigin::from_explicit_setting(&inner.explicit_origin)
            .map_err(Error::Media)?;
    let media_paths = paths.clone();
    let shutdown = inner.shutdown.clone();
    let held = owner.clone();
    let log = transcript.clone();
    let end = message.end();
    let channel = current.channel_uuid;
    let recording = message.recording_id();
    let mut media = tokio::task::spawn_blocking(move || {
        recording_process::capture(
            channel,
            recording,
            media_paths,
            origin,
            end,
            shutdown,
            held,
            log,
        )
    });
    let mut renewal = tokio::time::interval(Duration::from_secs(15));
    renewal.tick().await;
    loop {
        tokio::select! {
            result=&mut media=>{result.map_err(|_|Error::Supervisor)?.map_err(Error::Media)?;break;},
            _=renewal.tick()=>{
                if Utc::now()>=message.end() {continue;}
                match inner.active.refresh_confirmed(recording,&lease,owner.clone()).await {
                    Ok(value)=>trace(&inner,"renewed",value.receipt(json!(value.value)))?,
                    Err(error)=>{
                    inner.shutdown.store(true,Ordering::Release);
                    // A failed Redis refresh never abandons the actual child.
                    let _=media.await.map_err(|_|Error::Supervisor)?;
                    return Err(Error::Active(error));
                    }
                }
            }
        }
    }
    let files = tokio::task::spawn_blocking(move || {
        files.ensure_endlist()?;
        let bytes = files.segment_bytes()?;
        Ok::<_, recording_files::Error>((files, bytes))
    })
    .await
    .map_err(|_| Error::Supervisor)?
    .map_err(Error::Files)?;
    let (files, bytes_written) = files;
    let shutdown = inner.shutdown.clone();
    let held = owner.clone();
    let log = transcript.clone();
    tokio::task::spawn_blocking(move || recording_process::remux(paths, shutdown, held, log))
        .await
        .map_err(|_| Error::Supervisor)?
        .map_err(Error::Media)?;
    let auxiliary = inner
        .active
        .auxiliary_absent(recording, current.channel_uuid, owner.clone())
        .await
        .map_err(Error::Active)?;
    trace(&inner, "after-auxiliary", auxiliary.receipt())?;
    if !auxiliary.absent {
        return Err(Error::Unsupported(
            "viewer/proxy metadata arrived outside normal Source3 scope",
        ));
    }
    let (files, file_size) = tokio::task::spawn_blocking(move || {
        let size = files.final_size()?;
        files.remove_hls()?;
        Ok::<_, recording_files::Error>((files, size))
    })
    .await
    .map_err(|_| Error::Supervisor)?
    .map_err(Error::Files)?;
    inner
        .store
        .clone()
        .write_properties(recording, PropertyWrite::RemovedOwnedHls, owner.clone())
        .await
        .map_err(Error::Store)?;
    let properties = inner
        .store
        .clone()
        .write_properties(
            recording,
            PropertyWrite::CompletedNormal {
                actual_ended: Utc::now(),
                bytes_written,
            },
            owner.clone(),
        )
        .await
        .map_err(Error::Store)?;
    let details = json!({"recording_id":recording,"outcome":"success","has_file":true,"failure_reason":null,
        "status":"completed","interrupted_reason":null,"file_path":properties["file_path"],"file_name":properties["file_name"],
        "file_url":properties["file_url"],"file_size":file_size,"remux_success":true,"bytes_written":bytes_written,
        "start_time":recording_normal::source_iso(message.start()),"end_time":recording_normal::source_iso(message.end())});
    inner
        .store
        .clone()
        .insert_normal_event(
            NormalEvent::Ended,
            current.channel_uuid,
            current.channel_name.clone(),
            details,
            max_events,
            owner.clone(),
        )
        .await
        .map_err(Error::Store)?;
    if inner
        .settings
        .normal_effect(true, owner.clone())
        .await
        .map_err(Error::Settings)?
        != groups.system
    {
        return Err(Error::Unsupported("system group changed at end"));
    }
    recording_events::send(
        inner.events.clone(),
        recording_events::ended(&current.channel_name),
        owner.clone(),
    )
    .await
    .map_err(Error::Events)?;
    let removed = inner
        .active
        .remove_confirmed(recording, &lease, owner.clone())
        .await
        .map_err(Error::Active)?;
    trace(&inner, "removed", removed.receipt(json!(removed.value)))?;
    let (records, poisoned, pending) = transcript.snapshot();
    if poisoned
        || pending
        || records.len() != 2
        || records.iter().any(|row| {
            row.emergency
                || row.wait_error.is_some()
                || row.reader_error.is_some()
                || row.command_error.is_some()
                || row.settled_utc.is_none()
        })
    {
        return Err(Error::Unsettled);
    }
    drop(files);
    let jobs = inner.jobs.clone();
    let result_message = message.clone();
    let metadata_written = transport(owner, move |guard| async move {
        jobs.write_recording_dvr(&result_message, guard).await
    })
    .await?;
    let active_observations = inner
        .active_trace
        .lock()
        .map_err(|_| Error::Unsettled)?
        .clone();
    Ok(Run::Completed {
        task: message.task_id(),
        metadata_written,
        bytes_written,
        file_size,
        processes: records.iter().map(|row| row.receipt()).collect(),
        active_observations,
    })
}
