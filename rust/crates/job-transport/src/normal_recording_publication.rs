//! Normal due-clocked SQL→broker provenance, never minted from an envelope alone.
//! Original Dispatcharr/Celery/django-celery-beat authors retain all credit.
//! This module starts no scheduler; the normal scheduler remains explicitly off
//! until its installed families and full due/publication/sync scope are assembled.
use crate::{
    clocked_recording_envelope::{self, ObservedProducerEnvelope},
    transport::{self, Jobs},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::{Connection, PgPool, Row};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
#[derive(Debug)]
pub enum Error {
    Closed,
    Capacity,
    Unsupported(&'static str),
    Database {
        primary: Option<Box<sqlx::Error>>,
        ready: Option<Box<sqlx::Error>>,
        close: Option<Box<sqlx::Error>>,
    },
    Transport(transport::Error),
    Policy(dispatcharr_ingest_epg::cache_policy::ConfiguredSettingsError),
    Settings(crate::recording_settings::Error),
    Audit(Box<dispatcharr_store::recording_capture::Error>),
    Cleanup(Box<dispatcharr_store::recording_beat_cleanup::Error>),
    Supervisor,
    DrainTimeout,
}
/// Evidence source, never a caller-minted publication or fabricated row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    Published,
    ObservedRecording,
    ObservedAbsence,
}
struct Entry {
    message: ObservedProducerEnvelope,
    periodic: Option<i64>,
    admission: Admission,
    _slot: OwnedSemaphorePermit,
}
struct RegistryInner {
    entries: Mutex<BTreeMap<Uuid, Arc<Entry>>>,
    slots: Arc<Semaphore>,
}
#[derive(Clone)]
pub struct NormalRecordingPublications {
    inner: Arc<RegistryInner>,
}
/// Sealed actual publication: fields can be inspected, never arbitrarily set.
#[derive(Clone)]
pub struct NormalRecordingPublication {
    entry: Arc<Entry>,
    origin: Arc<RegistryInner>,
}
impl NormalRecordingPublication {
    pub fn message(&self) -> &ObservedProducerEnvelope {
        &self.entry.message
    }
    pub fn task_id(&self) -> Uuid {
        self.message().task_id()
    }
    pub fn raw_frame(&self) -> &[u8] {
        self.message().raw_frame()
    }
    pub fn recording_id(&self) -> i64 {
        self.message().recording_id()
    }
    pub fn channel_id(&self) -> i64 {
        self.message().channel_id()
    }
    pub fn periodic_name(&self) -> &str {
        self.message().periodic_name()
    }
    pub fn periodic_id(&self) -> Option<i64> {
        self.entry.periodic
    }
    pub fn admission(&self) -> Admission {
        self.entry.admission
    }
    pub fn start(&self) -> DateTime<Utc> {
        self.message().start()
    }
    pub fn end(&self) -> DateTime<Utc> {
        self.message().end()
    }
}
impl NormalRecordingPublications {
    pub fn matching(&self, raw: &[u8]) -> Result<Option<NormalRecordingPublication>, Error> {
        let parsed = clocked_recording_envelope::parse_normal_head(raw)
            .map_err(|_| Error::Unsupported("normal recording envelope"))?;
        let entry = self
            .inner
            .entries
            .lock()
            .map_err(|_| Error::Supervisor)?
            .get(&parsed.task_id())
            .cloned();
        let Some(entry) = entry else { return Ok(None) };
        if entry.message != parsed || entry.message.raw_frame() != raw {
            return Err(Error::Unsupported("publication identity/bytes"));
        }
        Ok(Some(NormalRecordingPublication {
            entry,
            origin: self.inner.clone(),
        }))
    }
    pub fn complete(&self, proof: &NormalRecordingPublication) -> Result<(), Error> {
        if !Arc::ptr_eq(&self.inner, &proof.origin) {
            return Err(Error::Unsupported("foreign publication registry"));
        }
        let mut entries = self.inner.entries.lock().map_err(|_| Error::Supervisor)?;
        let actual = entries
            .get(&proof.task_id())
            .ok_or(Error::Unsupported("publication missing/completed"))?;
        if !Arc::ptr_eq(actual, &proof.entry) {
            return Err(Error::Unsupported("publication generation"));
        }
        entries.remove(&proof.task_id());
        Ok(())
    }
    pub fn pending(&self) -> Result<usize, Error> {
        Ok(self
            .inner
            .entries
            .lock()
            .map_err(|_| Error::Supervisor)?
            .len())
    }
}
struct Inner {
    booking_artwork: Mutex<Option<crate::transport::ArtworkRootPublisher>>,
    booking_backpressure_closed: AtomicBool,
    pool: PgPool,
    jobs: Jobs,
    registry: NormalRecordingPublications,
    named: crate::normal_named_recording::NormalNamedRecordingPublications,
    slot: Arc<Semaphore>,
    named_admission: Arc<Semaphore>,
    closed: AtomicBool,
    reply: Mutex<BTreeMap<u64, Uuid>>,
    identity: Arc<()>,
}
#[derive(Clone)]
pub struct NormalRecordingPublisher {
    inner: Arc<Inner>,
}
struct Owner {
    _slot: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
    _producer: Arc<Inner>,
}
impl NormalRecordingPublisher {
    pub async fn prepare(
        pool: PgPool,
        broker: &str,
        backend: &str,
    ) -> Result<(Self, NormalRecordingPublications), Error> {
        let registry = NormalRecordingPublications {
            inner: Arc::new(RegistryInner {
                entries: Mutex::new(BTreeMap::new()),
                slots: Arc::new(Semaphore::new(64)),
            }),
        };
        let named = crate::normal_named_recording::NormalNamedRecordingPublications::prepare(
            registry.inner.slots.clone(),
        );
        let jobs = Jobs::connect_recording_dvr(broker, backend, transport::Config::default())
            .await
            .map_err(Error::Transport)?;
        Ok((
            Self {
                inner: Arc::new(Inner {
                    booking_artwork: Mutex::new(None),
                    booking_backpressure_closed: AtomicBool::new(false),
                    pool,
                    jobs,
                    registry: registry.clone(),
                    named,
                    slot: Arc::new(Semaphore::new(1)),
                    named_admission: Arc::new(Semaphore::new(1)),
                    closed: AtomicBool::new(false),
                    reply: Mutex::new(BTreeMap::new()),
                    identity: Arc::new(()),
                }),
            },
            registry,
        ))
    }
    pub fn named_publications(
        &self,
    ) -> crate::normal_named_recording::NormalNamedRecordingPublications {
        self.inner.named.clone()
    }
    /// Caller is the reviewed normal clocked scheduler, not arbitrary task bytes.
    /// Re-read actual due one-off/Recording identity and original args before
    /// publishing. No UUID is retained as completed before a real broker reply.
    pub(crate) async fn publish_due(
        &self,
        periodic: i64,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<crate::protocol::Published, Error> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let slot = self
            .inner
            .slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let owner = Arc::new(Owner {
            _slot: slot,
            _caller: caller,
            _producer: self.inner.clone(),
        });
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let retained = inner
                .registry
                .inner
                .slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| Error::Closed)?;
            let row = due(&inner.pool, periodic, owner.clone()).await?;
            let id = Uuid::new_v4();
            let bytes = encode(&row, id, reply_oid(&inner)?)?;
            let message = clocked_recording_envelope::parse_normal_head(&bytes)
                .map_err(|_| Error::Unsupported("encoded clocked frame"))?;
            let published = inner
                .jobs
                .publish_normal_recording(&message, owner)
                .await
                .map_err(Error::Transport)?;
            let mut entries = inner
                .registry
                .inner
                .entries
                .lock()
                .map_err(|_| Error::Supervisor)?;
            if entries.contains_key(&id) {
                return Err(Error::Unsupported("duplicate publication UUID"));
            }
            entries.insert(
                id,
                Arc::new(Entry {
                    message,
                    periodic: Some(periodic),
                    admission: Admission::Published,
                    _slot: retained,
                }),
            );
            Ok(published)
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    pub(crate) fn uses_pool(&self, pool: &PgPool) -> bool {
        std::ptr::eq(self.inner.pool.options(), pool.options())
    }
    pub(crate) async fn publish_recovery(
        &self,
        row: crate::normal_recording_recovery::InterruptedRecording,
        invocation: &crate::normal_recording_recovery::Invocation,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<String, Error> {
        self.publish_named(
            row.recording_id(),
            row.channel_id(),
            row.recording_start(),
            row.recording_end(),
            invocation.now(),
            crate::normal_named_recording::Kind::Recovery,
            invocation.root(),
            invocation.task_id(),
            owner,
        )
        .await
    }
    pub(crate) async fn publish_immediate_repair(
        &self,
        row: &crate::normal_recording_recovery::UpcomingRecording,
        invocation: &crate::normal_recording_recovery::Invocation,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<String, Error> {
        self.publish_named(
            row.recording_id(),
            row.channel_id(),
            row.recording_start(),
            row.recording_end(),
            row.recording_start(),
            crate::normal_named_recording::Kind::Immediate,
            invocation.root(),
            invocation.task_id(),
            owner,
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    async fn publish_named(
        &self,
        recording: i64,
        channel: i64,
        recording_start: DateTime<Utc>,
        recording_end: DateTime<Utc>,
        argument_start: DateTime<Utc>,
        kind: crate::normal_named_recording::Kind,
        root: Uuid,
        parent: Uuid,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<String, Error> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        // The serialized recovery service has already committed interruption.
        // Ordinary concurrent DVR admission is backpressure, not uncertainty.
        let queued = self
            .inner
            .named_admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let inner = self.inner.clone();
        // One bounded accepted waiter retains its caller after await cancellation.
        tokio::spawn(async move {
            let _queued = queued;
            let slot = inner
                .slot
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| Error::Closed)?;
            if inner.closed.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            let owner = Arc::new(Owner {
                _slot: slot,
                _caller: caller,
                _producer: inner.clone(),
            });
            let pending = inner.named.reserve().await.map_err(|_| Error::Closed)?;
            let key = match kind {
                crate::normal_named_recording::Kind::Recovery => format!("dvr-recover-{recording}"),
                crate::normal_named_recording::Kind::Immediate => {
                    format!("dvr-now-{recording}-{}", Uuid::new_v4().simple())
                }
            };
            let raw = encode_named(
                &key,
                recording,
                channel,
                argument_start,
                recording_end,
                kind,
                root,
                parent,
                reply_oid(&inner)?,
            )?;
            let message = crate::normal_named_recording::parse_head(&raw)
                .map_err(|_| Error::Unsupported("normal named frame"))?;
            let reply = inner
                .jobs
                .publish_named_recording(&message, owner)
                .await
                .map_err(Error::Transport)?;
            inner
                .named
                .record_actual(message, recording_start, recording_end, pending, reply)
                .map_err(|_| Error::Unsupported("named publication identity/generation"))?;
            Ok(key)
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    /// Stop only backpressured booking publications. Already accepted partial
    /// effects propagate Closed to their supervisor, which retains the fault and
    /// owner; this is not a Source-complete result. DVR adoption ports stay open.
    pub fn stop_booking_backpressure(&self) {
        self.inner
            .booking_backpressure_closed
            .store(true, Ordering::Release);
    }
    pub fn close_admission(&self) {
        self.stop_booking_backpressure();
        self.inner.closed.store(true, Ordering::Release);
        self.inner.named_admission.close();
        self.inner.registry.inner.slots.close();
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.close_admission();
        let deadline = tokio::time::Instant::now() + timeout;
        // An accepted detached waiter might not have polled the physical fence.
        // Wait for that real operation before claiming publisher drain.
        while self.inner.named_admission.available_permits() != 1 {
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::DrainTimeout);
            }
            tokio::time::sleep_until(
                (tokio::time::Instant::now() + Duration::from_millis(1)).min(deadline),
            )
            .await;
        }
        let _slot = tokio::time::timeout_at(deadline, self.inner.slot.clone().acquire_owned())
            .await
            .map_err(|_| Error::DrainTimeout)?
            .map_err(|_| Error::Closed)?;
        self.inner
            .jobs
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .map_err(Error::Transport)
    }
}
struct Due {
    recording: i64,
    channel: i64,
    args: Vec<Value>,
    name: String,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}
fn database(error: sqlx::Error) -> Error {
    Error::Database {
        primary: Some(Box::new(error)),
        ready: None,
        close: None,
    }
}
async fn due(pool: &PgPool, periodic: i64, owner: Arc<dyn Send + Sync>) -> Result<Due, Error> {
    if !(1..=i64::from(i32::MAX)).contains(&periodic) {
        return Err(Error::Unsupported("periodic id"));
    }
    let pool = pool.clone();
    tokio::spawn(async move{
  let _owner=owner;let mut c=pool.acquire().await.map_err(database)?.detach();
  let result=async{
   let row=sqlx::query("SELECT p.id::bigint,p.enabled,p.one_off,p.total_run_count::bigint,p.start_time,p.expires,p.expire_seconds::integer,p.priority::integer,cl.clocked_time,r.id::bigint AS recording,r.channel_id::bigint AS channel,r.start_time AS recording_start,r.end_time AS recording_end,CASE WHEN octet_length(p.name)<=128 THEN p.name END AS name,CASE WHEN octet_length(p.task)<=128 THEN p.task END AS task,CASE WHEN octet_length(p.args)<=8192 THEN p.args END AS args,CASE WHEN octet_length(p.kwargs)<=8192 THEN p.kwargs END AS kwargs,CASE WHEN octet_length(p.headers)<=8192 THEN p.headers END AS headers,CASE WHEN COALESCE(octet_length(p.queue),0)<=128 THEN p.queue END AS queue,CASE WHEN COALESCE(octet_length(p.exchange),0)<=128 THEN p.exchange END AS exchange,CASE WHEN COALESCE(octet_length(p.routing_key),0)<=128 THEN p.routing_key END AS routing_key,(COALESCE(octet_length(p.queue),0)<=128 AND COALESCE(octet_length(p.exchange),0)<=128 AND COALESCE(octet_length(p.routing_key),0)<=128) AS options_fit FROM django_celery_beat_periodictask p JOIN django_celery_beat_clockedschedule cl ON cl.id=p.clocked_id JOIN dispatcharr_channels_recording r ON r.task_id=p.name AND p.name='dvr-recording-'||r.id::text WHERE p.id=$1").bind(periodic).fetch_optional(&mut c).await.map_err(database)?.ok_or(Error::Unsupported("actual clocked recording row"))?;
   let now=Utc::now();let start:DateTime<Utc>=row.try_get("recording_start").map_err(database)?;let end:DateTime<Utc>=row.try_get("recording_end").map_err(database)?;let clocked:DateTime<Utc>=row.try_get("clocked_time").map_err(database)?;
   if !row.try_get::<bool,_>("enabled").map_err(database)?||!row.try_get::<bool,_>("one_off").map_err(database)?||row.try_get::<i64,_>("total_run_count").map_err(database)?!=0||clocked>now||clocked!=start||end<=start||row.try_get::<Option<DateTime<Utc>>,_>("start_time").map_err(database)?.is_some_and(|v|v>now)||row.try_get::<Option<DateTime<Utc>>,_>("expires").map_err(database)?.is_some()||row.try_get::<Option<i32>,_>("expire_seconds").map_err(database)?.is_some()||row.try_get::<Option<i32>,_>("priority").map_err(database)?.is_some_and(|v|v!=0)||!row.try_get::<bool,_>("options_fit").map_err(database)?{return Err(Error::Unsupported("normal due one-off/options"));}
   for(field,allowed)in[("queue","dvr"),("exchange",""),("routing_key","dvr")]{if row.try_get::<Option<String>,_>(field).map_err(database)?.as_deref().is_some_and(|v|v!=allowed){return Err(Error::Unsupported("clocked routing"));}}
   if row.try_get::<Option<String>,_>("task").map_err(database)?.as_deref()!=Some("apps.channels.tasks.run_recording"){return Err(Error::Unsupported("clocked task"));}
   for field in ["kwargs","headers"]{let value=row.try_get::<Option<String>,_>(field).map_err(database)?.ok_or(Error::Unsupported("clocked JSON bound"))?;if serde_json::from_str::<Value>(&value).map_err(|_|Error::Unsupported("clocked JSON"))?!=json!({}){return Err(Error::Unsupported("clocked options"));}}
   let args=row.try_get::<Option<String>,_>("args").map_err(database)?.ok_or(Error::Unsupported("clocked args bound"))?;let args:Value=serde_json::from_str(&args).map_err(|_|Error::Unsupported("clocked args JSON"))?;let args=args.as_array().filter(|v|v.len()==4).ok_or(Error::Unsupported("clocked argument arity"))?.clone();
   let recording:i64=row.try_get("recording").map_err(database)?;let channel:i64=row.try_get("channel").map_err(database)?;
   if !(1..=i64::from(i32::MAX)).contains(&recording)||!(1..=i64::from(i32::MAX)).contains(&channel)||args[0]!=json!(recording)||args[1]!=json!(channel){return Err(Error::Unsupported("recording/channel argument identity"));}
   Ok(Due{recording,channel,args,name:row.try_get::<Option<String>,_>("name").map_err(database)?.ok_or(Error::Unsupported("periodic name bound"))?,start,end})
  }.await;
  let ready=c.ping().await.err().map(Box::new);let close=c.close().await.err().map(Box::new);
  match result{Err(Error::Database{primary,..})=>Err(Error::Database{primary,ready,close}),other=>if ready.is_some()||close.is_some(){Err(Error::Database{primary:None,ready,close})}else{other}}
 }).await.map_err(|_|Error::Supervisor)?
}
fn encode(row: &Due, id: Uuid, reply: Uuid) -> Result<Vec<u8>, Error> {
    let base = crate::protocol::encode(
        &crate::protocol::Task::CreateBackup,
        id,
        reply,
        Uuid::new_v4(),
        &origin()?,
    )
    .map_err(|_| Error::Unsupported("clocked common frame"))?;
    let mut frame: Value =
        serde_json::from_slice(&base).map_err(|_| Error::Unsupported("clocked common JSON"))?;
    let start = row.args[2]
        .as_str()
        .ok_or(Error::Unsupported("clocked start string"))?;
    let end = row.args[3]
        .as_str()
        .ok_or(Error::Unsupported("clocked end string"))?;
    if start.len() > 40 || end.len() > 40 {
        return Err(Error::Unsupported("clocked date bound"));
    }
    frame["headers"]["task"] = json!("apps.channels.tasks.run_recording");
    frame["headers"]["periodic_task_name"] = json!(row.name);
    frame["headers"]["argsrepr"] = json!(format!(
        "[{}, {}, '{start}', '{end}']",
        row.recording, row.channel
    ));
    frame["properties"]["delivery_info"]["routing_key"] = json!("dvr");
    frame["body"] = json!(STANDARD.encode(
        serde_json::to_vec(
            &json!([row.args,{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        )
        .map_err(|_| Error::Unsupported("clocked body"))?
    ));
    let raw = serde_json::to_vec(&frame).map_err(|_| Error::Unsupported("clocked frame"))?;
    let parsed = clocked_recording_envelope::parse_normal_head(&raw)
        .map_err(|_| Error::Unsupported("normal clocked dates/envelope"))?;
    if parsed.recording_id() != row.recording
        || parsed.channel_id() != row.channel
        || parsed.start() != row.start
        || parsed.end() != row.end
        || parsed.periodic_name() != row.name
    {
        return Err(Error::Unsupported("actual SQL/date identity"));
    }
    Ok(raw)
}

fn reply_oid(inner: &Inner) -> Result<Uuid, Error> {
    static NODE: std::sync::OnceLock<u128> = std::sync::OnceLock::new();
    let node = *NODE.get_or_init(|| Uuid::new_v4().as_u128());
    // Pinned CPython's threading.get_ident() on this Linux pthread build uses
    // pthread_self. Cache one actual app-instance/thread identity like thread_oid.
    let thread = unsafe { libc::pthread_self() } as u64;
    let mut replies = inner.reply.lock().map_err(|_| Error::Supervisor)?;
    if let Some(reply) = replies.get(&thread) {
        return Ok(*reply);
    }
    if replies.len() >= 256 {
        return Err(Error::Capacity);
    }
    let instance = Arc::as_ptr(&inner.identity) as usize;
    let entity = format!("{node:x}-{:x}-{thread:x}-{instance:x}", std::process::id());
    let reply = Uuid::new_v3(&Uuid::NAMESPACE_OID, entity.as_bytes());
    replies.insert(thread, reply);
    Ok(reply)
}
fn origin() -> Result<String, Error> {
    let mut bytes = [0u8; 256];
    if unsafe { libc::gethostname(bytes.as_mut_ptr().cast(), bytes.len()) } != 0 {
        return Err(Error::Unsupported("actual hostname"));
    }
    let size = bytes
        .iter()
        .position(|byte| *byte == 0)
        .ok_or(Error::Unsupported("hostname bound"))?;
    let hostname =
        std::str::from_utf8(&bytes[..size]).map_err(|_| Error::Unsupported("hostname encoding"))?;
    Ok(format!("gen{}@{hostname}", std::process::id()))
}

pub(crate) fn python_datetime(value: DateTime<Utc>, iso: bool) -> String {
    let base = value.format(if iso {
        "%Y-%m-%dT%H:%M:%S"
    } else {
        "%Y-%m-%d %H:%M:%S"
    });
    if value.timestamp_subsec_micros() == 0 {
        format!("{base}+00:00")
    } else {
        format!("{base}.{:06}+00:00", value.timestamp_subsec_micros())
    }
}
#[allow(clippy::too_many_arguments)]
fn encode_named(
    key: &str,
    recording: i64,
    channel: i64,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    kind: crate::normal_named_recording::Kind,
    root: Uuid,
    parent: Uuid,
    reply: Uuid,
) -> Result<Vec<u8>, Error> {
    let start_text = python_datetime(start, false);
    let end_text = python_datetime(end, false);
    let base = crate::protocol::encode(
        &crate::protocol::Task::CreateBackup,
        Uuid::new_v4(),
        reply,
        Uuid::new_v4(),
        &origin()?,
    )
    .map_err(|_| Error::Unsupported("named common frame"))?;
    let mut frame: Value =
        serde_json::from_slice(&base).map_err(|_| Error::Unsupported("named common JSON"))?;
    frame["headers"]["task"] = json!("apps.channels.tasks.run_recording");
    frame["headers"]["id"] = json!(key);
    frame["headers"]["root_id"] = json!(root.to_string());
    frame["headers"]["parent_id"] = json!(parent.to_string());
    frame["headers"]["argsrepr"] = json!(format!(
        "[{recording}, {channel}, '{start_text}', '{end_text}']"
    ));
    frame["headers"]["eta"] = if kind == crate::normal_named_recording::Kind::Recovery {
        json!(python_datetime(start, true))
    } else {
        Value::Null
    };
    frame["properties"]["correlation_id"] = json!(key);
    frame["properties"]["delivery_info"]["routing_key"] = json!("dvr");
    frame["body"]=json!(STANDARD.encode(serde_json::to_vec(&json!([[recording,channel,start_text,end_text],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).map_err(|_|Error::Unsupported("named body"))?));
    let raw = serde_json::to_vec(&frame).map_err(|_| Error::Unsupported("named frame"))?;
    crate::normal_named_recording::parse_head(&raw)
        .map_err(|_| Error::Unsupported("named envelope identity"))?;
    Ok(raw)
}

impl NormalRecordingPublisher {
    /// Adopt a legitimate surviving Source/Native DVR head after independent
    /// authenticated broker observation and actual bounded SQL identity check.
    /// This is not a fabricated publication: same-process/new-parent completion
    /// provenance is deliberately not required for Source Celery compatibility.
    pub async fn adopt_existing(
        &self,
        expected: Arc<[u8]>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
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
            _slot: slot,
            _caller: caller,
            _producer: self.inner.clone(),
        });
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let identity: serde_json::Value = serde_json::from_slice(&expected)
                .map_err(|_| Error::Unsupported("DVR envelope"))?;
            let task_key = identity["headers"]["id"]
                .as_str()
                .ok_or(Error::Unsupported("DVR identity"))?;
            let named = Uuid::parse_str(task_key).is_err();
            if named {
                if inner
                    .named
                    .matching(&expected)
                    .map_err(|_| Error::Unsupported("named recording frame"))?
                    .is_some()
                {
                    return Ok(());
                }
            } else if inner.registry.matching(&expected)?.is_some() {
                return Ok(());
            }
            let retained = inner
                .registry
                .inner
                .slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| Error::Closed)?;
            let observed = inner
                .jobs
                .observe_existing_recording(&expected, owner.clone())
                .await
                .map_err(Error::Transport)?;
            if named {
                let message = crate::normal_named_recording::parse_head(&expected)
                    .map_err(|_| Error::Unsupported("named DVR frame"))?;
                let row = adoption_row(
                    inner.pool.clone(),
                    message.recording_id(),
                    None,
                    owner.clone(),
                )
                .await?;
                let (start, end, admission) = match row {
                    Some(row) => {
                        // This is historical actual row evidence, not a gate
                        // requiring Source's published arguments to remain equal
                        // after update/extend/recovery. The consumer separately
                        // rereads live row + argument Channel before effects.
                        (row.start, row.end, Admission::ObservedRecording)
                    }
                    None => (
                        message.argument_start(),
                        message.argument_end(),
                        Admission::ObservedAbsence,
                    ),
                };
                inner
                    .named
                    .adopt_observed(message, start, end, admission, retained, observed)
                    .map_err(|_| Error::Unsupported("named adoption generation"))?;
            } else {
                let message = clocked_recording_envelope::parse_normal_head(&expected)
                    .map_err(|_| Error::Unsupported("clocked DVR frame"))?;
                let row = adoption_row(
                    inner.pool.clone(),
                    message.recording_id(),
                    Some(message.periodic_name().into()),
                    owner,
                )
                .await?;
                if !observed.confirms(message.raw_frame()) {
                    return Err(Error::Unsupported("clocked broker observation"));
                }
                let (periodic, admission) = match row {
                    Some(row) => (row.periodic, Admission::ObservedRecording),
                    None => (None, Admission::ObservedAbsence),
                };
                let mut entries = inner
                    .registry
                    .inner
                    .entries
                    .lock()
                    .map_err(|_| Error::Supervisor)?;
                if let Some(existing) = entries.get(&message.task_id()) {
                    if existing.message != message {
                        return Err(Error::Unsupported("clocked adoption identity"));
                    }
                } else {
                    entries.insert(
                        message.task_id(),
                        Arc::new(Entry {
                            message,
                            periodic,
                            admission,
                            _slot: retained,
                        }),
                    );
                }
            }
            Ok(())
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
}
struct AdoptionRow {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    periodic: Option<i64>,
}
async fn adoption_row(
    pool: PgPool,
    recording: i64,
    periodic: Option<String>,
    owner: Arc<dyn Send + Sync>,
) -> Result<Option<AdoptionRow>, Error> {
    tokio::spawn(async move {let _owner=owner;let mut connection=pool.acquire().await.map_err(database)?.detach();
  let observed=async {
   let row=sqlx::query("SELECT channel_id::bigint,start_time,end_time FROM dispatcharr_channels_recording WHERE id=$1").bind(recording).fetch_optional(&mut connection).await.map_err(database)?;
   let Some(row)=row else{return Ok(None);};
   let start=row.try_get("start_time").map_err(database)?;let end=row.try_get("end_time").map_err(database)?;
   let periodic=if let Some(name)=periodic {
    // A PeriodicTask may legitimately have changed/disappeared since Beat
    // published this authenticated raw. Its current ID is optional provenance,
    // never a requirement to consume a surviving Source frame.
    let ids=sqlx::query_scalar::<_,i64>("SELECT id::bigint FROM django_celery_beat_periodictask WHERE name=$1 LIMIT 2").bind(name).fetch_all(&mut connection).await.map_err(database)?;
    if ids.len()>1{return Err(Error::Unsupported("ambiguous periodic schedule name"));}ids.into_iter().next()
   }else{None};Ok(Some(AdoptionRow{start,end,periodic}))
  }.await;
  let ready=connection.ping().await.err().map(Box::new);let close=connection.close().await.err().map(Box::new);
  match observed {Err(Error::Database{primary,..})=>Err(Error::Database{primary,ready,close}),other=>if ready.is_some()||close.is_some(){Err(Error::Database{primary:None,ready,close})}else{other}}
 }).await.map_err(|_|Error::Supervisor)?
}

impl NormalRecordingPublisher {
    pub(crate) fn startup_frame(&self, task: &str, id: Uuid) -> Result<Vec<u8>, Error> {
        startup_frame(task, id, reply_oid(&self.inner)?, &origin()?)
    }
}

pub(crate) fn startup_frame(
    task: &str,
    id: Uuid,
    reply_to: Uuid,
    origin: &str,
) -> Result<Vec<u8>, Error> {
    if !matches!(
        task,
        "apps.channels.tasks.recover_recordings_on_startup" | crate::version_protocol::TASK
    ) {
        return Err(Error::Unsupported("startup task"));
    }
    let raw = crate::protocol::encode(
        &crate::protocol::Task::CreateBackup,
        id,
        reply_to,
        Uuid::new_v4(),
        origin,
    )
    .map_err(|_| Error::Unsupported("startup frame"))?;
    let mut value: Value =
        serde_json::from_slice(&raw).map_err(|_| Error::Unsupported("startup frame"))?;
    value["headers"]["task"] = json!(task);
    serde_json::to_vec(&value).map_err(|_| Error::Unsupported("startup frame"))
}

impl NormalRecordingPublisher {
    /// Bounded authenticated broker read for direct normal run_once. This grants
    /// no claim/publication authority; adopt_existing and consumer re-peek follow.
    pub async fn peek_existing(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<Arc<[u8]>>, Error> {
        self.inner
            .jobs
            .peek_existing_recording(owner)
            .await
            .map(|value| value.map(Arc::from))
            .map_err(Error::Transport)
    }
}

#[path = "normal_booking_publication.rs"]
mod normal_booking_publication;
pub use normal_booking_publication::{BookingPublication, BookingTask};

impl NormalRecordingPublisher {
    /// Compose the actual Main artwork Jobs, once and before HTTP producers.
    /// The typed port exposes no arbitrary public frame/parent minting method.
    pub fn set_booking_artwork(
        &self,
        publisher: crate::transport::ArtworkRootPublisher,
    ) -> Result<(), Error> {
        let mut current = self
            .inner
            .booking_artwork
            .lock()
            .map_err(|_| Error::Supervisor)?;
        if current.is_some() {
            return Err(Error::Unsupported(
                "booking artwork publisher already composed",
            ));
        }
        *current = Some(publisher);
        Ok(())
    }
}

#[cfg(test)]
mod controls {
    use super::*;
    fn text(value: DateTime<Utc>) -> String {
        let base = value.format("%Y-%m-%d %H:%M:%S");
        if value.timestamp_subsec_micros() == 0 {
            format!("{base}+00:00")
        } else {
            format!("{base}.{:06}+00:00", value.timestamp_subsec_micros())
        }
    }
    #[test]
    fn actual_shape_is_generic_and_preserves_source_datetime_precision() {
        for micros in [0, 123456] {
            let start = DateTime::from_timestamp(1800000000, micros * 1000).unwrap();
            let end = start + chrono::Duration::seconds(347);
            let row = Due {
                recording: 17,
                channel: 42,
                args: vec![json!(17), json!(42), json!(text(start)), json!(text(end))],
                name: "dvr-recording-17".into(),
                start,
                end,
            };
            let raw = encode(
                &row,
                Uuid::new_v4(),
                Uuid::new_v3(&Uuid::NAMESPACE_OID, b"normal-clocked-control"),
            )
            .unwrap();
            let message = clocked_recording_envelope::parse_normal_head(&raw).unwrap();
            assert_eq!(message.recording_id(), 17);
            assert_eq!(message.channel_id(), 42);
            assert_eq!(message.start(), start);
            assert_eq!(message.end(), end);
            assert_eq!(
                clocked_recording_envelope::parse_head(&raw).is_ok(),
                micros == 0,
                "legacy parser is unchanged"
            );
            let mut wrong = row;
            wrong.args[1] = json!(99);
            assert!(encode(
                &wrong,
                Uuid::new_v4(),
                Uuid::new_v3(&Uuid::NAMESPACE_OID, b"control")
            )
            .is_err());
        }
    }
}

#[cfg(test)]
#[path = "normal_recovery_publication_peer.rs"]
mod normal_recovery_publication_peer;
