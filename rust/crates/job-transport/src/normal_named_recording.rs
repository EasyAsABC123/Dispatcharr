//! Additive named immediate/recovery recording data and sealed publication ledger.
//! Original Dispatcharr/Celery/Kombu authors retain all credit. Named task keys,
//! root/parent/correlation and backend identity are NEVER rewritten to UUIDs.
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Immediate,
    Recovery,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    raw: Vec<u8>,
    task_key: String,
    root: String,
    parent: Option<String>,
    recording: i64,
    channel: i64,
    argument_start: DateTime<Utc>,
    argument_end: DateTime<Utc>,
    eta: Option<DateTime<Utc>>,
    kind: Kind,
}
impl Message {
    pub fn raw_frame(&self) -> &[u8] {
        &self.raw
    }
    pub fn task_key(&self) -> &str {
        &self.task_key
    }
    pub fn root(&self) -> &str {
        &self.root
    }
    pub fn parent(&self) -> Option<&str> {
        self.parent.as_deref()
    }
    pub fn recording_id(&self) -> i64 {
        self.recording
    }
    pub fn channel_id(&self) -> i64 {
        self.channel
    }
    pub fn argument_start(&self) -> DateTime<Utc> {
        self.argument_start
    }
    pub fn argument_end(&self) -> DateTime<Utc> {
        self.argument_end
    }
    pub fn eta(&self) -> Option<DateTime<Utc>> {
        self.eta
    }
    pub fn kind(&self) -> Kind {
        self.kind
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    Capacity,
    Malformed,
    Identity,
    Envelope,
    Argument,
    Date,
}
fn uuid4(text: &str) -> Option<Uuid> {
    let value = Uuid::parse_str(text).ok()?;
    (value.to_string() == text
        && value.get_version_num() == 4
        && value.get_variant() == uuid::Variant::RFC4122)
        .then_some(value)
}
/// Pure observation only. Execution requires matching exact bytes in the actual
/// SQL→settled-publication ledger. Legacy clocked UUID parsing stays unchanged.
pub fn parse_head(raw: &[u8]) -> Result<Message, Refusal> {
    if raw.is_empty() || raw.len() > crate::clocked_recording_envelope::MAX_FRAME_BYTES {
        return Err(Refusal::Capacity);
    }
    let mut value: Value =
        crate::clocked_recording_envelope::json_value(raw).map_err(|_| Refusal::Malformed)?;
    let h = &value["headers"];
    let task_key = h["id"]
        .as_str()
        .filter(|s| s.len() <= 96)
        .ok_or(Refusal::Identity)?
        .to_owned();
    if h.get("periodic_task_name").is_some() || value["properties"]["correlation_id"] != task_key {
        return Err(Refusal::Envelope);
    }
    let body = STANDARD
        .decode(value["body"].as_str().ok_or(Refusal::Malformed)?)
        .map_err(|_| Refusal::Malformed)?;
    if body.len() > crate::clocked_recording_envelope::MAX_BODY_BYTES {
        return Err(Refusal::Capacity);
    }
    let body: Value =
        crate::clocked_recording_envelope::json_value(&body).map_err(|_| Refusal::Malformed)?;
    let recording = body
        .get(0)
        .and_then(|v| v.get(0))
        .and_then(Value::as_i64)
        .filter(|v| (1..=i64::from(i32::MAX)).contains(v))
        .ok_or(Refusal::Argument)?;
    let recovery = format!("dvr-recover-{recording}");
    let immediate = format!("dvr-now-{recording}-");
    let (kind, nonce) = if task_key == recovery {
        (Kind::Recovery, None)
    } else if let Some(suffix) = task_key.strip_prefix(&immediate) {
        let nonce = Uuid::parse_str(suffix).map_err(|_| Refusal::Identity)?;
        if suffix != nonce.simple().to_string()
            || nonce.get_version_num() != 4
            || nonce.get_variant() != uuid::Variant::RFC4122
        {
            return Err(Refusal::Identity);
        }
        (Kind::Immediate, Some(nonce))
    } else {
        return Err(Refusal::Identity);
    };
    let root = h["root_id"]
        .as_str()
        .filter(|s| s.len() <= 96)
        .ok_or(Refusal::Identity)?
        .to_owned();
    let parent = if h["parent_id"].is_null() {
        None
    } else {
        Some(h["parent_id"].as_str().ok_or(Refusal::Identity)?.to_owned())
    };
    let validation_id = match &parent {
        Some(parent) => {
            uuid4(parent).ok_or(Refusal::Identity)?;
            uuid4(&root).ok_or(Refusal::Identity)?
        }
        None if kind == Kind::Immediate && root == task_key => nonce.ok_or(Refusal::Identity)?,
        _ => return Err(Refusal::Identity),
    };
    let eta_text = if h["eta"].is_null() {
        None
    } else {
        Some(h["eta"].as_str().ok_or(Refusal::Date)?.to_owned())
    };
    if (kind == Kind::Recovery) != eta_text.is_some() {
        return Err(Refusal::Date);
    }
    // Reuse only the common bounded envelope validator. Original named identity
    // remains in Message/raw/backend; this temporary validation projection is not
    // a publication, accepted frame, UUID alias, or minted capability.
    value["headers"]["id"] = json!(validation_id.to_string());
    value["headers"]["root_id"] = json!(validation_id.to_string());
    value["headers"]["parent_id"] = Value::Null;
    value["headers"]["eta"] = Value::Null;
    value["headers"]["periodic_task_name"] = json!(format!("dvr-recording-{recording}"));
    value["properties"]["correlation_id"] = json!(validation_id.to_string());
    let projected = serde_json::to_vec(&value).map_err(|_| Refusal::Malformed)?;
    let common = crate::clocked_recording_envelope::parse_normal_head(&projected)
        .map_err(|_| Refusal::Envelope)?;
    let eta = if let Some(text) = eta_text {
        let original = body[0][2].as_str().ok_or(Refusal::Date)?;
        if text != original.replacen(' ', "T", 1) {
            return Err(Refusal::Date);
        }
        let eta: DateTime<Utc> = text.parse().map_err(|_| Refusal::Date)?;
        if eta != common.start() {
            return Err(Refusal::Date);
        }
        Some(eta)
    } else {
        None
    };
    Ok(Message {
        raw: raw.to_vec(),
        task_key,
        root,
        parent,
        recording: common.recording_id(),
        channel: common.channel_id(),
        argument_start: common.start(),
        argument_end: common.end(),
        eta,
        kind,
    })
}
struct Entry {
    message: Message,
    recording_start: DateTime<Utc>,
    recording_end: DateTime<Utc>,
    admission: crate::normal_recording_publication::Admission,
    _slot: OwnedSemaphorePermit,
}
struct Inner {
    entries: Mutex<BTreeMap<String, Arc<Entry>>>,
    slots: Arc<Semaphore>,
}
#[derive(Clone)]
pub struct NormalNamedRecordingPublications {
    inner: Arc<Inner>,
}
#[derive(Clone)]
pub struct NormalNamedRecordingPublication {
    entry: Arc<Entry>,
    origin: Arc<Inner>,
}
#[derive(Debug)]
pub enum Error {
    Closed,
    Identity,
    Capacity,
    Poisoned,
}
impl NormalNamedRecordingPublication {
    pub fn message(&self) -> &Message {
        &self.entry.message
    }
    pub fn task_key(&self) -> &str {
        self.message().task_key()
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
    pub fn admission(&self) -> crate::normal_recording_publication::Admission {
        self.entry.admission
    }
    /// Actual row window for published/observed rows; original task argument
    /// window only for ObservedAbsence. The consumer must separately observe
    /// actual SQL absence before terminal None/REVOKED and perform no media.
    pub fn recording_start(&self) -> DateTime<Utc> {
        self.entry.recording_start
    }
    pub fn recording_end(&self) -> DateTime<Utc> {
        self.entry.recording_end
    }
}
impl NormalNamedRecordingPublications {
    pub(crate) fn prepare(slots: Arc<Semaphore>) -> Self {
        Self {
            inner: Arc::new(Inner {
                entries: Mutex::new(BTreeMap::new()),
                slots,
            }),
        }
    }
    /// Broker adoption is distinct from newly published provenance. The only
    /// caller has independently read actual SQL identity and receives a sealed
    /// authenticated exact-head observation; no caller-supplied parent witness.
    pub(crate) fn adopt_observed(
        &self,
        message: Message,
        recording_start: DateTime<Utc>,
        recording_end: DateTime<Utc>,
        admission: crate::normal_recording_publication::Admission,
        slot: OwnedSemaphorePermit,
        observed: crate::transport::ExistingRecordingHead,
    ) -> Result<(), Error> {
        if !observed.confirms(message.raw_frame()) {
            return Err(Error::Identity);
        }
        let mut entries = self.inner.entries.lock().map_err(|_| Error::Poisoned)?;
        if let Some(existing) = entries.get(message.task_key()) {
            return if existing.message == message {
                Ok(())
            } else {
                Err(Error::Identity)
            };
        }
        entries.insert(
            message.task_key().into(),
            Arc::new(Entry {
                message,
                recording_start,
                recording_end,
                admission,
                _slot: slot,
            }),
        );
        Ok(())
    }
    pub fn matching(&self, raw: &[u8]) -> Result<Option<NormalNamedRecordingPublication>, Error> {
        let message = parse_head(raw).map_err(|_| Error::Identity)?;
        let entry = self
            .inner
            .entries
            .lock()
            .map_err(|_| Error::Poisoned)?
            .get(message.task_key())
            .cloned();
        let Some(entry) = entry else { return Ok(None) };
        if entry.message != message {
            return Err(Error::Identity);
        }
        Ok(Some(NormalNamedRecordingPublication {
            entry,
            origin: self.inner.clone(),
        }))
    }
    pub fn complete(&self, proof: &NormalNamedRecordingPublication) -> Result<(), Error> {
        if !Arc::ptr_eq(&self.inner, &proof.origin) {
            return Err(Error::Identity);
        }
        let mut entries = self.inner.entries.lock().map_err(|_| Error::Poisoned)?;
        let actual = entries.get(proof.task_key()).ok_or(Error::Identity)?;
        if !Arc::ptr_eq(actual, &proof.entry) {
            return Err(Error::Identity);
        }
        entries.remove(proof.task_key());
        Ok(())
    }
    pub fn pending(&self) -> Result<usize, Error> {
        Ok(self
            .inner
            .entries
            .lock()
            .map_err(|_| Error::Poisoned)?
            .len())
    }
    pub(crate) async fn reserve(&self) -> Result<OwnedSemaphorePermit, Error> {
        self.inner
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Closed)
    }
    /// Only the actual normal SQL + settled producer calls this internal method;
    /// no public arbitrary-frame constructor exists. Named reply is sealed by its
    /// private actual broker operation, not invented by a consumer or test peer.
    pub(crate) fn record_actual(
        &self,
        message: Message,
        recording_start: DateTime<Utc>,
        recording_end: DateTime<Utc>,
        slot: OwnedSemaphorePermit,
        reply: crate::transport::NamedPublicationReply,
    ) -> Result<(), Error> {
        if !reply.confirms(&message)
            || recording_end != message.argument_end()
            || recording_end <= recording_start
        {
            return Err(Error::Identity);
        }
        let mut entries = self.inner.entries.lock().map_err(|_| Error::Poisoned)?;
        if entries.len() >= 64 || entries.contains_key(message.task_key()) {
            return Err(Error::Capacity);
        }
        entries.insert(
            message.task_key.clone(),
            Arc::new(Entry {
                message,
                recording_start,
                recording_end,
                admission: crate::normal_recording_publication::Admission::Published,
                _slot: slot,
            }),
        );
        Ok(())
    }
}
