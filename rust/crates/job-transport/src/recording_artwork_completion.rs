//! Bounded actual Series/recurring report→backend→artwork publication provenance.
//! Original Dispatcharr authors retain application and algorithm credit.
use crate::{
    configured_recording_service::{RecurringReport, SeriesReport},
    default_protocol::{self, Kind},
    recording_artwork::{DirectTask, RefreshSeriesChild},
    recording_artwork_protocol::{Message, SeriesArtworkChild},
    series_protocol::SeriesMessage,
    transport::Error,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
enum Parent {
    Series {
        message: SeriesMessage,
        refresh: Option<RefreshSeriesChild>,
        broker_observed: bool,
        report: SeriesReport,
    },
    Recurring {
        message: default_protocol::Message,
        report: RecurringReport,
    },
}
impl Parent {
    fn identity(&self) -> (Uuid, Uuid, Option<Uuid>) {
        match self {
            Self::Series { message, .. } => (message.task_id, message.root, message.parent),
            Self::Recurring { message, .. } => (message.task_id, message.task_id, None),
        }
    }
    fn targets(&self) -> &[(Uuid, i64)] {
        match self {
            Self::Series { report, .. } => report.artwork_targets(),
            Self::Recurring { report, .. } => report.artwork_targets(),
        }
    }
    fn frames(&self) -> &[Message] {
        match self {
            Self::Series { report, .. } => report.artwork_frames(),
            Self::Recurring { report, .. } => report.artwork_frames(),
        }
    }
    fn result(&self) -> serde_json::Value {
        match self {
            Self::Series { report, .. } => report.value(),
            Self::Recurring { report, .. } => report.value(),
        }
    }
    fn children(&self) -> Vec<Uuid> {
        match self {
            Self::Series { report, .. } => report.children(),
            Self::Recurring { report, .. } => report.children(),
        }
    }
    fn valid(&self) -> bool {
        match self {
            Self::Series {
                message,
                refresh,
                broker_observed,
                report,
            } => {
                report.matches(message)
                    && match refresh {
                        Some(cap) => cap.admit(message).is_some(),
                        None if *broker_observed => DirectTask::observed_series(message).is_some(),
                        None => message.parent.is_none() && DirectTask::series(message).is_some(),
                    }
            }
            Self::Recurring { message, report } => {
                message.kind == Kind::RecurringMaintenance
                    && report.task_id() == message.task_id
                    && DirectTask::recurring(message).is_some()
            }
        }
    }
}
struct Entry {
    parent: Parent,
    backend: Vec<u8>,
    remaining: Mutex<BTreeSet<Uuid>>,
    _permit: OwnedSemaphorePermit,
}
struct Inner {
    entries: Mutex<BTreeMap<Uuid, Arc<Entry>>>,
    slots: Arc<Semaphore>,
}
#[derive(Clone)]
pub struct Completions {
    inner: Arc<Inner>,
}
pub(crate) struct Proof {
    message: Message,
}
impl Proof {
    pub(crate) fn admits(&self, message: &Message) -> bool {
        self.message == *message
    }
}
impl Default for Completions {
    fn default() -> Self {
        Self::new()
    }
}
impl Completions {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                entries: Mutex::new(BTreeMap::new()),
                slots: Arc::new(Semaphore::new(64)),
            }),
        }
    }
    /// Existing parented-Series entry point remains narrow and unchanged.
    pub(crate) async fn record(
        &self,
        parent: SeriesMessage,
        refresh: RefreshSeriesChild,
        report: SeriesReport,
        backend: Vec<u8>,
    ) -> Result<(), Error> {
        self.record_parent(
            Parent::Series {
                message: parent,
                refresh: Some(refresh),
                broker_observed: false,
                report,
            },
            backend,
        )
        .await
    }
    pub(crate) async fn record_direct(
        &self,
        parent: SeriesMessage,
        report: SeriesReport,
        backend: Vec<u8>,
    ) -> Result<(), Error> {
        self.record_parent(
            Parent::Series {
                message: parent,
                refresh: None,
                broker_observed: false,
                report,
            },
            backend,
        )
        .await
    }
    /// Selected countdown children settle through the broker-owned artwork path.
    /// Validate the parent report here without creating a legacy child permit.
    pub(crate) fn verify_broker_series(
        &self,
        message: SeriesMessage,
        report: SeriesReport,
        backend: &[u8],
    ) -> Result<(), Error> {
        verify_parent(
            &Parent::Series {
                message,
                refresh: None,
                broker_observed: true,
                report,
            },
            backend,
        )
    }
    pub(crate) fn verify_recurring(
        &self,
        message: default_protocol::Message,
        report: RecurringReport,
        backend: &[u8],
    ) -> Result<(), Error> {
        verify_parent(&Parent::Recurring { message, report }, backend)
    }
    async fn record_parent(&self, parent: Parent, backend: Vec<u8>) -> Result<(), Error> {
        verify_parent(&parent, &backend)?;
        if parent.targets().is_empty() {
            return Ok(());
        }
        let permit = self
            .inner
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Closed)?;
        let remaining = parent
            .targets()
            .iter()
            .map(|(id, _)| *id)
            .collect::<BTreeSet<_>>();
        let id = parent.identity().0;
        let mut entries = self.inner.entries.lock().map_err(|_| Error::Protocol)?;
        if entries.contains_key(&id) {
            return Err(Error::Protocol);
        }
        entries.insert(
            id,
            Arc::new(Entry {
                parent,
                backend,
                remaining: Mutex::new(remaining),
                _permit: permit,
            }),
        );
        Ok(())
    }
    pub(crate) fn admit(&self, message: &Message) -> Result<Option<Proof>, Error> {
        let entry = self
            .inner
            .entries
            .lock()
            .map_err(|_| Error::Protocol)?
            .get(&message.parent)
            .cloned();
        let Some(entry) = entry else { return Ok(None) };
        if !entry
            .remaining
            .lock()
            .map_err(|_| Error::Protocol)?
            .contains(&message.task_id)
        {
            return Ok(None);
        }
        validate(&entry.parent, &entry.backend, message)
            .map(Some)
            .ok_or(Error::Protocol)
    }
    pub(crate) fn complete(&self, message: &Message) -> Result<(), Error> {
        let mut entries = self.inner.entries.lock().map_err(|_| Error::Protocol)?;
        let entry = entries.get(&message.parent).ok_or(Error::Protocol)?;
        let empty = {
            let mut remaining = entry.remaining.lock().map_err(|_| Error::Protocol)?;
            if !remaining.remove(&message.task_id) {
                return Err(Error::Protocol);
            }
            remaining.is_empty()
        };
        if empty {
            entries.remove(&message.parent);
        }
        Ok(())
    }
    pub fn close_admission(&self) {
        self.inner.slots.close();
    }
}
fn verify_parent(parent: &Parent, backend: &[u8]) -> Result<(), Error> {
    if !parent.valid() {
        return Err(Error::Protocol);
    }
    if parent.targets().is_empty() {
        return Ok(());
    }
    if parent.targets().len() > 64 {
        return Err(Error::Capacity);
    }
    let first = parent.frames().first().ok_or(Error::Protocol)?;
    validate(parent, backend, first).ok_or(Error::Protocol)?;
    Ok(())
}
fn validate(parent: &Parent, backend: &[u8], child: &Message) -> Option<Proof> {
    if !parent.valid() {
        return None;
    }
    // Retain the older qualified parented constructor, rather than silently
    // widening its root!=parent rule to cover direct/recurring tasks.
    if let Parent::Series {
        message,
        refresh: Some(refresh),
        report,
        ..
    } = parent
    {
        SeriesArtworkChild::from_completed_report(*refresh, message, report, backend, child)?;
        return Some(Proof {
            message: child.clone(),
        });
    }
    let (task, root, metadata_parent) = parent.identity();
    if child.parent != task
        || child.root != root
        || !parent.targets().contains(&(child.task_id, child.recording))
        || !parent.frames().contains(child)
    {
        return None;
    }
    let children = parent.children();
    if children.len() != parent.targets().len()
        || children.len() != parent.frames().len()
        || children.len() > 64
    {
        return None;
    }
    let mut unique = BTreeSet::new();
    for (id, recording) in parent.targets() {
        if !unique.insert(*id)
            || *id == task
            || *id == root
            || !(1..=i64::from(i32::MAX)).contains(recording)
            || !children.contains(id)
        {
            return None;
        }
        let frames = parent
            .frames()
            .iter()
            .filter(|frame| frame.task_id == *id)
            .collect::<Vec<_>>();
        if frames.len() != 1
            || frames[0].recording != *recording
            || frames[0].parent != task
            || frames[0].root != root
        {
            return None;
        }
    }
    let decoded = crate::decode::decode(backend, task).ok()?;
    if decoded.state != crate::decode::State::Success
        || decoded.traceback.is_some()
        || decoded.date_done.is_none()
        || !decoded.tagged_values.is_empty()
        || decoded.result != parent.result()
    {
        return None;
    }
    let raw: serde_json::Value = serde_json::from_slice(backend).ok()?;
    let fields = raw.as_object()?;
    let allowed = [
        "status",
        "result",
        "traceback",
        "children",
        "date_done",
        "task_id",
        "parent_id",
    ];
    if !(6..=7).contains(&fields.len())
        || fields.keys().any(|key| !allowed.contains(&key.as_str()))
        || raw["task_id"] != task.to_string()
        || raw["parent_id"]
            != metadata_parent
                .map(|id| serde_json::json!(id.to_string()))
                .unwrap_or(serde_json::Value::Null)
        || raw["children"]
            != serde_json::json!(children
                .iter()
                .map(|id| serde_json::json!([[id.to_string(), null], null]))
                .collect::<Vec<_>>())
    {
        return None;
    }
    Some(Proof {
        message: child.clone(),
    })
}
