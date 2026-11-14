//! Same-process, bounded normal Series completion provenance.
//! Original Dispatcharr authors retain application and algorithm credit.
use crate::{
    configured_recording_service::SeriesReport,
    recording_artwork::RefreshSeriesChild,
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
struct Entry {
    parent: SeriesMessage,
    refresh: RefreshSeriesChild,
    report: SeriesReport,
    backend: Vec<u8>,
    remaining: Mutex<BTreeSet<Uuid>>,
    _permit: OwnedSemaphorePermit,
}
struct Inner {
    entries: Mutex<BTreeMap<Uuid, Arc<Entry>>>,
    slots: Arc<Semaphore>,
}
/// Only internal actual Series execution can populate this registry. It does
/// not reconstruct authority from externally supplied envelopes after restart.
#[derive(Clone)]
pub struct Completions {
    inner: Arc<Inner>,
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
    pub(crate) async fn record(
        &self,
        parent: SeriesMessage,
        refresh: RefreshSeriesChild,
        report: SeriesReport,
        backend: Vec<u8>,
    ) -> Result<(), Error> {
        if !report.matches(&parent) || refresh.admit(&parent).is_none() {
            return Err(Error::Protocol);
        }
        if report.artwork_targets().is_empty() {
            return Ok(());
        }
        if report.artwork_targets().len() > 64 {
            return Err(Error::Protocol);
        }
        let permit = self
            .inner
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Closed)?;
        let remaining = report
            .artwork_targets()
            .iter()
            .map(|(id, _)| *id)
            .collect::<BTreeSet<_>>();
        if remaining.len() != report.artwork_targets().len() {
            return Err(Error::Protocol);
        }
        let mut entries = self.inner.entries.lock().map_err(|_| Error::Protocol)?;
        if entries.contains_key(&parent.task_id) {
            return Err(Error::Protocol);
        }
        entries.insert(
            parent.task_id,
            Arc::new(Entry {
                parent,
                refresh,
                report,
                backend,
                remaining: Mutex::new(remaining),
                _permit: permit,
            }),
        );
        Ok(())
    }
    pub(crate) fn admit(&self, message: &Message) -> Result<Option<SeriesArtworkChild>, Error> {
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
        SeriesArtworkChild::from_completed_report(
            entry.refresh,
            &entry.parent,
            &entry.report,
            &entry.backend,
            message,
        )
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
    /// Cancels bounded producer admission before Series drain; it neither ACKs
    /// pending children nor pretends that unconsumed results completed.
    pub fn close_admission(&self) {
        self.inner.slots.close();
    }
}
