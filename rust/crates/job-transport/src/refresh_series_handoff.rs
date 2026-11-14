//! Same-process handoff from actual Refresh execution to configured Series.
//! Original Dispatcharr authors retain all algorithm and protocol credit.
//! Captured initial model/repeat/API-false parents only; restart reconstruction
//! and deferred/forced parent chains remain unsupported, with their FIFO intact.
use crate::{
    recording_artwork::RefreshSeriesChild,
    refresh_protocol::{RefreshCall, RefreshMessage},
    refresh_worker::{ChildPublisher, Error, ExecuteFuture, RefreshExecutor},
    series_protocol::SeriesMessage,
    transport::{Error as TransportError, Jobs},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

struct Entry {
    parent: RefreshMessage,
    children: Mutex<BTreeSet<Uuid>>,
    _permit: OwnedSemaphorePermit,
}
struct Inner {
    entries: Mutex<BTreeMap<Uuid, Arc<Entry>>>,
    slots: Arc<Semaphore>,
    jobs: Jobs,
}
/// A read-only consumer handle: it cannot publish, ACK or add arbitrary parents.
#[derive(Clone)]
pub struct Handoff {
    inner: Arc<Inner>,
}
pub(crate) struct CompletedParent {
    entry: Arc<Entry>,
    pub(crate) child: RefreshSeriesChild,
}
impl Handoff {
    pub(crate) fn prepare(jobs: Jobs) -> Self {
        Self {
            inner: Arc::new(Inner {
                entries: Mutex::new(BTreeMap::new()),
                slots: Arc::new(Semaphore::new(64)),
                jobs,
            }),
        }
    }
    pub(crate) async fn completed_parent(
        &self,
        message: &SeriesMessage,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<CompletedParent>, TransportError> {
        let Some(parent) = message.parent else {
            return Ok(None);
        };
        let entry = self
            .inner
            .entries
            .lock()
            .map_err(|_| TransportError::Protocol)?
            .get(&parent)
            .cloned();
        let Some(entry) = entry else {
            return Ok(None);
        };
        if !entry
            .children
            .lock()
            .map_err(|_| TransportError::Protocol)?
            .contains(&message.task_id)
        {
            return Ok(None);
        }
        // Queue publication precedes parent SUCCESS. Absence is an ordinary
        // wait, never permission to synthesize completion or remove this head.
        let Some(raw) = self
            .inner
            .jobs
            .read_refresh_lineage(&entry.parent, owner)
            .await?
        else {
            return Ok(None);
        };
        let child = RefreshSeriesChild::from_completed_refresh(&entry.parent, &raw, message)
            .or_else(|| {
                RefreshSeriesChild::from_completed_refresh_api(&entry.parent, &raw, message)
            })
            .ok_or(TransportError::Protocol)?;
        Ok(Some(CompletedParent { entry, child }))
    }
    pub(crate) fn completed_child(
        &self,
        pending: CompletedParent,
        child: Uuid,
    ) -> Result<(), TransportError> {
        let mut entries = self
            .inner
            .entries
            .lock()
            .map_err(|_| TransportError::Protocol)?;
        let Some(entry) = entries.get(&pending.entry.parent.task_id) else {
            return Err(TransportError::Protocol);
        };
        if !Arc::ptr_eq(entry, &pending.entry) {
            return Err(TransportError::Protocol);
        }
        let empty = {
            let mut children = entry
                .children
                .lock()
                .map_err(|_| TransportError::Protocol)?;
            if !children.remove(&child) {
                return Err(TransportError::Protocol);
            }
            children.is_empty()
        };
        if empty {
            entries.remove(&pending.entry.parent.task_id);
        }
        Ok(())
    }
    pub(crate) fn close_admission(&self) {
        self.inner.slots.close();
    }
}
fn supported(parent: &RefreshMessage) -> bool {
    if parent.parent.is_some()
        || parent.root != parent.task_id
        || parent.retry != 0
        || parent.eta.is_some()
        || parent.force
    {
        return false;
    }
    let mut model = RefreshCall::initial(parent.source_id);
    model.list_args = parent.call.list_args;
    if parent.call == model {
        return true;
    }
    !parent.call.list_args
        && parent.call.args == model.args
        && parent.call.kwargs
            == serde_json::json!({"force":false})
                .as_object()
                .unwrap()
                .clone()
}
pub(crate) struct Executor {
    pub(crate) inner: Arc<dyn RefreshExecutor>,
    pub(crate) handoff: Handoff,
}
impl RefreshExecutor for Executor {
    fn execute<'a>(
        &'a self,
        message: RefreshMessage,
        publisher: ChildPublisher,
        owner: Arc<dyn Send + Sync>,
    ) -> ExecuteFuture<'a> {
        Box::pin(async move {
            let selected = supported(&message);
            // Bound retained parents without eviction. Backpressure retains the
            // actual accepted parent owner while older Series children drain.
            let permit = if selected {
                Some(
                    self.handoff
                        .inner
                        .slots
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| Error::Closed)?,
                )
            } else {
                None
            };
            let result = self
                .inner
                .execute(message.clone(), publisher.clone(), owner)
                .await?;
            let children = publisher.completed_series_children()?;
            if let Some(permit) = permit.filter(|_| !children.is_empty()) {
                let mut entries = self
                    .handoff
                    .inner
                    .entries
                    .lock()
                    .map_err(|_| Error::Execution)?;
                // A duplicated root is not allowed to replace an older pending
                // capability. No fabricated/reused completion after replay.
                if entries.contains_key(&message.task_id) {
                    return Err(Error::Execution);
                }
                entries.insert(
                    message.task_id,
                    Arc::new(Entry {
                        parent: message,
                        children: Mutex::new(children.into_iter().collect()),
                        _permit: permit,
                    }),
                );
            }
            Ok(result)
        })
    }
}
