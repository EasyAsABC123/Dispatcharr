//! Same-process handoff from actual Refresh execution to configured Series.
//! Original Dispatcharr authors retain all algorithm and protocol credit.
//! Captured initial model/repeat and immediate API parents only. Restart
//! reconstruction and deferred parent chains remain unsupported.
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

enum Parent {
    Individual(Box<RefreshMessage>),
    Aggregate(Uuid),
}

struct Entry {
    parent: Parent,
    children: Mutex<BTreeSet<Uuid>>,
    _permit: OwnedSemaphorePermit,
}
impl Entry {
    fn owns_child(&self, message: &SeriesMessage) -> Result<bool, TransportError> {
        let children = self.children.lock().map_err(|_| TransportError::Protocol)?;
        Ok(children.contains(&message.task_id))
    }

    fn completed_child(&self, raw: &[u8], message: &SeriesMessage) -> Option<RefreshSeriesChild> {
        match &self.parent {
            Parent::Individual(refresh) => {
                RefreshSeriesChild::from_completed_refresh(refresh, raw, message).or_else(|| {
                    RefreshSeriesChild::from_completed_live_refresh_api(refresh, raw, message)
                })
            }
            Parent::Aggregate(root) => {
                RefreshSeriesChild::from_completed_aggregate(*root, raw, message)
            }
        }
    }
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
    root: Uuid,
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
        if !entry.owns_child(message)? {
            return Ok(None);
        }
        // Queue publication precedes parent SUCCESS. Absence is an ordinary
        // wait, never permission to synthesize completion or remove this head.
        let raw = match &entry.parent {
            Parent::Individual(refresh) => {
                self.inner.jobs.read_refresh_lineage(refresh, owner).await?
            }
            Parent::Aggregate(root) => {
                self.inner
                    .jobs
                    .read_aggregate_refresh_lineage(*root, owner)
                    .await?
            }
        };
        let Some(raw) = raw else {
            return Ok(None);
        };
        let child = entry
            .completed_child(&raw, message)
            .ok_or(TransportError::Protocol)?;
        Ok(Some(CompletedParent {
            entry,
            root: parent,
            child,
        }))
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
        let Some(entry) = entries.get(&pending.root) else {
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
            entries.remove(&pending.root);
        }
        Ok(())
    }
    async fn acquire(&self) -> Result<OwnedSemaphorePermit, Error> {
        self.inner
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Closed)
    }

    fn retain(
        &self,
        id: Uuid,
        parent: Parent,
        children: &[Uuid],
        permit: OwnedSemaphorePermit,
    ) -> Result<(), Error> {
        if children.is_empty() {
            return Ok(());
        }
        let mut entries = self.inner.entries.lock().map_err(|_| Error::Execution)?;
        if entries.contains_key(&id) {
            return Err(Error::Execution);
        }
        entries.insert(
            id,
            Arc::new(Entry {
                parent,
                children: Mutex::new(children.iter().copied().collect()),
                _permit: permit,
            }),
        );
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
    {
        return false;
    }
    let mut model = RefreshCall::initial(parent.source_id);
    model.list_args = parent.call.list_args;
    (!parent.force && parent.call == model) || parent.has_explicit_api_arguments()
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
                Some(self.handoff.acquire().await?)
            } else {
                None
            };
            let result = self
                .inner
                .execute(message.clone(), publisher.clone(), owner)
                .await?;
            let children = publisher.completed_series_children()?;
            if let Some(permit) = permit {
                self.handoff.retain(
                    message.task_id,
                    Parent::Individual(Box::new(message)),
                    &children,
                    permit,
                )?;
            }
            Ok(result)
        })
    }
    fn execute_aggregate<'a>(
        &'a self,
        sources: crate::refresh_aggregate::XmltvSources,
        publisher: ChildPublisher,
        owner: Arc<dyn Send + Sync>,
    ) -> crate::refresh_worker::AggregateFuture<'a> {
        Box::pin(async move {
            let permit = self.handoff.acquire().await?;
            let root = publisher.task_id();
            let completed = self
                .inner
                .execute_aggregate(sources, publisher, owner)
                .await?;
            self.handoff
                .retain(root, Parent::Aggregate(root), &completed.series, permit)?;
            Ok(completed)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn immediate_api_force_is_a_live_parent() {
        let id = Uuid::new_v4();
        for force in [false, true] {
            let mut call = RefreshCall::initial(1);
            call.kwargs
                .insert("force".into(), serde_json::Value::Bool(force));
            let parent = RefreshMessage {
                task_id: id,
                source_id: 1,
                force,
                retry: 0,
                root: id,
                parent: None,
                eta: None,
                call,
            };
            assert!(supported(&parent));
            assert!(!supported(&RefreshMessage {
                force: !force,
                ..parent
            }));
        }
    }

    #[test]
    fn forced_refresh_live_entry_completes_its_published_series_child() {
        let root = Uuid::new_v4();
        let child = Uuid::new_v4();
        let index = Uuid::new_v4();
        let mut call = RefreshCall::initial(1);
        call.kwargs
            .insert("force".into(), serde_json::Value::Bool(true));
        let parent = RefreshMessage {
            task_id: root,
            source_id: 1,
            force: true,
            retry: 0,
            root,
            parent: None,
            eta: None,
            call,
        };
        assert!(supported(&parent));
        let slots = Arc::new(Semaphore::new(1));
        let entry = Entry {
            parent: Parent::Individual(Box::new(parent)),
            children: Mutex::new(BTreeSet::from([child])),
            _permit: slots.clone().try_acquire_owned().unwrap(),
        };
        assert_eq!(slots.available_permits(), 0);
        let message = SeriesMessage {
            task_id: child,
            root,
            parent: Some(root),
        };
        assert!(entry.owns_child(&message).unwrap());
        let terminal = serde_json::json!({
            "status": "SUCCESS",
            "result": null,
            "traceback": null,
            "children": [[[index, null], null], [[child, null], null]],
            "date_done": "2026-11-21T08:06:00+00:00",
            "task_id": root
        });
        let raw = serde_json::to_vec(&terminal).unwrap();
        assert!(entry
            .completed_child(&raw, &message)
            .and_then(|completed| completed.admit(&message))
            .is_some());
        drop(entry);
        assert_eq!(slots.available_permits(), 1);
    }
}
