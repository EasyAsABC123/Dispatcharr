//! Scanner-owned immediate Refresh lane. No default intake or polling starts here.
//! The caller explicitly owns the scanner assembly; parent-result task IDs alone
//! do not establish task-family provenance. Unlisted duplicate-ID children,
//! failed-parent/orphan children and ETA remain refused before ACK.
use crate::{
    refresh_protocol::{RefreshCall, RefreshMessage},
    refresh_worker::{ChildPublisher, RefreshExecutor},
    transport::{Error as TransportError, Jobs},
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
#[derive(Clone, Debug)]
pub struct Scope {
    parent: Uuid,
    sources: Vec<i64>,
}
impl Scope {
    /// Explicit scanner-assembly capability, not an untrusted HTTP request.
    /// The successful parent receipt must also list each real child before ACK.
    pub fn new(parent: Uuid, sources: Vec<i64>) -> Result<Self, TransportError> {
        if parent.is_nil()
            || sources.is_empty()
            || sources.len() > 4
            || sources.iter().any(|v| *v <= 0)
            || sources
                .iter()
                .enumerate()
                .any(|(n, v)| sources[..n].contains(v))
        {
            return Err(TransportError::Configuration);
        }
        Ok(Self { parent, sources })
    }
    pub(crate) fn parent(&self) -> Uuid {
        self.parent
    }
    pub(crate) fn accepts(&self, message: &RefreshMessage) -> bool {
        message.root == self.parent
            && message.parent == Some(self.parent)
            && message.eta.is_none()
            && !message.force
            && message.retry == 0
            && self.sources.contains(&message.source_id)
            && message.call == RefreshCall::initial(message.source_id)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Run {
    Empty,
    Completed(Uuid),
    ResultAlreadySucceeded(Uuid),
}
struct Owner {
    _slot: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
}
struct Inner {
    jobs: Jobs,
    executor: Arc<dyn RefreshExecutor>,
    scope: Scope,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
}
#[derive(Clone)]
pub struct Worker {
    inner: Arc<Inner>,
}
impl Worker {
    pub fn new(
        jobs: Jobs,
        executor: Arc<dyn RefreshExecutor>,
        scope: Scope,
    ) -> Result<Self, crate::refresh_worker::Error> {
        if !jobs.allows_refresh() {
            return Err(crate::refresh_worker::Error::Configuration);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                executor,
                scope,
                slots: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
            }),
        })
    }
    pub fn available_slots(&self) -> usize {
        self.inner.slots.available_permits()
    }
    pub async fn run_once(
        &self,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Run, crate::refresh_worker::Error> {
        use crate::refresh_worker::Error;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let slot = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let owner = Arc::new(Owner {
            _slot: slot,
            _caller: caller,
        });
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let Some(message) = inner
                .jobs
                .claim_scanner_refresh(&inner.scope, owner.clone())
                .await
                .map_err(Error::Transport)?
            else {
                return Ok(Run::Empty);
            };
            let publisher =
                ChildPublisher::context(inner.jobs.clone(), message.clone(), owner.clone());
            let result = inner
                .executor
                .execute(message.clone(), publisher.clone(), owner.clone())
                .await?;
            let children = publisher.completed_children()?;
            let written = inner
                .jobs
                .write_scanner_refresh(&inner.scope, &message, result, children, owner)
                .await
                .map_err(Error::Transport)?;
            Ok(if written {
                Run::Completed(message.task_id)
            } else {
                Run::ResultAlreadySucceeded(message.task_id)
            })
        })
        .await
        .map_err(|_| Error::WorkerPanic)?
    }
    /// Drain only this worker; close shared Jobs after all family/service drains.
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), crate::refresh_worker::Error> {
        use crate::refresh_worker::Error;
        self.inner.closed.store(true, Ordering::Release);
        let _permit = tokio::time::timeout(timeout, self.inner.slots.clone().acquire_owned())
            .await
            .map_err(|_| Error::DrainTimeout)?
            .map_err(|_| Error::Closed)?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scope_admits_only_proven_immediate_shape() {
        let p = Uuid::new_v4();
        let scope = Scope::new(p, vec![1]).unwrap();
        let mut m = RefreshMessage {
            task_id: Uuid::new_v4(),
            source_id: 1,
            force: false,
            retry: 0,
            root: p,
            parent: Some(p),
            eta: None,
            call: RefreshCall::initial(1),
        };
        assert!(scope.accepts(&m));
        m.parent = None;
        assert!(!scope.accepts(&m));
        m.parent = Some(p);
        m.root = Uuid::new_v4();
        assert!(!scope.accepts(&m));
        m.root = p;
        m.source_id = 2;
        assert!(!scope.accepts(&m));
        m.source_id = 1;
        m.call.list_args = true;
        assert!(!scope.accepts(&m));
        assert!(Scope::new(p, vec![1, 1]).is_err());
        assert!(Scope::new(Uuid::nil(), vec![1]).is_err());
    }
}
