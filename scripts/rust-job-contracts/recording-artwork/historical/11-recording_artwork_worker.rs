//! Inert one-child artwork worker; no default consumer, Main or registry changes.
//! Original Dispatcharr authors retain all application/algorithm credit.
use crate::{
    recording_artwork_protocol::SeriesArtworkChild,
    recording_local_artwork_service::{self, Service},
    transport::{self, Jobs},
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
#[derive(Debug)]
pub enum Error {
    Closed,
    Capacity,
    Transport(transport::Error),
    Domain(recording_local_artwork_service::Error),
    Supervisor,
    DrainTimeout,
}
#[derive(Debug, PartialEq, Eq)]
pub enum Run {
    Empty,
    UnsupportedHead,
    NotDue,
    Completed { task_id: Uuid, changed: bool },
    AlreadySucceeded { task_id: Uuid, changed: bool },
}
struct Owner {
    _slot: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
}
struct Inner {
    jobs: Jobs,
    service: Arc<Service>,
    child: SeriesArtworkChild,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
}
#[derive(Clone)]
pub struct Worker {
    inner: Arc<Inner>,
}
impl Worker {
    /// Creates a dedicated artwork-only transport, never exposes its Jobs handle.
    /// The one-child capability must come from actual completed Series metadata.
    pub async fn connect(
        broker: &str,
        backend: &str,
        config: transport::Config,
        service: Arc<Service>,
        child: SeriesArtworkChild,
    ) -> Result<Self, Error> {
        let jobs = Jobs::connect_local_artwork(broker, backend, config)
            .await
            .map_err(Error::Transport)?;
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                service,
                child,
                slots: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
            }),
        })
    }
    #[cfg(test)]
    pub(crate) async fn connect_fixture(
        broker: &str,
        backend: &str,
        database: &str,
        config: transport::Config,
        service: Arc<Service>,
        child: SeriesArtworkChild,
    ) -> Result<Self, Error> {
        let jobs = Jobs::connect_local_artwork_fixture(broker, backend, database, config)
            .await
            .map_err(Error::Transport)?;
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                service,
                child,
                slots: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
            }),
        })
    }
    pub fn available_slots(&self) -> usize {
        self.inner.slots.available_permits()
    }
    pub async fn run_once(&self, caller: Arc<dyn Send + Sync>) -> Result<Run, Error> {
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
            let candidate = match inner.jobs.peek_local_artwork(owner.clone()).await {
                Ok(Some(c)) => c,
                Ok(None) => return Ok(Run::Empty),
                Err(transport::Error::UnsupportedTask) => return Ok(Run::UnsupportedHead),
                Err(e) => return Err(Error::Transport(e)),
            };
            if !inner.child.admits(&candidate.message) {
                return Ok(Run::UnsupportedHead);
            }
            // No timer/ETA heap claim: retain an early frame unchanged. The
            // explicit caller may drive this inert worker after its actual ETA.
            if candidate.message.eta > chrono::Utc::now() {
                return Ok(Run::NotDue);
            }
            let message = candidate.message.clone();
            let prepared = inner
                .service
                .prepare(message.recording, owner.clone())
                .await
                .map_err(Error::Domain)?;
            inner
                .jobs
                .claim_local_artwork(candidate, owner.clone())
                .await
                .map_err(Error::Transport)?;
            let report = inner
                .service
                .clone()
                .execute(prepared, owner.clone())
                .await
                .map_err(Error::Domain)?;
            match inner
                .jobs
                .write_local_artwork(&message, owner)
                .await
                .map_err(Error::Transport)?
            {
                true => Ok(Run::Completed {
                    task_id: message.task_id,
                    changed: report.changed(),
                }),
                false => Ok(Run::AlreadySucceeded {
                    task_id: message.task_id,
                    changed: report.changed(),
                }),
            }
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.inner.closed.store(true, Ordering::Release);
        let deadline = tokio::time::Instant::now() + timeout;
        let _permit = tokio::time::timeout(timeout, self.inner.slots.clone().acquire_owned())
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
