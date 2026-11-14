//! Retained, one-at-a-time normal version task execution on the default FIFO.
//! Original Dispatcharr/Celery authors retain application/protocol credit.
use crate::{
    transport::{Error as TransportError, VersionJobs},
    version_service::{Error as ServiceError, Service},
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
#[derive(Debug)]
pub enum Error {
    Closed,
    Capacity,
    Transport(TransportError),
    Service(ServiceError),
    Supervisor,
    Drain,
}
#[derive(Clone, Debug)]
pub enum Run {
    Empty,
    UnsupportedHead,
    Completed(uuid::Uuid, crate::version_service::Report),
    AlreadySucceeded(uuid::Uuid, crate::version_service::Report),
}
struct Owner {
    _caller: Arc<dyn Send + Sync>,
    _slot: OwnedSemaphorePermit,
}
struct Inner {
    jobs: VersionJobs,
    service: Service,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
    uncertain: Mutex<Option<Arc<Owner>>>,
}
#[derive(Clone)]
pub struct Worker {
    inner: Arc<Inner>,
}
impl Worker {
    pub fn prepare(jobs: VersionJobs, service: Service) -> Self {
        Self {
            inner: Arc::new(Inner {
                jobs,
                service,
                slots: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
                uncertain: Mutex::new(None),
            }),
        }
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
            _caller: caller,
            _slot: slot,
        });
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let message = match inner.jobs.claim(owner.clone()).await {
                Ok(Some(m)) => m,
                Ok(None) => return Ok(Run::Empty),
                Err(TransportError::UnsupportedTask) => return Ok(Run::UnsupportedHead),
                Err(e) => {
                    if matches!(e, TransportError::AmbiguousClaim(_)) {
                        *inner.uncertain.lock().map_err(|_| Error::Supervisor)? = Some(owner);
                    }
                    return Err(Error::Transport(e));
                }
            };
            // The real matching frame has been ACKed exactly once, as Source.
            // No guessed retries or guessed SUCCESS after an operational error.
            let report = inner
                .service
                .execute(owner.clone())
                .await
                .map_err(Error::Service)?;
            let written = inner.jobs.write(&message, owner.clone()).await;
            let written = match written {
                Err(error @ TransportError::AmbiguousResult(_)) => {
                    *inner.uncertain.lock().map_err(|_| Error::Supervisor)? = Some(owner);
                    return Err(Error::Transport(error));
                }
                other => other.map_err(Error::Transport)?,
            };
            match written {
                true => Ok(Run::Completed(message.task_id, report)),
                false => Ok(Run::AlreadySucceeded(message.task_id, report)),
            }
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.inner.closed.store(true, Ordering::Release);
        let permit = tokio::time::timeout(timeout, self.inner.slots.clone().acquire_owned())
            .await
            .map_err(|_| Error::Drain)?
            .map_err(|_| Error::Drain)?;
        drop(permit);
        Ok(())
    }
}
