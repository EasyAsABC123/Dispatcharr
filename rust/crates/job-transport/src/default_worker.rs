//! Bounded inert default-task library. Original Dispatcharr/Celery authors retain credit.
//! No Main registration or automatic intake; unsupported domains remain explicit.
use crate::{
    default_domains::{Domains, Error as DomainError},
    default_protocol::Kind,
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
#[derive(Debug)]
pub enum Error {
    Configuration,
    Closed,
    Capacity,
    Transport(TransportError),
    Domain(DomainError),
    Panic,
}
#[derive(Debug)]
pub enum Run {
    Empty,
    UnsupportedHead,
    Unsupported(Kind),
    Completed(Uuid),
    AlreadySucceeded(Uuid),
}
struct Owner {
    _slot: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
}
struct Inner {
    jobs: Jobs,
    domains: Arc<dyn Domains>,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
}
#[derive(Clone)]
pub struct DefaultWorker {
    inner: Arc<Inner>,
}
impl DefaultWorker {
    pub fn new_fixture(jobs: Jobs, domains: Arc<dyn Domains>) -> Result<Self, Error> {
        let database = jobs.copied_database().ok_or(Error::Configuration)?;
        if std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref() != Ok("playback-fixture-only")
            || std::env::var("POSTGRES_DB").as_deref() != Ok(database)
        {
            return Err(Error::Configuration);
        }
        // Source93 actual worker is prefork concurrency1. One slot also bounds
        // candidate bytes and process-local first-scan state; do not generalize.
        Self::new(jobs, domains)
    }
    /// Explicit worker only; no automatic intake or shared transport closure.
    pub fn new(jobs: Jobs, domains: Arc<dyn Domains>) -> Result<Self, Error> {
        if !jobs.allows_defaults() {
            return Err(Error::Configuration);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                domains,
                slots: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
            }),
        })
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
            let candidate = match inner.jobs.peek_default(owner.clone()).await {
                Ok(Some(candidate)) => candidate,
                Ok(None) => return Ok(Run::Empty),
                Err(TransportError::UnsupportedTask) => return Ok(Run::UnsupportedHead),
                Err(error) => return Err(Error::Transport(error)),
            };
            let kind = candidate.message.kind;
            match inner.domains.preflight(kind, owner.clone()).await {
                Ok(()) => {}
                Err(DomainError::Unsupported) => return Ok(Run::Unsupported(kind)),
                Err(error) => return Err(Error::Domain(error)),
            }
            let message = inner
                .jobs
                .claim_default(candidate)
                .await
                .map_err(Error::Transport)?;
            // Recheck actual domain after claim. An insertion/mutation during
            // preflight cannot produce a fake known-null/zero result.
            let result = inner
                .domains
                .execute(kind, owner.clone())
                .await
                .map_err(Error::Domain)?;
            let saved = inner
                .jobs
                .write_default_success(&message, result, owner)
                .await
                .map_err(Error::Transport)?;
            Ok(if saved {
                Run::Completed(message.task_id)
            } else {
                Run::AlreadySucceeded(message.task_id)
            })
        })
        .await
        .map_err(|_| Error::Panic)?
    }
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.inner.closed.store(true, Ordering::Release);
        matches!(
            tokio::time::timeout(timeout, self.inner.slots.clone().acquire_owned()).await,
            Ok(Ok(_))
        )
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration => f.write_str("Configuration"),
            Self::Closed => f.write_str("Closed"),
            Self::Capacity => f.write_str("Capacity"),
            Self::Panic => f.write_str("Panic"),
            Self::Transport(error) => write!(f, "Transport({error:?})"),
            Self::Domain(error) => write!(f, "Domain({error})"),
        }
    }
}
impl Run {
    pub fn summary(&self) -> String {
        match self {
            Self::Empty => "Empty".into(),
            Self::UnsupportedHead => "UnsupportedHead".into(),
            Self::Unsupported(kind) => format!("Unsupported({kind:?})"),
            Self::Completed(id) => format!("Completed({id})"),
            Self::AlreadySucceeded(id) => format!("AlreadySucceeded({id})"),
        }
    }
}
