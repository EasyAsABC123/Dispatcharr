//! Bounded no-recording Series child consumer. Original Dispatcharr authors
//! retain application/algorithm credit. Nonempty dictionary rules remain queued;
//! preflight/admission is deliberately not a Source early-ACK timing claim.
use crate::{
    series_protocol::KnownReturn,
    transport::{Error as TransportError, Jobs},
};
use dispatcharr_coordination::{TaskLease, TaskLockDetailedError, TaskLockError, TaskLocks};
use dispatcharr_ingest_epg::{cache_policy::CachedSwapPolicy, SwapPolicyContext};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Closed,
    Capacity,
    UnsupportedRules,
    Transport(TransportError),
    Operational,
    UnsettledRelease,
    Panic,
    DrainTimeout,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Run {
    Empty,
    Completed(Uuid),
    AlreadySucceeded(Uuid),
}
struct Owner {
    _slot: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
}
struct Scope {
    lease: Mutex<Option<TaskLease>>,
    _owner: Arc<Owner>,
}
impl Scope {
    async fn release(&self) -> Result<(), Error> {
        let lease = self
            .lease
            .lock()
            .unwrap()
            .take()
            .ok_or(Error::UnsettledRelease)?;
        match lease.release_owned_detailed().await {
            Ok(true) => Ok(()),
            _ => Err(Error::UnsettledRelease),
        }
    }
}
struct Inner {
    jobs: Jobs,
    locks: TaskLocks,
    policy: Arc<CachedSwapPolicy>,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
}
#[derive(Clone)]
pub struct SeriesWorker {
    inner: Arc<Inner>,
}
impl SeriesWorker {
    pub fn new_fixture(
        jobs: Jobs,
        locks: TaskLocks,
        policy: Arc<CachedSwapPolicy>,
    ) -> Result<Self, Error> {
        let db = jobs.copied_database().ok_or(Error::Configuration)?;
        if std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref() != Ok("playback-fixture-only")
            || std::env::var("POSTGRES_DB").as_deref() != Ok(db)
            || !db.starts_with("rust_probe_backup_jobs_")
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                locks,
                policy,
                slots: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
            }),
        })
    }
    /// Explicit Series-only authority; opening it does not start polling.
    pub fn new(jobs: Jobs, locks: TaskLocks, policy: Arc<CachedSwapPolicy>) -> Result<Self, Error> {
        if !jobs.allows_series() {
            return Err(Error::Configuration);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                locks,
                policy,
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
        let permit = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let owner = Arc::new(Owner {
            _slot: permit,
            _caller: caller,
        });
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let Some((bytes, message)) = inner
                .jobs
                .peek_series(owner.clone())
                .await
                .map_err(Error::Transport)?
            else {
                return Ok(Run::Empty);
            };
            let global = inner
                .locks
                .acquire_series_rules_retained(owner.clone())
                .await;
            let (value, scope) = match global {
                Err(TaskLockDetailedError::Operational(TaskLockError::Held)) => {
                    (KnownReturn::Busy, None)
                }
                Err(_) => return Err(Error::Operational),
                Ok(lease) => {
                    let scope = Arc::new(Scope {
                        lease: Mutex::new(Some(lease)),
                        _owner: owner.clone(),
                    });
                    let group = inner
                        .policy
                        .retained_dvr_settings(SwapPolicyContext {
                            cancelled: Arc::new(AtomicBool::new(false)),
                            retained: scope.clone(),
                        })
                        .await
                        .map_err(|_| Error::Operational)?;
                    let configured = group
                        .get("series_rules")
                        .and_then(serde_json::Value::as_array)
                        .is_some_and(|rules| rules.iter().any(serde_json::Value::is_object));
                    if configured {
                        scope.release().await?;
                        return Err(Error::UnsupportedRules);
                    }
                    (KnownReturn::NoRules, Some(scope))
                }
            };
            let claimed = inner.jobs.claim_series(&bytes, owner.clone()).await;
            if let Some(scope) = scope {
                scope.release().await?;
            }
            claimed.map_err(Error::Transport)?;
            match inner
                .jobs
                .write_series_success(&message, value, owner)
                .await
                .map_err(Error::Transport)?
            {
                true => Ok(Run::Completed(message.task_id)),
                false => Ok(Run::AlreadySucceeded(message.task_id)),
            }
        })
        .await
        .map_err(|_| Error::Panic)?
    }
    /// Stop/drain this family only. The owner closes shared transports last.
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.inner.closed.store(true, Ordering::Release);
        let _permit = tokio::time::timeout(timeout, self.inner.slots.clone().acquire_owned())
            .await
            .map_err(|_| Error::DrainTimeout)?
            .map_err(|_| Error::Closed)?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "series_worker_peer.rs"]
mod peer;
