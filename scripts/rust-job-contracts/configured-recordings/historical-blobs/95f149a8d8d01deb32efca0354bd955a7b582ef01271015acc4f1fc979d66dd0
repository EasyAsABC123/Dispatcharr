//! Explicit inert configured Series/recurring consumers. Original Dispatcharr
//! authors retain credit. No default worker or Main behavior is changed.
use crate::{
    configured_recording_service::{self, Service},
    default_protocol::Kind,
    recording_artwork::DirectTask,
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
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
#[derive(Debug)]
pub enum Error {
    Configuration,
    Closed,
    Capacity,
    Unsupported,
    Transport(TransportError),
    Domain(configured_recording_service::Error),
    Operational,
    Policy(dispatcharr_ingest_epg::cache_policy::ConfiguredSettingsError),
    UnsettledRelease,
    Supervisor,
    DrainTimeout,
}
#[derive(Debug, PartialEq, Eq)]
pub enum Run {
    Empty,
    UnsupportedHead,
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
            .map_err(|_| Error::UnsettledRelease)?
            .take()
            .ok_or(Error::UnsettledRelease)?;
        match lease.release_owned_detailed().await {
            Ok(true) => Ok(()),
            _ => Err(Error::UnsettledRelease),
        }
    }
}
#[derive(Clone, Copy)]
pub enum Family {
    Series,
    Recurring,
}
pub struct Assembly {
    pub jobs: Jobs,
    pub locks: TaskLocks,
    pub store: dispatcharr_store::Store,
    pub cache: String,
    pub artwork: Arc<dyn configured_recording_service::ArtworkSink>,
    pub events: Arc<dyn dispatcharr_contracts::UpdateEventSink>,
    pub family: Family,
}
struct PolicyReceipt {
    done: Option<oneshot::Sender<()>>,
    _owner: Arc<dyn Send + Sync>,
}
impl Drop for PolicyReceipt {
    fn drop(&mut self) {
        if let Some(tx) = self.done.take() {
            let _ = tx.send(());
        }
    }
}
struct Inner {
    jobs: Jobs,
    locks: TaskLocks,
    policy: Arc<CachedSwapPolicy>,
    service: Arc<Service>,
    family: Family,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
    #[cfg(test)]
    owned_parent_route: bool,
}
#[derive(Clone)]
pub struct Worker {
    inner: Arc<Inner>,
}
impl Worker {
    /// Merely constructs this ONE family. Caller owns transport/policy/service
    /// assembly and must retain/drain each before shared transport shutdown.
    pub fn new(assembly: Assembly) -> Result<Self, Error> {
        let Assembly {
            jobs,
            locks,
            store,
            cache,
            artwork,
            events,
            family,
        } = assembly;
        let service = Arc::new(Service::new(store.pool().clone(), artwork, events));
        let policy = Arc::new(CachedSwapPolicy::new(store, Some(cache)));
        if !(match family {
            Family::Series => jobs.allows_series(),
            Family::Recurring => jobs.allows_defaults(),
        }) {
            return Err(Error::Configuration);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                locks,
                policy,
                service,
                family,
                slots: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
                #[cfg(test)]
                owned_parent_route: false,
            }),
        })
    }
    #[cfg(test)]
    pub fn new_parent_route_fixture(assembly: Assembly, queue: &str) -> Result<Self, Error> {
        assembly
            .jobs
            .require_configured_parent_fixture(queue)
            .map_err(Error::Transport)?;
        let mut worker = Self::new(assembly)?;
        Arc::get_mut(&mut worker.inner)
            .ok_or(Error::Configuration)?
            .owned_parent_route = true;
        Ok(worker)
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
            match inner.family {
                Family::Series => series(inner, owner).await,
                Family::Recurring => recurring(inner, owner).await,
            }
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.inner.closed.store(true, Ordering::Release);
        let _permit = tokio::time::timeout(timeout, self.inner.slots.clone().acquire_owned())
            .await
            .map_err(|_| Error::DrainTimeout)?
            .map_err(|_| Error::Closed)?;
        Ok(())
    }
}
enum Message {
    Series(crate::series_protocol::SeriesMessage),
    Recurring(crate::default_protocol::Message),
}
enum Claim {
    Series(Vec<u8>),
    Recurring(crate::transport::DefaultCandidate),
    #[cfg(test)]
    Owned(crate::transport::ConfiguredParentCandidate),
}
struct Pending {
    message: Message,
    claim: Claim,
}
async fn peek(inner: &Inner, owner: Arc<Owner>) -> Result<Option<Pending>, TransportError> {
    #[cfg(test)]
    if inner.owned_parent_route {
        return inner
            .jobs
            .peek_configured_parent_fixture(
                "rust-owned-configured-recordings",
                matches!(inner.family, Family::Series),
                owner,
            )
            .await
            .map(|v| {
                v.map(|c| {
                    let message = match c.message.clone() {
                        crate::transport::ConfiguredParentMessage::Series(v) => Message::Series(v),
                        crate::transport::ConfiguredParentMessage::Recurring(v) => {
                            Message::Recurring(v)
                        }
                    };
                    Pending {
                        message,
                        claim: Claim::Owned(c),
                    }
                })
            });
    }
    match inner.family {
        Family::Series => inner.jobs.peek_series(owner).await.map(|v| {
            v.map(|(bytes, message)| Pending {
                message: Message::Series(message),
                claim: Claim::Series(bytes),
            })
        }),
        Family::Recurring => inner.jobs.peek_default(owner).await.map(|v| {
            v.map(|candidate| Pending {
                message: Message::Recurring(candidate.message.clone()),
                claim: Claim::Recurring(candidate),
            })
        }),
    }
}
fn canonical(inner: &Inner, pending: &Pending, task: DirectTask) -> bool {
    match &pending.claim {
        Claim::Series(raw) => crate::recording_artwork::canonical_frame(raw, task),
        Claim::Recurring(candidate) => inner.jobs.configured_candidate_canonical(candidate, task),
        #[cfg(test)]
        Claim::Owned(candidate) => candidate.canonical(task),
    }
}
async fn claim(
    inner: &Inner,
    pending: Pending,
    owner: Arc<dyn Send + Sync>,
) -> Result<(), TransportError> {
    match pending.claim {
        Claim::Series(bytes) => inner.jobs.claim_series(&bytes, owner).await,
        Claim::Recurring(candidate) => inner.jobs.claim_default(candidate).await.map(|_| ()),
        #[cfg(test)]
        Claim::Owned(candidate) => inner.jobs.claim_configured_parent_fixture(candidate).await,
    }
}
async fn settings(
    inner: &Inner,
    owner: Arc<dyn Send + Sync>,
    system: bool,
) -> Result<serde_json::Value, Error> {
    let (tx, rx) = oneshot::channel();
    let receipt = Arc::new(PolicyReceipt {
        done: Some(tx),
        _owner: owner,
    });
    let context = SwapPolicyContext {
        cancelled: Arc::new(AtomicBool::new(false)),
        retained: receipt.clone(),
    };
    let observed = if system {
        inner
            .policy
            .retained_configured_system_settings(context)
            .await
    } else {
        inner.policy.retained_configured_dvr_settings(context).await
    };
    drop(receipt);
    // Cached-policy logical fallback/error does not establish physical Redis
    // settlement. Await every transferred guard before ACK or releasing lease.
    rx.await.map_err(|_| Error::Operational)?;
    observed.map_err(Error::Policy)
}
async fn series(inner: Arc<Inner>, owner: Arc<Owner>) -> Result<Run, Error> {
    let pending = match peek(&inner, owner.clone()).await {
        Ok(Some(v)) => v,
        Ok(None) => return Ok(Run::Empty),
        Err(TransportError::UnsupportedTask) => return Ok(Run::UnsupportedHead),
        Err(e) => return Err(Error::Transport(e)),
    };
    let message = match &pending.message {
        Message::Series(v) => v.clone(),
        _ => return Ok(Run::UnsupportedHead),
    };
    // Configured children/parented rules remain OPEN: refuse before lease/cache,
    // ACK, revocation or any SQL mutation. Old no-rule worker is untouched.
    let task = DirectTask::series(&message).ok_or(Error::Unsupported)?;
    if !canonical(&inner, &pending, task) {
        return Err(Error::Unsupported);
    }
    let lease = match inner
        .locks
        .acquire_series_rules_retained(owner.clone())
        .await
    {
        Ok(lease) => lease,
        Err(TaskLockDetailedError::Operational(TaskLockError::Held)) => {
            claim(&inner, pending, owner.clone())
                .await
                .map_err(Error::Transport)?;
            return match inner
                .jobs
                .write_series_success(&message, KnownReturn::Busy, owner)
                .await
                .map_err(Error::Transport)?
            {
                true => Ok(Run::Completed(message.task_id)),
                false => Ok(Run::AlreadySucceeded(message.task_id)),
            };
        }
        Err(_) => return Err(Error::Operational),
    };
    let scope = Arc::new(Scope {
        lease: Mutex::new(Some(lease)),
        _owner: owner.clone(),
    });
    let result = async {
        let group = settings(&inner, scope.clone(), false).await?;
        let prepared = inner
            .service
            .prepare_series(&group, chrono::Utc::now(), task, scope.clone())
            .await
            .map_err(Error::Domain)?;
        claim(&inner, pending, scope.clone())
            .await
            .map_err(Error::Transport)?;
        inner
            .service
            .clone()
            .execute_series(prepared, scope.clone())
            .await
            .map_err(Error::Domain)
    }
    .await;
    // Finally release settles before terminal reporting. Any uncertain release
    // remains operational; no fabricated normal result after ambiguous cleanup.
    scope.release().await?;
    let report = result?;
    match inner
        .jobs
        .write_configured_series_success(&message, &report, owner)
        .await
        .map_err(Error::Transport)?
    {
        true => Ok(Run::Completed(message.task_id)),
        false => Ok(Run::AlreadySucceeded(message.task_id)),
    }
}
async fn recurring(inner: Arc<Inner>, owner: Arc<Owner>) -> Result<Run, Error> {
    let pending = match peek(&inner, owner.clone()).await {
        Ok(Some(v)) => v,
        Ok(None) => return Ok(Run::Empty),
        Err(TransportError::UnsupportedTask) => return Ok(Run::UnsupportedHead),
        Err(e) => return Err(Error::Transport(e)),
    };
    let message = match &pending.message {
        Message::Recurring(v) => v.clone(),
        _ => return Ok(Run::UnsupportedHead),
    };
    // Essential inverse guard: recognizing other known families does not grant
    // this assembly their expiry/revocation/admission/early-ACK authority.
    if message.kind != Kind::RecurringMaintenance {
        return Ok(Run::UnsupportedHead);
    }
    let task = DirectTask::recurring(&message).ok_or(Error::Unsupported)?;
    if !canonical(&inner, &pending, task) {
        return Err(Error::Unsupported);
    }
    let system = settings(&inner, owner.clone(), true).await?;
    let prepared = inner
        .service
        .prepare_recurring(&system, chrono::Utc::now(), task, owner.clone())
        .await
        .map_err(Error::Domain)?;
    claim(&inner, pending, owner.clone())
        .await
        .map_err(Error::Transport)?;
    let report = inner
        .service
        .clone()
        .execute_prepared_recurring(prepared, owner.clone())
        .await
        .map_err(Error::Domain)?;
    match inner
        .jobs
        .write_configured_recurring_success(&message, &report, owner)
        .await
        .map_err(Error::Transport)?
    {
        true => Ok(Run::Completed(message.task_id)),
        false => Ok(Run::AlreadySucceeded(message.task_id)),
    }
}
