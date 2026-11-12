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
use tokio::sync::{oneshot, watch, OwnedSemaphorePermit, Semaphore};
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
    Failed(Uuid),
    AlreadySucceeded(Uuid),
    Revoked(Uuid, crate::cleanup_expiry::Revocation),
}
struct Owner {
    _slot: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
}
struct Inner {
    jobs: Jobs,
    accepted_kind:Option<Kind>,
    domains: Arc<dyn Domains>,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
    revoked: std::sync::Mutex<crate::cleanup_expiry::RevokedSet>,
    stop: watch::Sender<bool>,
}
struct Lifetime(watch::Sender<bool>);
impl Drop for Lifetime {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}
#[derive(Clone)]
pub struct DefaultWorker {
    inner: Arc<Inner>,
    _lifetime: Arc<Lifetime>,
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
        Self::new_scoped(jobs,domains,None)
    }
    /// The logo-only assembly must never revoke/consume another family, even
    /// an expired cleanup message whose normal domain preflight is skipped.
    pub fn new_logo_scanner(jobs:Jobs,domains:Arc<dyn Domains>)->Result<Self,Error>{Self::new_scoped(jobs,domains,Some(Kind::LogoScanner))}
    fn new_scoped(jobs:Jobs,domains:Arc<dyn Domains>,accepted_kind:Option<Kind>)->Result<Self,Error>{
        if !jobs.allows_defaults() {
            return Err(Error::Configuration);
        }
        let stop = watch::channel(false).0;
        Ok(Self {
            _lifetime: Arc::new(Lifetime(stop.clone())),
            inner: Arc::new(Inner {
                jobs,
                accepted_kind,
                domains,
                slots: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
                revoked: std::sync::Mutex::new(crate::cleanup_expiry::RevokedSet::new()),
                stop,
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
        let (outcome, received) = oneshot::channel();
        // The owned job survives a dropped caller. On backend failure it can
        // report the operational result while retaining its receipt/admission
        // until explicit/final-owner warm shutdown restores the message.
        tokio::spawn(async move {
            let mut outcome = Some(outcome);
            let result = async {
                let candidate = match inner.jobs.peek_default(owner.clone()).await {
                    Ok(Some(value)) => value,
                    Ok(None) => return Ok(Run::Empty),
                    Err(TransportError::UnsupportedTask) => return Ok(Run::UnsupportedHead),
                    Err(error) => return Err(Error::Transport(error)),
                };
                let kind = candidate.message.kind;
                // Preserve legacy intake authority/classification. Merely
                // recognizing the new protocol must not activate this family.
                // This check precedes revocation, expiry and domain callbacks.
                if inner.accepted_kind.is_none()&&kind==Kind::LogoScanner{return Ok(Run::UnsupportedHead);}
                if inner.accepted_kind.is_some_and(|accepted|accepted!=kind){return Ok(Run::Unsupported(kind));}
                let cached = inner
                    .revoked
                    .lock()
                    .map_err(|_| Error::Panic)?
                    .contains(candidate.message.task_id);
                if cached && kind != Kind::RedisBackendCleanup {
                    return Ok(Run::Unsupported(kind));
                }
                let initially_expired = candidate
                    .message
                    .expires
                    .as_ref()
                    .is_some_and(|value| value.expired(chrono::Utc::now()));
                if !cached && !initially_expired {
                    match inner.domains.preflight_message(candidate.message.clone(), owner.clone()).await {
                        Ok(()) => {}
                        Err(DomainError::Unsupported) => return Ok(Run::Unsupported(kind)),
                        Err(error) => return Err(Error::Domain(error)),
                    }
                }
                let needs_receipt = kind == Kind::RedisBackendCleanup
                    && (candidate.message.expires.is_some() || cached);
                if needs_receipt {
                    if !cached && !initially_expired && !inner.jobs.cleanup_parallel() {
                        return Ok(Run::Unsupported(kind));
                    }
                    let receipt = inner
                        .jobs
                        .reserve_cleanup(candidate)
                        .await
                        .map_err(Error::Transport)?;
                    let message = receipt.message().clone();
                    // This is Source's message-admission point, after its broker
                    // reservation reply and before dispatch/ACK. A normal ACK
                    // response delayed beyond expiry must not change this decision.
                    let reason = if message
                        .expires
                        .as_ref()
                        .is_some_and(|value| value.expired(chrono::Utc::now()))
                    {
                        inner
                            .revoked
                            .lock()
                            .map_err(|_| Error::Panic)?
                            .add(message.task_id);
                        Some(crate::cleanup_expiry::Revocation::Expired)
                    } else if cached {
                        Some(crate::cleanup_expiry::Revocation::Revoked)
                    } else {
                        None
                    };
                    if let Some(reason) = reason {
                        let (done, completed) = oneshot::channel();
                        let physical: Arc<dyn Send + Sync> =
                            Arc::new((owner.clone(), receipt.clone(), WriteCompletion(Some(done))));
                        let written = inner
                            .jobs
                            .write_default_revoked(&message, reason, physical.clone())
                            .await;
                        drop(physical);
                        let _ = completed.await;
                        let failure = match written {
                            Ok(saved) => match inner.jobs.ack_cleanup(&receipt).await {
                                Ok(true) => {
                                    return Ok(if saved {
                                        Run::Revoked(message.task_id, reason)
                                    } else {
                                        Run::AlreadySucceeded(message.task_id)
                                    })
                                }
                                Ok(false) => {
                                    return Err(Error::Transport(TransportError::LostReservation))
                                }
                                Err(error) => Error::Transport(error),
                            },
                            Err(error) => Error::Transport(error),
                        };
                        if let Some(sender) = outcome.take() {
                            let _ = sender.send(Err(failure));
                        }
                        warm_restore(inner.clone(), receipt).await;
                        // The operational outcome was already sent. No invented
                        // terminal result follows warm restoration.
                        return Err(Error::Closed);
                    }
                    // Source ACK3: prefork body/result can complete while the
                    // early ACK response is held. Two *existing* operation
                    // credits are required; one-credit configurations refuse
                    // this branch before POP rather than exceeding the budget.
                    let work = async {
                        let value = inner
                            .domains
                            .execute(kind, owner.clone())
                            .await
                            .map_err(Error::Domain)?;
                        let (done, completed) = oneshot::channel();
                        let physical: Arc<dyn Send + Sync> =
                            Arc::new((owner.clone(), receipt.clone(), WriteCompletion(Some(done))));
                        let result = inner
                            .jobs
                            .write_default_success(&message, value, physical.clone())
                            .await
                            .map_err(Error::Transport);
                        drop(physical);
                        let _ = completed.await;
                        result
                    };
                    let (ack, worked) = tokio::join!(inner.jobs.ack_cleanup(&receipt), work);
                    match ack {
                        Ok(true) => {
                            return worked.map(|saved| {
                                if saved {
                                    Run::Completed(message.task_id)
                                } else {
                                    Run::AlreadySucceeded(message.task_id)
                                }
                            })
                        }
                        Ok(false) => return Err(Error::Transport(TransportError::LostReservation)),
                        Err(error) => {
                            if let Some(sender) = outcome.take() {
                                let _ = sender.send(Err(Error::Transport(error)));
                            }
                            warm_restore(inner.clone(), receipt).await;
                            return Err(Error::Closed);
                        }
                    }
                }
                let message = inner
                    .jobs
                    .claim_default(candidate)
                    .await
                    .map_err(Error::Transport)?;
                let value = inner
                    .domains
                    .execute_message(message.clone(), owner.clone())
                    .await
                    .map_err(Error::Domain)?;
                let raised = value.raised();
                let saved = inner
                    .jobs
                    .write_default_task_terminal(&message, value, owner)
                    .await
                    .map_err(Error::Transport)?;
                Ok(if saved {
                    if raised {
                        Run::Failed(message.task_id)
                    } else {
                        Run::Completed(message.task_id)
                    }
                } else {
                    Run::AlreadySucceeded(message.task_id)
                })
            }
            .await;
            if let Some(sender) = outcome.take() {
                let _ = sender.send(result);
            }
        });
        received.await.map_err(|_| Error::Panic)?
    }
    pub(crate) fn request_stop(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.stop.send_replace(true);
    }
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.request_stop();
        matches!(
            tokio::time::timeout(timeout, self.inner.slots.clone().acquire_owned()).await,
            Ok(Ok(_))
        )
    }
}
async fn warm_restore(inner: Arc<Inner>, receipt: crate::transport::CleanupReservation) {
    let mut stopping = inner.stop.subscribe();
    if !*stopping.borrow() {
        let _ = stopping.wait_for(|value| *value).await;
    }
    loop {
        match inner.jobs.restore_cleanup(&receipt).await {
            Ok(_) => return,
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}
struct WriteCompletion(Option<oneshot::Sender<()>>);
impl Drop for WriteCompletion {
    fn drop(&mut self) {
        if let Some(done) = self.0.take() {
            let _ = done.send(());
        }
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
            Self::Failed(id) => format!("Failed({id})"),
            Self::Completed(id) => format!("Completed({id})"),
            Self::Revoked(id, reason) => format!("Revoked({id},{})", reason.reason()),
            Self::AlreadySucceeded(id) => format!("AlreadySucceeded({id})"),
        }
    }
}
