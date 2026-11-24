//! Normal XML TVG durable15-second deferrals; same worker, Jobs and ETA budget.
//! Original Dispatcharr/Celery/Kombu authors retain application/protocol credit.
use super::*;
use crate::{
    milestone_dispatch::{Disposition as D, Handler, Lifecycle},
    normal_revocation::Revoked,
    transport::{Error as Wire, Jobs},
    xml_tvg_eta::Reservation,
};
use dispatcharr_contracts::BoxFuture;
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::Notify;
type Owner = Arc<dyn Send + Sync>;
pub(super) struct HandlerImpl {
    inner: Arc<Shared>,
}
struct Shared {
    app: Application,
    jobs: Jobs,
    revoked: Revoked,
    stopped: Arc<AtomicBool>,
    pending: Mutex<VecDeque<Reservation>>,
    active: AtomicUsize,
    changed: Notify,
    heads: Arc<Notify>,
    uncertain: Mutex<Vec<Owner>>,
    cursor: AtomicUsize,
    next: Mutex<Option<tokio::time::Instant>>,
}
struct Active(Arc<Shared>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}
struct Accepted {
    inner: Arc<Shared>,
    owner: Owner,
    armed: bool,
}
impl Drop for Accepted {
    fn drop(&mut self) {
        if self.armed {
            self.inner
                .uncertain
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(self.owner.clone());
        }
    }
}
fn fault(stage: &str) -> D {
    D::Operational(format!("normal XML TVG {stage} unsettled"))
}
fn wire(e: Wire) -> D {
    match e {
        Wire::Capacity => D::Capacity,
        Wire::ClaimRace => D::HeadChanged,
        Wire::UnsupportedTask => D::UnsupportedHead,
        _ => fault("broker"),
    }
}
impl HandlerImpl {
    pub(super) fn prepare(
        app: Application,
        jobs: Jobs,
        identity: dispatcharr_store::ProducerIdentitySource,
        revoked: Revoked,
    ) -> Result<Arc<dyn Handler>, &'static str> {
        let stopped = Arc::new(AtomicBool::new(false));
        app.inner
            .jobs
            .bind_shared(jobs.clone(), identity, stopped.clone())
            .map_err(|_| "normal TVG shared broker binding failed")?;
        app.inner.jobs.enable_normal_intake();
        Ok(Arc::new(Self {
            inner: Arc::new(Shared {
                app,
                jobs,
                revoked,
                stopped,
                pending: Mutex::new(VecDeque::new()),
                active: AtomicUsize::new(0),
                changed: Notify::new(),
                heads: Arc::new(Notify::new()),
                uncertain: Mutex::new(Vec::new()),
                cursor: AtomicUsize::new(0),
                next: Mutex::new(None),
            }),
        }))
    }
    async fn execute(&self, expected: Option<Arc<[u8]>>, due: bool, caller: Owner) -> Result<D, D> {
        if self.inner.stopped.load(Ordering::Acquire) {
            return Ok(D::NotDue);
        }
        let inner = self.inner.clone();
        inner.active.fetch_add(1, Ordering::AcqRel);
        tokio::spawn(async move {
            let _active = Active(inner.clone());
            let owner: Owner = Arc::new((caller, inner.clone()));
            let mut accepted = Accepted {
                inner: inner.clone(),
                owner: owner.clone(),
                armed: false,
            };
            let raw = if let Some(raw) = expected {
                raw
            } else {
                let Some(raw) = inner
                    .jobs
                    .peek_xml_tvg_raw(owner.clone())
                    .await
                    .map_err(wire)?
                else {
                    return Ok(D::Empty);
                };
                Arc::from(raw)
            };
            let message = crate::xml_tvg_protocol::parse(&raw).map_err(|_| D::UnsupportedHead)?;
            if !due && message.eta.is_some() {
                let Some(candidate) = inner
                    .jobs
                    .peek_xml_tvg_candidate(Some(&raw), owner.clone(), Default::default(), false)
                    .await
                    .map_err(wire)?
                else {
                    return Ok(D::Empty);
                };
                accepted.armed = true;
                let reserved = match inner
                    .jobs
                    .reserve_xml_tvg_candidate_settled(candidate, owner.clone(), Default::default())
                    .await
                {
                    Ok(reserved) => reserved,
                    Err(error @ (Wire::Capacity | Wire::ClaimRace)) => {
                        accepted.armed = false;
                        return Ok(wire(error));
                    }
                    Err(error) => return Err(wire(error)),
                };
                inner
                    .pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push_back(reserved.clone());
                inner.heads.notify_one();
                if inner
                    .revoked
                    .contains(&message.common.id.to_string())
                    .map_err(|_| fault("revoked read"))?
                {
                    inner
                        .jobs
                        .write_xc_live_revoked(&message.common, owner.clone())
                        .await
                        .map_err(wire)?;
                    inner
                        .jobs
                        .ack_xml_tvg_eta(&reserved, owner)
                        .await
                        .map_err(wire)?;
                    inner
                        .pending
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .retain(|v| v.delivery_tag() != reserved.delivery_tag());
                    accepted.armed = false;
                    return Ok(D::Revoked(message.common.id));
                }
                accepted.armed = false;
                return Ok(D::Reserved(message.common.id));
            }
            let reserved = if due {
                let pending = inner.pending.lock().unwrap_or_else(|e| e.into_inner());
                Some(
                    pending
                        .iter()
                        .find(|v| {
                            v.receipt.queued_bytes.as_slice() == raw.as_ref()
                                && v.message().eta.is_some_and(|e| e <= chrono::Utc::now())
                        })
                        .cloned()
                        .ok_or(D::NotDue)?,
                )
            } else {
                None
            };
            if inner
                .revoked
                .contains(&message.common.id.to_string())
                .map_err(|_| fault("revoked read"))?
            {
                let reserved = if let Some(reserved) = reserved {
                    accepted.armed = true;
                    reserved
                } else {
                    // A revoked immediate Source request still has an
                    // unacknowledged broker reservation until its request
                    // result is written. Keep the exact recoverable tuple.
                    let candidate = match inner
                        .jobs
                        .peek_xml_tvg_candidate(Some(&raw), owner.clone(), Default::default(), true)
                        .await
                    {
                        Ok(Some(candidate)) => candidate,
                        Ok(None) | Err(Wire::ClaimRace) => return Ok(D::HeadChanged),
                        Err(Wire::Capacity) => return Ok(D::Capacity),
                        Err(error) => return Err(wire(error)),
                    };
                    accepted.armed = true;
                    match inner
                        .jobs
                        .reserve_xml_tvg_candidate_settled(
                            candidate,
                            owner.clone(),
                            Default::default(),
                        )
                        .await
                    {
                        Ok(reserved) => reserved,
                        Err(error @ (Wire::Capacity | Wire::ClaimRace)) => {
                            accepted.armed = false;
                            return Ok(wire(error));
                        }
                        Err(error) => return Err(wire(error)),
                    }
                };
                inner
                    .jobs
                    .write_xc_live_revoked(&message.common, owner.clone())
                    .await
                    .map_err(wire)?;
                inner
                    .jobs
                    .ack_xml_tvg_eta(&reserved, owner)
                    .await
                    .map_err(wire)?;
                inner
                    .pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .retain(|v| v.delivery_tag() != reserved.delivery_tag());
                inner.heads.notify_one();
                accepted.armed = false;
                return Ok(D::Revoked(message.common.id));
            }
            accepted.armed = true;
            let result = if let Some(reserved) = reserved.clone() {
                inner.app.inner.worker.run_reserved(reserved, owner).await
            } else {
                inner.app.inner.worker.run_expected(Some(raw), owner).await
            };
            let result = match result {
                Ok(Run::Empty) => Ok(D::Empty),
                Ok(Run::UnsupportedHead) => Ok(D::UnsupportedHead),
                Ok(Run::Completed(id)) => Ok(D::Completed(id)),
                Ok(Run::Failed(id)) => Ok(D::TaskFailed(id)),
                Err(Error::Capacity) => Ok(D::Capacity),
                Err(Error::Transport(Wire::ClaimRace)) => Ok(D::HeadChanged),
                Err(_) => Err(fault("execution")),
            };
            if result.is_ok() {
                accepted.armed = false
            }
            if result
                .as_ref()
                .is_ok_and(|d| matches!(d, D::Completed(_) | D::TaskFailed(_)))
            {
                if let Some(reserved) = reserved {
                    inner
                        .pending
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .retain(|v| v.delivery_tag() != reserved.delivery_tag());
                }
                inner.heads.notify_one();
            }
            result
        })
        .await
        .map_err(|_| fault("supervisor"))?
    }
}
impl Handler for HandlerImpl {
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn head_advance_notifier(&self) -> Option<Arc<Notify>> {
        Some(self.inner.heads.clone())
    }
    fn run_once(&self, owner: Owner) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.execute(None, false, owner))
    }
    fn run_selected(&self, raw: Arc<[u8]>, owner: Owner) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.execute(Some(raw), false, owner))
    }
    fn due_head(&self) -> Option<Arc<[u8]>> {
        if self.inner.stopped.load(Ordering::Acquire) {
            return None;
        }
        self.inner
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|v| v.message().eta.is_some_and(|e| e <= chrono::Utc::now()))
            .min_by_key(|v| v.message().eta)
            .map(|v| Arc::from(v.receipt.queued_bytes.clone()))
    }
    fn run_due(&self, raw: Arc<[u8]>, owner: Owner) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.execute(Some(raw), true, owner))
    }
    fn maintenance(&self, owner: Owner) -> BoxFuture<'_, Result<(), D>> {
        Box::pin(async move {
            {
                let mut next = self.inner.next.lock().unwrap_or_else(|e| e.into_inner());
                let now = tokio::time::Instant::now();
                if next.is_some_and(|v| v > now) {
                    return Ok(());
                }
                *next = Some(now + Duration::from_secs(5));
            }
            let cursor = self.inner.cursor.load(Ordering::Acquire);
            let next = self
                .inner
                .jobs
                .recover_xml_tvg_visible(owner, cursor)
                .await
                .map_err(wire)?;
            self.inner.cursor.store(next, Ordering::Release);
            Ok(())
        })
    }
}
impl Lifecycle for HandlerImpl {
    fn stop_admission(&self) {
        self.inner.stopped.store(true, Ordering::Release);
    }
    fn shutdown(&self, budget: Duration) -> BoxFuture<'_, Result<(), D>> {
        Box::pin(async move {
            self.stop_admission();
            let until = tokio::time::Instant::now() + budget;
            loop {
                let changed = self.inner.changed.notified();
                if self.inner.active.load(Ordering::Acquire) == 0 {
                    break;
                }
                if tokio::time::timeout_at(until, changed).await.is_err() {
                    return Err(fault("active drain"));
                }
            }
            if !self
                .inner
                .uncertain
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty()
            {
                return Err(fault("accepted ownership"));
            }
            loop {
                let reserved = self
                    .inner
                    .pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .front()
                    .cloned();
                let Some(reserved) = reserved else { break };
                let owner: Owner = self.inner.clone();
                inner_restore(&self.inner, &reserved, owner).await?;
                self.inner
                    .pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .pop_front();
                if tokio::time::Instant::now() > until {
                    return Err(fault("warm restore deadline"));
                }
            }
            if self
                .inner
                .app
                .shutdown(until.saturating_duration_since(tokio::time::Instant::now()))
                .await
            {
                Ok(())
            } else {
                Err(fault("legacy physical drain"))
            }
        })
    }
}
async fn inner_restore(inner: &Shared, reserved: &Reservation, owner: Owner) -> Result<(), D> {
    inner
        .jobs
        .restore_xml_tvg_eta(reserved, owner)
        .await
        .map_err(wire)
}
