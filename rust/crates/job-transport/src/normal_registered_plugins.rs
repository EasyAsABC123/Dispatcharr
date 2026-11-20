//! Actual registered user-plugin family on normal Main's shared Celery budget.
//! SQL/actor admission occurs only at actual execution, not future timer intake.
//! Original Dispatcharr, Celery and Kombu authors retain full algorithm credit.
use crate::{
    milestone_dispatch::{Disposition as D, Family, Handler, Lifecycle},
    normal_revocation::Revoked,
    registered_plugin_eta::{Limits, Reservation},
    transport::{Error, Jobs},
};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_store::PluginRuntime;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::time::Instant;
use uuid::Uuid;
type Owner = Arc<dyn Send + Sync>;
struct Pending {
    receipt: Reservation,
    running: bool,
    retry_at: Instant,
}
struct Inner {
    jobs: Jobs,
    runtime: PluginRuntime,
    revoked: Revoked,
    pending: Mutex<VecDeque<Pending>>,
    closed: AtomicBool,
    visibility: AtomicUsize,
    next_visibility: Mutex<Option<Instant>>,
}
#[derive(Clone)]
pub struct Prepared {
    inner: Arc<Inner>,
}
fn fault(text: &str) -> D {
    D::Operational(text.into())
}
fn transport(error: Error) -> D {
    match error {
        Error::Capacity => D::Capacity,
        Error::ClaimRace => D::HeadChanged,
        Error::UnsupportedTask => D::UnsupportedHead,
        _ => fault("Registered plugin broker operation unsettled"),
    }
}
struct Running {
    inner: Arc<Inner>,
    tag: Uuid,
    terminal: bool,
}
impl Drop for Running {
    fn drop(&mut self) {
        let mut pending = self.inner.pending.lock().unwrap_or_else(|e| e.into_inner());
        if self.terminal {
            pending.retain(|p| p.receipt.delivery_tag() != self.tag);
        } else if let Some(p) = pending
            .iter_mut()
            .find(|p| p.receipt.delivery_tag() == self.tag)
        {
            p.running = false;
            p.retry_at = Instant::now() + Duration::from_millis(100);
        }
    }
}
impl Prepared {
    pub fn prepare(jobs: Jobs, runtime: PluginRuntime, revoked: Revoked) -> Result<Self, D> {
        if !jobs.allows_defaults() {
            return Err(D::UnsupportedDomain);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                runtime,
                revoked,
                pending: Mutex::new(VecDeque::new()),
                closed: AtomicBool::new(false),
                visibility: AtomicUsize::new(0),
                next_visibility: Mutex::new(None),
            }),
        })
    }
    pub(crate) fn runtime(&self) -> &PluginRuntime {
        &self.inner.runtime
    }

    pub fn entry(&self) -> (Family, Arc<dyn Handler>) {
        (Family::RegisteredPlugin, Arc::new(self.clone()))
    }
    async fn reserve(&self, expected: Arc<[u8]>, caller: Owner) -> Result<D, D> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Ok(D::NotDue);
        }
        let capability = self
            .inner
            .runtime
            .envelope_capability(&expected)
            .map_err(|_| D::UnsupportedHead)?;
        let candidate = self
            .inner
            .jobs
            .peek_registered_plugin_candidate(
                Some(&expected),
                capability,
                caller.clone(),
                Limits::default(),
            )
            .await
            .map_err(transport)?
            .ok_or(D::HeadChanged)?;
        let receipt = self
            .inner
            .jobs
            .reserve_registered_plugin_candidate_settled(candidate, caller, Limits::default())
            .await
            .map_err(transport)?;
        let id = receipt.message().id;
        self.inner
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push_back(Pending {
                receipt,
                running: false,
                retry_at: Instant::now(),
            });
        // Reserving, including an already-due task, never holds the selected global
        // execution owner after this returns. Actual due dispatch reacquires that SAME
        // normal queue six-slot budget; no additional family semaphore is created.
        Ok(D::Reserved(id))
    }
    async fn execute(&self, raw: Arc<[u8]>, caller: Owner) -> Result<D, D> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Ok(D::NotDue);
        }
        let receipt = {
            let mut pending = self.inner.pending.lock().unwrap_or_else(|e| e.into_inner());
            let Some(p) = pending.iter_mut().find(|p| {
                !p.running
                    && p.retry_at <= Instant::now()
                    && p.receipt.receipt.queued_bytes.as_slice() == raw.as_ref()
                    && p.receipt
                        .message()
                        .eta
                        .is_none_or(|t| t <= chrono::Utc::now())
            }) else {
                return Ok(D::NotDue);
            };
            p.running = true;
            p.receipt.clone()
        };
        let mut running = Running {
            inner: self.inner.clone(),
            tag: receipt.delivery_tag(),
            terminal: false,
        };
        let id = receipt.message().id;
        // Caller already owns actual common Celery admission. The runtime borrows
        // one of the SAME Auth4 writer/session admissions, or its actual causal
        // parent writer witness, and one physically distinct existing pool8 session.
        let admitted = match self.inner.runtime.prepare_task(raw, caller.clone()).await {
            Ok(v) => v,
            Err(dispatcharr_store::StoreError::StatusBody(503, _)) => return Ok(D::Capacity),
            Err(_) => return Err(fault("Registered plugin actor/session admission failed")),
        };
        let mut admission = Some(admitted);
        let result = async {
            let expired = receipt
                .message()
                .expires
                .is_some_and(|t| t <= chrono::Utc::now());
            let revoked = self
                .inner
                .revoked
                .contains(&id.to_string())
                .map_err(|_| fault("Registered plugin revoked state unavailable"))?;
            if expired || revoked {
                if expired {
                    self.inner
                        .revoked
                        .apply(&[id.to_string()], caller.clone())
                        .map_err(|_| fault("Registered plugin expiry revoke unavailable"))?;
                }
                self.inner
                    .runtime
                    .announce_revoked(
                        admission
                            .as_ref()
                            .ok_or_else(|| fault("Plugin admission missing"))?,
                        expired,
                    )
                    .await
                    .map_err(|_| {
                        fault(
                            "Registered plugin revoked backend unsettled; receipt remains durable",
                        )
                    })?;
                self.inner
                    .jobs
                    .ack_registered_plugin_eta(&receipt, caller.clone())
                    .await
                    .map_err(transport)?;
                self.inner
                    .runtime
                    .actual_task_acknowledged(id, &receipt.receipt.queued_bytes)
                    .map_err(|_| fault("Plugin actual ACK provenance settlement failed"))?;
                running.terminal = true;
                self.inner
                    .runtime
                    .finish_revocation(
                        admission
                            .take()
                            .ok_or_else(|| fault("Plugin admission missing"))?,
                        expired,
                    )
                    .await
                    .map_err(|_| fault("Registered plugin post-ACK revoked signal unsettled"))?;
                return Ok(D::Revoked(id));
            }
            // Task policy comes from the actually loaded task actor, not a producer's
            // mutable JSON flags. Request.on_accepted early ACK precedes its user trace.
            let policy = admission
                .as_ref()
                .ok_or_else(|| fault("Plugin admission missing"))?
                .capability()
                .policy()
                .clone();
            if !policy.acks_late {
                if let Err(e) = self
                    .inner
                    .jobs
                    .ack_registered_plugin_eta(&receipt, caller.clone())
                    .await
                {
                    self.inner
                        .runtime
                        .release_task(
                            admission
                                .take()
                                .ok_or_else(|| fault("Plugin admission missing"))?,
                        )
                        .await
                        .map_err(|_| fault("Registered plugin pretrace SQL close unsettled"))?;
                    return Err(transport(e));
                }
                self.inner
                    .runtime
                    .actual_task_acknowledged(id, &receipt.receipt.queued_bytes)
                    .map_err(|_| fault("Plugin actual ACK provenance settlement failed"))?;
                running.terminal = true;
            }
            let result = self
                .inner
                .runtime
                .execute_after_actual_admission(
                    admission
                        .take()
                        .ok_or_else(|| fault("Plugin admission missing"))?,
                )
                .await
                .map_err(|_| fault("Registered plugin trace/physical session unsettled"))?;
            let kind = result["actual_trace_kind"]
                .as_str()
                .ok_or_else(|| fault("Registered plugin actual trace receipt missing"))?;
            if result["task_id"].as_str() != Some(id.to_string().as_str()) {
                return Err(fault("Registered plugin actual trace identity mismatch"));
            }
            if policy.acks_late {
                if kind == "reject" && result["actual_reject_requeue"] == true {
                    self.inner
                        .jobs
                        .reject_requeue_registered_plugin_eta(&receipt, caller.clone())
                        .await
                        .map_err(transport)?;
                } else {
                    // Source success/Retry/Ignore ACK, failure without ACK-on-failure rejects
                    // without requeue. Both remove the exact receipt; this is not a success
                    // result synthesis. Actual backend metadata is produced by pinned trace.
                    self.inner
                        .jobs
                        .ack_registered_plugin_eta(&receipt, caller)
                        .await
                        .map_err(transport)?;
                }
                if kind == "reject" && result["actual_reject_requeue"] == true {
                    self.inner.runtime.actual_task_restored(
                        id,
                        &receipt.receipt.queued_bytes,
                        &receipt.receipt.restored_bytes,
                    )
                } else {
                    self.inner
                        .runtime
                        .actual_task_acknowledged(id, &receipt.receipt.queued_bytes)
                }
                .map_err(|_| fault("Plugin actual ACK/requeue provenance settlement failed"))?;
                running.terminal = true;
            }
            match kind {
                "success" | "ignore" | "retry" | "reject" => Ok(D::Completed(id)),
                "failure" => Ok(D::TaskFailed(id)),
                _ => Err(fault(
                    "Registered plugin unknown actual trace classification",
                )),
            }
        }
        .await;
        if let Some(admission) = admission {
            self.inner
                .runtime
                .release_task(admission)
                .await
                .map_err(|_| fault("Registered plugin SQL retirement unsettled"))?;
        }
        result
    }
}
impl Handler for Prepared {
    fn maximum_concurrency(&self) -> usize {
        6
    }
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn registered_head(&self, raw: &[u8]) -> bool {
        self.inner.runtime.registered_head(raw)
    }
    fn run_once(&self, _owner: Owner) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(async { Err(D::UnsupportedDomain) })
    }
    fn run_selected(&self, raw: Arc<[u8]>, owner: Owner) -> BoxFuture<'_, Result<D, D>> {
        let this = self.clone();
        Box::pin(async move {
            tokio::spawn(async move { this.reserve(raw, owner).await })
                .await
                .map_err(|_| fault("Registered plugin reserve join unsettled"))?
        })
    }
    fn due_head(&self) -> Option<Arc<[u8]>> {
        if self.inner.closed.load(Ordering::Acquire) {
            return None;
        }
        self.inner
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|p| {
                !p.running
                    && p.retry_at <= Instant::now()
                    && p.receipt
                        .message()
                        .eta
                        .is_none_or(|t| t <= chrono::Utc::now())
            })
            .min_by_key(|p| p.receipt.message().eta)
            .map(|p| Arc::from(p.receipt.receipt.queued_bytes.clone()))
    }
    fn run_due(&self, raw: Arc<[u8]>, owner: Owner) -> BoxFuture<'_, Result<D, D>> {
        let this = self.clone();
        Box::pin(async move {
            tokio::spawn(async move { this.execute(raw, owner).await })
                .await
                .map_err(|_| fault("Registered plugin execute join unsettled"))?
        })
    }
    fn maintenance(&self, owner: Owner) -> BoxFuture<'_, Result<(), D>> {
        Box::pin(async move {
            if self.inner.closed.load(Ordering::Acquire) {
                return Ok(());
            }
            {
                let mut next = self
                    .inner
                    .next_visibility
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if next.is_some_and(|t| t > Instant::now()) {
                    return Ok(());
                }
                *next = Some(Instant::now() + Duration::from_secs(1));
            }
            let cursor = self.inner.visibility.load(Ordering::Acquire);
            let next = self
                .inner
                .jobs
                .recover_registered_plugin_visible(self.inner.runtime.clone(), owner, cursor)
                .await
                .map_err(transport)?;
            self.inner.visibility.store(next, Ordering::Release);
            Ok(())
        })
    }
}
impl Lifecycle for Prepared {
    fn stop_admission(&self) {
        self.inner.closed.store(true, Ordering::Release)
    }
    fn begin_drain(&self, budget: Duration) {
        self.inner.runtime.interrupt_at(Instant::now() + budget)
    }
    fn shutdown(&self, budget: Duration) -> BoxFuture<'_, Result<(), D>> {
        Box::pin(async move {
            self.stop_admission();
            let deadline = Instant::now() + budget;
            loop {
                let next = {
                    let pending = self.inner.pending.lock().unwrap_or_else(|e| e.into_inner());
                    if pending.iter().any(|p| p.running) {
                        return Err(fault("Registered plugin physical execution remains active"));
                    }
                    pending.front().map(|p| p.receipt.clone())
                };
                let Some(receipt) = next else { break };
                self.inner
                    .jobs
                    .restore_registered_plugin_eta(&receipt, self.inner.clone())
                    .await
                    .map_err(transport)?;
                self.inner
                    .runtime
                    .actual_task_restored(
                        receipt.message().id,
                        &receipt.receipt.queued_bytes,
                        &receipt.receipt.restored_bytes,
                    )
                    .map_err(|_| fault("Plugin actual warm restore provenance failed"))?;
                self.inner
                    .pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .pop_front();
                if Instant::now() >= deadline {
                    return Err(fault("Registered plugin warm restore budget elapsed"));
                }
            }
            self.inner
                .runtime
                .shutdown(deadline.saturating_duration_since(Instant::now()))
                .await
                .map_err(|_| fault("Registered plugin host drain unsettled"))
        })
    }
}
