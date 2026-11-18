//! Regex-only dependency lifetime under an EXISTING admitted output owner.
//! Original Dispatcharr/CPython/regex authors retain algorithm credit.
use dispatcharr_store::{RegexExecutor, RegexNamedGroups, StoreError};
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, OnceLock, Weak,
    },
    time::Duration,
};
use tokio::sync::Notify;
use tokio::time::Instant;
type Owner = Arc<dyn Send + Sync>;
struct Owners(Mutex<Vec<Owner>>);
struct CallState {
    retiring: bool,
}
fn unsettled() -> StoreError {
    StoreError::Unsupported("Owned EPG regex operation physically unsettled")
}
struct RegistryInner {
    fault: AtomicBool,
    diagnostic: bool,
    records: AtomicUsize,
    drain: Mutex<Option<Instant>>,
    next: AtomicU64,
    flows: Mutex<HashMap<u64, Weak<Flow>>>,
    changed: Notify,
}
#[derive(Clone)]
pub struct Registry {
    inner: Arc<RegistryInner>,
}
impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}
impl Registry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                fault: AtomicBool::new(false),
                diagnostic: std::env::var("RUST_EPG_REGEX_DIAGNOSTICS").as_deref() == Ok("1"),
                records: AtomicUsize::new(0),
                drain: Mutex::new(None),
                next: AtomicU64::new(1),
                flows: Mutex::new(HashMap::new()),
                changed: Notify::new(),
            }),
        }
    }
    /// The caller supplies its already-owned request/build/memory Arc. This starts
    /// no process, SQL, pool or semaphore; the existing actor starts only on call.
    pub fn open(&self, owner: Owner, deadline: Instant) -> Result<Arc<Scope>, StoreError> {
        let mut flows = self.inner.flows.lock().map_err(|_| unsettled())?;
        // Only already-admitted view owners reach this constructor. Service
        // intake owns rejection; accepted work inherits the SAME earlier drain
        // deadline even if its lazy scope is registered during warm shutdown.
        let deadline = self
            .inner
            .drain
            .lock()
            .map_err(|_| unsettled())?
            .map_or(deadline, |drain| deadline.min(drain));
        if Instant::now() >= deadline {
            return Err(unsettled());
        }
        let retained = Arc::new(Owners(Mutex::new(vec![owner])));
        let owner: Owner = retained.clone();
        let id = self.inner.next.fetch_add(1, Ordering::Relaxed);
        let flow = Arc::new(Flow {
            id,
            regex: OnceLock::new(),
            owner,
            retained,
            registry: Arc::downgrade(&self.inner),
            deadline: Mutex::new(deadline),
            active: AtomicUsize::new(0),
            gate: Mutex::new(CallState { retiring: false }),
            retired: AtomicBool::new(false),
            fault: AtomicBool::new(false),
            changed: Notify::new(),
        });
        flows.insert(id, Arc::downgrade(&flow));
        Ok(Arc::new(Scope { inner: flow }))
    }
    pub fn begin_drain(&self, deadline: Instant) {
        if let Ok(mut drain) = self.inner.drain.lock() {
            *drain = Some(drain.map_or(deadline, |old| old.min(deadline)));
        }
        if let Ok(flows) = self.inner.flows.lock() {
            for flow in flows.values().filter_map(Weak::upgrade) {
                flow.set_deadline(deadline);
            }
        }
    }
    pub async fn shutdown(&self, deadline: Instant) -> bool {
        self.begin_drain(deadline);
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.inner.flows.lock().is_ok_and(|flows| flows.is_empty()) {
                return !self.inner.fault.load(Ordering::Acquire);
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return false;
            }
        }
    }
}
struct Flow {
    id: u64,
    regex: OnceLock<RegexExecutor>,
    owner: Owner,
    retained: Arc<Owners>,
    registry: Weak<RegistryInner>,
    deadline: Mutex<Instant>,
    active: AtomicUsize,
    gate: Mutex<CallState>,
    retired: AtomicBool,
    fault: AtomicBool,
    changed: Notify,
}
impl Flow {
    fn set_deadline(&self, deadline: Instant) {
        // SAME gate as lazy host publication: drain cannot observe None while
        // the first caller initializes an actor with the older view deadline.
        let Ok(_gate) = self.gate.lock() else {
            self.fault.store(true, Ordering::Release);
            return;
        };
        let deadline = if let Ok(mut current) = self.deadline.lock() {
            *current = (*current).min(deadline);
            *current
        } else {
            self.fault.store(true, Ordering::Release);
            Instant::now()
        };
        if let Some(regex) = self.regex.get() {
            regex.interrupt_at(deadline);
        }
    }
    fn deadline(&self) -> Instant {
        self.deadline
            .lock()
            .map(|v| *v)
            .unwrap_or_else(|_| Instant::now())
    }
    fn complete(&self, normal: bool) {
        if !normal {
            self.fault.store(true, Ordering::Release);
        }
        self.retired.store(true, Ordering::Release);
        self.changed.notify_waiters();
        if let Some(registry) = self.registry.upgrade() {
            if self.fault.load(Ordering::Acquire) {
                registry.fault.store(true, Ordering::Release);
            }
            if let Ok(mut flows) = registry.flows.lock() {
                flows.remove(&self.id);
            }
            registry.changed.notify_waiters();
        }
    }
    fn record(&self, receipt: &dispatcharr_store::PythonRegexReceipt) {
        if let Some(registry) = self.registry.upgrade() {
            if registry.diagnostic && registry.records.fetch_add(1, Ordering::Relaxed) < 64 {
                eprintln!(
                    "epg_regex_retirement {}",
                    json!({"scope":self.id,"pid":receipt.identity.pid,"generation":receipt.identity.start_generation,"actual_exit":receipt.actual_exit,"wait_observed":receipt.wait_observed,"stdout_eof":receipt.stdout_eof,"stderr_eof":receipt.stderr_eof,"group_absent":receipt.descendants_absent,"callbacks_settled":receipt.callbacks_settled,"forced":receipt.forced,"operation_fault":self.fault.load(Ordering::Acquire)})
                );
            }
        }
    }
    fn start_retirement(flow: Arc<Self>, aborted: bool) {
        let regex = {
            let Ok(mut gate) = flow.gate.lock() else {
                return;
            };
            if gate.retiring {
                return;
            }
            gate.retiring = true;
            flow.regex.get().cloned()
        };
        let Some(regex) = regex else {
            flow.complete(true);
            return;
        };
        if aborted && flow.active.load(Ordering::Acquire) > 0 {
            flow.set_deadline(Instant::now());
        }
        tokio::spawn(async move {
            let budget = flow.deadline().saturating_duration_since(Instant::now());
            let outcome = regex.shutdown(budget).await;
            let normal = outcome.is_ok() && !flow.fault.load(Ordering::Acquire);
            if let Ok(Some(receipt)) = &outcome {
                flow.record(receipt);
            }
            // Logical timeout is not physical retirement. A failed observer keeps this
            // same admitted owner/registry entry until actual wait+bothEOF+group proof.
            if outcome.is_err() {
                flow.fault.store(true, Ordering::Release);
                loop {
                    // A sealed actual normal supervisor epilogue, with NO OS
                    // spawn attempt ever entered, is a distinct no-process
                    // observation. Operation still remains Err/faulted.
                    if regex.no_spawn_retirement().is_some() {
                        break;
                    }
                    if let Some(receipt) = regex.last_receipt().await {
                        if receipt.wait_observed
                            && receipt.stdout_eof
                            && receipt.stderr_eof
                            && receipt.descendants_absent
                            && receipt.callbacks_settled
                        {
                            flow.record(&receipt);
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
            flow.complete(normal);
        });
    }
}
/// This wrapper intentionally has NO Clone. Only external Arc<Scope> clones
/// exist; final wrapper Drop starts cleanup independently of internal RPC Arcs.
/// Actual retirement proof for this scope only; no task/publication authority.
pub struct CompletedScope {
    inner: Arc<Flow>,
}
pub struct Scope {
    inner: Arc<Flow>,
}
impl Scope {
    pub fn retain(&self, owner: Owner) -> Result<(), StoreError> {
        self.inner
            .retained
            .0
            .lock()
            .map_err(|_| unsettled())?
            .push(owner);
        Ok(())
    }
    pub fn retain_workspace(&self, workspace: Arc<Workspace>) -> Result<(), StoreError> {
        workspace
            .bound
            .set(Arc::downgrade(&self.inner))
            .map_err(|_| unsettled())?;
        self.retain(workspace)
    }
    fn begin_call(&self) -> Result<(Arc<Flow>, RegexExecutor), StoreError> {
        let flow = self.inner.clone();
        {
            let gate = flow.gate.lock().map_err(|_| unsettled())?;
            if gate.retiring {
                return Err(unsettled());
            }
            flow.active.fetch_add(1, Ordering::AcqRel);
            let regex = flow
                .regex
                .get_or_init(|| {
                    let regex = RegexExecutor::prepare(
                        "/usr/local/bin/dispatcharr-regex-python".into(),
                        flow.owner.clone(),
                    );
                    regex.interrupt_at(flow.deadline());
                    regex
                })
                .clone();
            Ok((flow.clone(), regex))
        }
    }
    pub async fn groups(
        &self,
        pattern: String,
        names: Vec<String>,
    ) -> Result<RegexNamedGroups, StoreError> {
        let (flow, regex) = self.begin_call()?;
        tokio::spawn(async move {
            let result = regex
                .search_named_groups(pattern, &names, flow.owner.clone())
                .await;
            flow.active.fetch_sub(1, Ordering::AcqRel);
            if result.is_err() {
                flow.fault.store(true, Ordering::Release);
            }
            result.map_err(|_| unsettled())
        })
        .await
        .map_err(|_| unsettled())?
    }
    pub async fn normalize(&self, names: Vec<String>) -> Result<Vec<String>, StoreError> {
        let (flow, regex) = self.begin_call()?;
        tokio::spawn(async move {
            let result = regex
                .normalize_capture_values(&names, flow.owner.clone())
                .await;
            flow.active.fetch_sub(1, Ordering::AcqRel);
            if result.is_err() {
                flow.fault.store(true, Ordering::Release);
            }
            result.map_err(|_| unsettled())
        })
        .await
        .map_err(|_| unsettled())?
    }
    /// Same existing output owner/permit/memory/deadline; no independent pool.
    pub async fn profile_urls(
        &self,
        rows: Vec<serde_json::Value>,
    ) -> Result<serde_json::Value, StoreError> {
        let (flow, regex) = self.begin_call()?;
        tokio::spawn(async move {
            let result = regex
                .execute(
                    json!({"kind":"profile_urls","rows":rows}),
                    flow.owner.clone(),
                )
                .await;
            flow.active.fetch_sub(1, Ordering::AcqRel);
            if result.is_err() {
                flow.fault.store(true, Ordering::Release);
            }
            result.map_err(|_| unsettled())
        })
        .await
        .map_err(|_| unsettled())?
    }
    pub async fn finish(&self, aborted: bool) -> Result<CompletedScope, StoreError> {
        Flow::start_retirement(self.inner.clone(), aborted);
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.inner.retired.load(Ordering::Acquire) {
                return if self.inner.fault.load(Ordering::Acquire) {
                    Err(unsettled())
                } else {
                    Ok(CompletedScope {
                        inner: self.inner.clone(),
                    })
                };
            }
            if tokio::time::timeout_at(self.inner.deadline(), changed)
                .await
                .is_err()
            {
                return Err(unsettled());
            }
        }
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        Flow::start_retirement(self.inner.clone(), true);
    }
}
/// Existing memory grants can be retained by actor ownership while a detached
/// caller settles. The gateway takes it only AFTER joined scope retirement.
pub struct Workspace {
    permit: Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,
    bound: OnceLock<Weak<Flow>>,
}
impl Workspace {
    pub fn new(permit: tokio::sync::OwnedSemaphorePermit) -> Self {
        Self {
            permit: Mutex::new(Some(permit)),
            bound: OnceLock::new(),
        }
    }
    pub fn take_after_retirement(
        &self,
        proof: &CompletedScope,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, StoreError> {
        let bound = self
            .bound
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(unsettled)?;
        if !Arc::ptr_eq(&bound, &proof.inner) {
            return Err(unsettled());
        }
        self.permit
            .lock()
            .map_err(|_| unsettled())?
            .take()
            .ok_or_else(unsettled)
    }
}
pub fn capacity() -> StoreError {
    StoreError::StatusBody(503, json!({"detail":"Custom guide capacity unavailable."}))
}
pub fn unsupported() -> StoreError {
    StoreError::StatusBody(
        501,
        json!({"detail":"This custom guide configuration is not supported."}),
    )
}

#[cfg(test)]
mod retirement_proof_tests {
    use super::*;
    #[tokio::test]
    async fn actual_admission_workspace_requires_its_own_completed_scope() {
        let permits = Arc::new(tokio::sync::Semaphore::new(4));
        let workspace = Arc::new(Workspace::new(
            permits.clone().acquire_many_owned(4).await.unwrap(),
        ));
        let registry = Registry::new();
        let a = registry
            .open(Arc::new(()), Instant::now() + Duration::from_secs(5))
            .unwrap();
        a.retain_workspace(workspace.clone()).unwrap();
        let b = registry
            .open(Arc::new(()), Instant::now() + Duration::from_secs(5))
            .unwrap();
        let other = b.finish(false).await.unwrap();
        assert!(workspace.take_after_retirement(&other).is_err());
        assert_eq!(permits.available_permits(), 0);
        let actual = a.finish(false).await.unwrap();
        assert!(a.inner.regex.get().is_none());
        let returned = workspace.take_after_retirement(&actual).unwrap();
        assert_eq!(returned.num_permits(), 4);
        drop(returned);
        assert_eq!(permits.available_permits(), 4);
        assert!(
            registry
                .shutdown(Instant::now() + Duration::from_secs(1))
                .await
        );
    }
}
