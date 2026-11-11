//! Private atomic runtime adapter. No route/application acceptance yet.
//! Original Dispatcharr authors retain all application and algorithm credit.
use super::*;
use crate::vod_api_draft::{
    vod_api_fair_budget::{queue_capacity, Budget, Grant, Need},
    vod_api_phase_scope::{Frame, Scope, Shape},
};
use std::sync::{atomic::AtomicUsize, Mutex};

pub(super) struct FairRuntime {
    pub(super) budget: Budget,
    inputs: Arc<Semaphore>,
    jobs: AtomicUsize,
    changed: tokio::sync::Notify,
}
/// Input bytes and the input-count slot have a separate immutable owner.
/// Worker/HTTP/work grants live only in `phase` and may be lent explicitly.
pub(crate) struct RequestScope {
    pub(super) phase: Scope,
    pub(super) runtime: Arc<FairRuntime>,
    _input: Mutex<Grant>,
    _count: OwnedSemaphorePermit,
    pub(super) error_frame: Mutex<Option<Frame>>,
    input_bytes: usize,
    claimed: AtomicBool,
}
#[derive(Clone)]
pub(crate) enum HttpScope {
    Legacy(Arc<OwnedSemaphorePermit>),
    Fair(Arc<RequestScope>),
}
impl HttpScope {
    pub(super) fn reject_unstarted(&self, runtime: &Arc<FairRuntime>) {
        if let Self::Fair(request) = self {
            if Arc::ptr_eq(runtime, &request.runtime)
                && request
                    .claimed
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                let _ = request.phase.abort_after_actor_end();
            }
        }
    }
}
impl RequestScope {
    pub(super) fn mark_input(&self, queued: bool) -> Result<(), ReadError> {
        let mut grant = self._input.lock().expect("input ownership");
        self.runtime
            .budget
            .replace(
                &mut grant,
                Need {
                    bytes: self.input_bytes,
                    queued_bytes: if queued { self.input_bytes } else { 0 },
                    ..Need::default()
                },
            )
            .map_err(|error| phase_error(super::super::vod_api_phase_scope::Error::Budget(error)))
    }
}
impl FairRuntime {
    pub(super) fn new(limits: ReadLimits) -> Result<Arc<Self>, ReadError> {
        // Failure to fit the refresh lane does not invalidate ordinary limits.
        let refresh = queue_capacity(
            limits.memory_bytes,
            limits.work_bytes,
            limits.response_bytes,
            limits.input_bytes,
        )
        .ok();
        let queued = refresh.unwrap_or(limits.memory_bytes.min(16 * 1024 * 1024));
        let upstream = usize::from(refresh.is_some()) * 2;
        let budget = Budget::new(
            Need {
                bytes: limits.memory_bytes,
                queued_bytes: queued,
                workers: limits.workers,
                http: limits.workers,
                upstream,
                continuation: usize::from(upstream > 0),
            },
            256,
        )
        .map_err(|_| ReadError::Capacity)?;
        Ok(Arc::new(Self {
            budget,
            inputs: Arc::new(Semaphore::new(256)),
            jobs: AtomicUsize::new(0),
            changed: tokio::sync::Notify::new(),
        }))
    }
    pub(super) fn request(
        self: &Arc<Self>,
        limits: ReadLimits,
        input: usize,
        http: bool,
    ) -> Result<Arc<RequestScope>, ReadError> {
        if input > limits.input_bytes {
            return Err(ReadError::Capacity);
        }
        let count = self
            .inputs
            .clone()
            .try_acquire_owned()
            .map_err(|_| ReadError::Capacity)?;
        let quote = limits.input_bytes;
        let shape = Shape {
            input: 0,
            work: limits.work_bytes,
            response: limits.response_bytes,
            http,
        };
        let (retained, phase) = match self.budget.try_claim(Need {
            bytes: quote,
            queued_bytes: quote,
            ..Need::default()
        }) {
            Ok(input) => {
                let phase = Scope::input(self.budget.clone(), shape).map_err(phase_error)?;
                (input, phase)
            }
            Err(super::super::vod_api_fair_budget::Error::Capacity) => {
                // A full cheap queue cannot consume the reserved ordinary lane.
                // Try the complete active grant atomically, without partial
                // worker/HTTP reservation or another snapshot allowance.
                let bytes = quote
                    .checked_add(limits.work_bytes)
                    .and_then(|n| n.checked_add(limits.response_bytes))
                    .ok_or(ReadError::Capacity)?;
                let mut whole = self
                    .budget
                    .try_claim(Need {
                        bytes,
                        workers: 1,
                        http: usize::from(http),
                        ..Need::default()
                    })
                    .map_err(|error| {
                        phase_error(super::super::vod_api_phase_scope::Error::Budget(error))
                    })?;
                let input = whole
                    .split(Need {
                        bytes: quote,
                        ..Need::default()
                    })
                    .map_err(|_| ReadError::Capacity)?;
                let phase =
                    Scope::from_active(self.budget.clone(), shape, whole).map_err(phase_error)?;
                (input, phase)
            }
            Err(error) => {
                return Err(phase_error(
                    super::super::vod_api_phase_scope::Error::Budget(error),
                ))
            }
        };
        Ok(Arc::new(RequestScope {
            phase,
            runtime: self.clone(),
            _input: Mutex::new(retained),
            _count: count,
            error_frame: Mutex::new(None),
            input_bytes: quote,
            claimed: AtomicBool::new(false),
        }))
    }

    fn request_active(
        self: &Arc<Self>,
        limits: ReadLimits,
        input: usize,
        http: bool,
    ) -> Result<Arc<RequestScope>, ReadError> {
        if input > limits.input_bytes {
            return Err(ReadError::Capacity);
        }
        let count = self
            .inputs
            .clone()
            .try_acquire_owned()
            .map_err(|_| ReadError::Capacity)?;
        let quote = limits.input_bytes;
        let bytes = quote
            .checked_add(limits.work_bytes)
            .and_then(|n| n.checked_add(limits.response_bytes))
            .ok_or(ReadError::Capacity)?;
        let mut grant = self
            .budget
            .try_claim(Need {
                bytes,
                workers: 1,
                http: usize::from(http),
                ..Need::default()
            })
            .map_err(|error| {
                phase_error(super::super::vod_api_phase_scope::Error::Budget(error))
            })?;
        let input = grant
            .split(Need {
                bytes: quote,
                ..Need::default()
            })
            .map_err(|_| ReadError::Capacity)?;
        let phase = Scope::from_active(
            self.budget.clone(),
            Shape {
                input: 0,
                work: limits.work_bytes,
                response: limits.response_bytes,
                http,
            },
            grant,
        )
        .map_err(phase_error)?;
        Ok(Arc::new(RequestScope {
            phase,
            runtime: self.clone(),
            _input: Mutex::new(input),
            _count: count,
            error_frame: Mutex::new(None),
            input_bytes: quote,
            claimed: AtomicBool::new(false),
        }))
    }
    pub(super) async fn admit_http(
        self: &Arc<Self>,
        limits: ReadLimits,
        input: usize,
        deadline: tokio::time::Instant,
    ) -> Result<HttpScope, ReadError> {
        if tokio::time::Instant::now() >= deadline {
            return Err(ReadError::Deadline);
        }
        let request = self.request(limits, input, true)?;
        if !request.phase.is_active() {
            if let Err(error) = request.phase.activate(deadline).await {
                request.phase.abort_after_actor_end().map_err(phase_error)?;
                return Err(phase_error(error));
            }
        }
        request.mark_input(false)?;
        Ok(HttpScope::Fair(request))
    }
    pub(super) async fn run<F, Fut>(
        self: Arc<Self>,
        runtime: ReadRuntime,
        input: usize,
        arrival: Option<tokio::time::Instant>,
        http: Option<HttpScope>,
        work: F,
    ) -> Result<Bytes, ReadError>
    where
        F: FnOnce(ReadWork) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Vec<u8>, ReadError>> + Send + 'static,
    {
        let limits = runtime.inner.limits;
        let local = tokio::time::Instant::now() + limits.logical_deadline;
        let original = arrival.map_or(local, |deadline| deadline.min(local));
        let request = match http {
            Some(HttpScope::Fair(request)) => {
                if !Arc::ptr_eq(&self, &request.runtime) {
                    return Err(ReadError::Unsupported(
                        "HTTP scope belongs to another read runtime or input",
                    ));
                }
                request
            }
            Some(HttpScope::Legacy(_)) => {
                return Err(ReadError::Unsupported(
                    "Legacy HTTP scope cannot enter the atomic runtime",
                ))
            }
            // Preserve immediate native component admission. Bounded waiting
            // belongs to the public HTTP scope and eligible refresh queue.
            None => self.request_active(limits, input, false)?,
        };
        if request
            .claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(ReadError::Unsupported(
                "HTTP scope was already consumed by a read actor",
            ));
        }
        if input > request.input_bytes {
            let _ = request.phase.abort_after_actor_end();
            return Err(ReadError::Capacity);
        }
        if tokio::time::Instant::now() >= original {
            let _ = request.phase.abort_after_actor_end();
            return Err(ReadError::Deadline);
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancelled_changed = Arc::new(tokio::sync::Notify::new());
        let _caller = CancelOnDrop(cancelled.clone(), cancelled_changed.clone());
        let (deadline, mut deadlines) = tokio::sync::watch::channel(original);
        let request_started = original
            .checked_sub(limits.logical_deadline)
            .ok_or(ReadError::Deadline)?;
        let lease = Arc::new(WorkLease {
            _worker: None,
            _memory: None,
            _request: Some(request.clone()),
        });
        let context = ReadWork {
            lease,
            inner: runtime.inner.clone(),
            cancelled,
            deadline,
            request_started,
            phase: Some(request.phase.clone()),
            cancelled_changed,
        };
        let (sender, mut receiver) = oneshot::channel();
        self.jobs.fetch_add(1, Ordering::AcqRel);
        let own = self.clone();
        tokio::spawn(async move {
            let outcome = AssertUnwindSafe(async { work(context).await })
                .catch_unwind()
                .await;
            let outcome = match outcome {
                Ok(result) => result,
                Err(_) => Err(ReadError::WorkerFailed),
            };
            let outcome = match outcome {
                Ok(data) if data.capacity() <= limits.response_bytes => {
                    match request.phase.finish(Some(data.capacity())) {
                        Ok(Some(frame)) => Ok(Bytes::from_owner(FairDocument {
                            data,
                            _frame: frame,
                            _request: request.clone(),
                        })),
                        _ => {
                            let _ = request.phase.abort_after_actor_end();
                            Err(ReadError::WorkerFailed)
                        }
                    }
                }
                Ok(_) => {
                    let _ = request.phase.abort_after_actor_end();
                    Err(ReadError::Capacity)
                }
                Err(error) => {
                    // A routed error is serialized by the HTTP adapter under
                    // this response reservation. The input owner survives a
                    // public deadline's small control response independently.
                    match request.phase.finish(Some(limits.response_bytes)) {
                        Ok(frame) => *request.error_frame.lock().expect("HTTP error frame") = frame,
                        Err(_) => {
                            let _ = request.phase.abort_after_actor_end();
                        }
                    }
                    Err(error)
                }
            };
            let _ = sender.send(outcome);
            request.phase.drained().await;
            own.jobs.fetch_sub(1, Ordering::AcqRel);
            own.changed.notify_waiters();
        });
        let mut watching = true;
        loop {
            let end = *deadlines.borrow();
            tokio::select! {biased;
                result=&mut receiver=>return result.map_err(|_|ReadError::WorkerFailed)?,
                changed=deadlines.changed(),if watching=>{if changed.is_err(){watching=false;}},
                _=tokio::time::sleep_until(end)=>{if tokio::time::Instant::now()>=*deadlines.borrow(){return Err(ReadError::Deadline)}}
            }
        }
    }
    #[cfg(test)]
    pub(super) fn scheduler_state(&self) -> serde_json::Value {
        let used = self.budget.used();
        let pending = self.budget.test_pending_counts();
        serde_json::json!({"atomic":true,"jobs":self.jobs.load(Ordering::Acquire),"inputs_used":256-self.inputs.available_permits(),"bytes":used.bytes,"queued_bytes":used.queued_bytes,"workers":used.workers,"http":used.http,"upstream":used.upstream,"continuation":used.continuation,"pending_new_refresh":pending[0],"pending_http":pending[1],"pending_continuation":pending[2],"admission_closed":self.budget.is_closing()})
    }
    pub(super) async fn shutdown(&self, budget: Duration) -> bool {
        self.inputs.close();
        self.budget.close_new();
        let end = tokio::time::Instant::now() + budget;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.jobs.load(Ordering::Acquire) == 0 && self.budget.used().workers == 0 {
                return true;
            }
            tokio::select! {_ = changed=>{},_ = tokio::time::sleep(Duration::from_millis(5))=>{},_ = tokio::time::sleep_until(end)=>return self.jobs.load(Ordering::Acquire)==0&&self.budget.used().workers==0}
        }
    }
}
struct FairDocument {
    data: Vec<u8>,
    _frame: Frame,
    _request: Arc<RequestScope>,
}
impl AsRef<[u8]> for FairDocument {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn settle(runtime: &ReadRuntime, workers: usize) {
        for _ in 0..100 {
            if runtime.physical_workers() == workers {
                return;
            }
            tokio::task::yield_now().await
        }
        panic!("physical workers did not settle")
    }
    fn response(work: &ReadWork) -> Result<Vec<u8>, ReadError> {
        let mut document = work.document();
        document.write_all(b"{}").map_err(|_| ReadError::Capacity)?;
        Ok(document.finish())
    }
    async fn paused(
        runtime: ReadRuntime,
    ) -> (
        tokio::task::JoinHandle<Result<Bytes, ReadError>>,
        Arc<tokio::sync::Notify>,
    ) {
        let (headers, ready) = oneshot::channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let wait = release.clone();
        let task = tokio::spawn(async move {
            runtime
                .run(10, move |work| async move {
                    let snapshot = work.freeze_snapshot(vec![0; 1024])?;
                    let snapshot = work.admit_refresh(snapshot).await?;
                    drop(work.consume_snapshot(snapshot)?);
                    let transport = work.transport_guard()?;
                    work.before_headers().await?;
                    work.start_auth_attempt()?;
                    let _ = headers.send(());
                    wait.notified().await;
                    work.resume_active().await?;
                    work.check()?;
                    drop(transport);
                    response(&work)
                })
                .await
        });
        ready.await.unwrap();
        (task, release)
    }
    #[tokio::test]
    async fn ordinary_result_keeps_only_paid_frame_and_input() {
        let runtime = ReadRuntime::new_fair(ReadLimits::default()).unwrap();
        let held_scope = runtime
            .acquire_http(10, tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        let frame = runtime
            .run_scoped(
                10,
                None,
                Some(held_scope),
                |work| async move { response(&work) },
            )
            .await
            .unwrap();
        settle(&runtime, 0).await;
        assert_eq!(frame.as_ref(), b"{}");
        assert_eq!(
            runtime.retained_memory_bytes(),
            ReadLimits::default().input_bytes + frame.len()
        );
        drop(frame);
        assert!(runtime.shutdown(Duration::from_secs(1)).await);
        assert_eq!(runtime.retained_memory_bytes(), 0);
    }
    #[tokio::test]
    async fn two_full_work_header_waits_allow_ordinary_result() {
        let runtime = ReadRuntime::new_fair(ReadLimits::default()).unwrap();
        let (a, release_a) = paused(runtime.clone()).await;
        let (b, release_b) = paused(runtime.clone()).await;
        assert_eq!(runtime.physical_workers(), 0);
        assert_eq!(
            runtime.retained_memory_bytes(),
            2 * (ReadLimits::default().work_bytes + ReadLimits::default().input_bytes)
        );
        let ordinary = runtime
            .run(10, |work| async move { response(&work) })
            .await
            .unwrap();
        assert_eq!(ordinary.as_ref(), b"{}");
        drop(ordinary);
        release_a.notify_one();
        release_b.notify_one();
        drop(a.await.unwrap().unwrap());
        drop(b.await.unwrap().unwrap());
        assert!(runtime.shutdown(Duration::from_secs(1)).await);
        assert_eq!(runtime.retained_memory_bytes(), 0);
    }
    #[tokio::test]
    async fn canceled_third_refresh_releases_cheap_snapshot_without_stopping_admitted_sessions() {
        let runtime = ReadRuntime::new_fair(ReadLimits::default()).unwrap();
        let (a, release_a) = paused(runtime.clone()).await;
        let (b, release_b) = paused(runtime.clone()).await;
        let (enter, entered) = oneshot::channel();
        let own = runtime.clone();
        let third = tokio::spawn(async move {
            own.run(10, move |work| async move {
                let snapshot = work.freeze_snapshot(vec![0; 1024])?;
                let _ = enter.send(());
                let _snapshot = work.admit_refresh(snapshot).await?;
                response(&work)
            })
            .await
        });
        entered.await.unwrap();
        for _ in 0..100 {
            if runtime.inner.fair.as_ref().unwrap().budget.queued() == 1 {
                break;
            }
            tokio::task::yield_now().await
        }
        assert_eq!(runtime.inner.fair.as_ref().unwrap().budget.queued(), 1);
        let observed = runtime.scheduler_state();
        assert_eq!(observed["pending_new_refresh"], 1);
        assert_eq!(observed["upstream"], 2);
        assert_eq!(observed["workers"], 0);
        assert!(observed["queued_bytes"].as_u64().unwrap() > 1024);
        third.abort();
        let _ = third.await;
        for _ in 0..100 {
            if runtime
                .inner
                .fair
                .as_ref()
                .unwrap()
                .jobs
                .load(Ordering::Acquire)
                == 2
            {
                break;
            }
            tokio::task::yield_now().await
        }
        assert_eq!(
            runtime
                .inner
                .fair
                .as_ref()
                .unwrap()
                .jobs
                .load(Ordering::Acquire),
            2
        );
        assert_eq!(
            runtime.retained_memory_bytes(),
            2 * (ReadLimits::default().work_bytes + ReadLimits::default().input_bytes)
        );
        release_a.notify_one();
        release_b.notify_one();
        drop(a.await.unwrap().unwrap());
        drop(b.await.unwrap().unwrap());
        assert!(runtime.shutdown(Duration::from_secs(1)).await);
        assert_eq!(runtime.retained_memory_bytes(), 0);
    }
    #[tokio::test]
    async fn late_physical_child_keeps_work_until_actual_release() {
        let runtime = ReadRuntime::new_fair(ReadLimits::default()).unwrap();
        let (send, recv) = oneshot::channel();
        let frame = runtime
            .run(10, move |work| async move {
                let child = work.io_guard()?.erased();
                let _ = send.send(child);
                response(&work)
            })
            .await
            .unwrap();
        let physical = recv.await.unwrap();
        assert_eq!(runtime.physical_workers(), 1);
        assert_eq!(
            runtime.retained_memory_bytes(),
            ReadLimits::default().input_bytes
                + ReadLimits::default().work_bytes
                + ReadLimits::default().response_bytes
        );
        drop(physical);
        settle(&runtime, 0).await;
        assert_eq!(
            runtime.retained_memory_bytes(),
            ReadLimits::default().input_bytes + frame.len()
        );
        drop(frame);
        assert!(runtime.shutdown(Duration::from_secs(1)).await);
    }
    #[tokio::test]
    async fn foreign_http_scope_is_rejected_before_work() {
        let a = ReadRuntime::new_fair(ReadLimits::default()).unwrap();
        let b = ReadRuntime::new_fair(ReadLimits::default()).unwrap();
        let scope = a
            .acquire_http(10, tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        let result = b
            .run_scoped(10, None, Some(scope), |_| async {
                panic!("foreign scope executed")
            })
            .await;
        assert!(matches!(result, Err(ReadError::Unsupported(_))));
        assert_eq!(a.retained_memory_bytes(), 0);
        assert_eq!(b.retained_memory_bytes(), 0);
    }
    #[tokio::test]
    async fn shutdown_waits_header_session_and_observer_loss_does_not_drop_resume() {
        let runtime = ReadRuntime::new_fair(ReadLimits::default()).unwrap();
        let (task, release) = paused(runtime.clone()).await;
        assert_eq!(runtime.physical_workers(), 0);
        assert!(!runtime.shutdown(Duration::from_millis(10)).await);
        task.abort();
        let _ = task.await;
        release.notify_one();
        assert!(runtime.shutdown(Duration::from_secs(1)).await);
        assert_eq!(runtime.retained_memory_bytes(), 0);
    }
    #[tokio::test]
    async fn full_cheap_metadata_does_not_consume_reserved_ordinary_input_lane() {
        let runtime = ReadRuntime::new_fair(ReadLimits::default()).unwrap();
        let (a, release_a) = paused(runtime.clone()).await;
        let (b, release_b) = paused(runtime.clone()).await;
        let frame = runtime
            .run(10, |work| async move {
                let mut d = work.document();
                d.write_all(&vec![0; 16 * 1024 * 1024])
                    .map_err(|_| ReadError::Capacity)?;
                Ok(d.finish())
            })
            .await
            .unwrap();
        settle(&runtime, 0).await;
        let budget = &runtime.inner.fair.as_ref().unwrap().budget;
        let q = budget.capacity().queued_bytes;
        let metadata = budget
            .try_claim(Need {
                bytes: q,
                queued_bytes: q,
                ..Need::default()
            })
            .unwrap();
        let ordinary_scope = runtime
            .acquire_http(10, tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        let ordinary = runtime
            .run_scoped(10, None, Some(ordinary_scope), |work| async move {
                response(&work)
            })
            .await
            .unwrap();
        assert_eq!(ordinary.as_ref(), b"{}");
        assert!(runtime.retained_memory_bytes() <= ReadLimits::default().memory_bytes);
        drop(ordinary);
        drop(metadata);
        drop(frame);
        release_a.notify_one();
        release_b.notify_one();
        drop(a.await.unwrap().unwrap());
        drop(b.await.unwrap().unwrap());
        assert!(runtime.shutdown(Duration::from_secs(1)).await);
        assert_eq!(runtime.retained_memory_bytes(), 0);
    }
    #[tokio::test]
    async fn frozen_snapshot_owner_outlives_actor_without_releasing_work() {
        let runtime = ReadRuntime::new_fair(ReadLimits::default()).unwrap();
        let (send, recv) = oneshot::channel();
        let frame = runtime
            .run(10, move |work| async move {
                let snapshot = work.freeze_snapshot(vec![0; 1024])?;
                let _ = send.send(snapshot);
                response(&work)
            })
            .await
            .unwrap();
        let snapshot = recv.await.unwrap();
        assert_eq!(runtime.physical_workers(), 1);
        assert_eq!(
            runtime.retained_memory_bytes(),
            ReadLimits::default().input_bytes
                + ReadLimits::default().work_bytes
                + ReadLimits::default().response_bytes
        );
        drop(snapshot);
        settle(&runtime, 0).await;
        drop(frame);
        assert!(runtime.shutdown(Duration::from_secs(1)).await);
        assert_eq!(runtime.retained_memory_bytes(), 0);
    }
    #[tokio::test]
    async fn formerly_valid_minimum_ordinary_configuration_remains_legacy_capable() {
        let limits = ReadLimits {
            workers: 1,
            memory_bytes: 68 * 1024 * 1024,
            ..ReadLimits::default()
        };
        let runtime = ReadRuntime::new_fair(limits).unwrap();
        assert!(!runtime.is_fair());
        let frame = runtime
            .run(1, |work| async move { response(&work) })
            .await
            .unwrap();
        assert_eq!(frame.as_ref(), b"{}");
        drop(frame);
        assert!(runtime.shutdown(Duration::from_secs(1)).await);
    }
    #[tokio::test]
    async fn cloned_http_scope_cannot_start_a_second_actor() {
        let runtime = ReadRuntime::new_fair(ReadLimits::default()).unwrap();
        let scope = runtime
            .acquire_http(10, tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        let (started, ready) = oneshot::channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let wait = release.clone();
        let own = runtime.clone();
        let first_scope = scope.clone();
        let first = tokio::spawn(async move {
            own.run_scoped(10, None, Some(first_scope), move |work| async move {
                let _ = started.send(());
                wait.notified().await;
                response(&work)
            })
            .await
        });
        ready.await.unwrap();
        let duplicate = runtime
            .run_scoped(10, None, Some(scope.clone()), |_| async {
                panic!("duplicate actor ran")
            })
            .await;
        assert!(matches!(duplicate, Err(ReadError::Unsupported(_))));
        assert_eq!(runtime.physical_workers(), 1);
        release.notify_one();
        drop(first.await.unwrap().unwrap());
        drop(scope);
        assert!(runtime.shutdown(Duration::from_secs(1)).await);
        assert_eq!(runtime.retained_memory_bytes(), 0);
    }
    #[tokio::test]
    async fn shutdown_rejects_unstarted_scope_without_pinning_work_to_error_body() {
        let runtime = ReadRuntime::new_fair(ReadLimits::default()).unwrap();
        let scope = runtime
            .acquire_http(10, tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        runtime.inner.closing.store(true, Ordering::Release);
        let result = runtime
            .run_scoped(10, None, Some(scope.clone()), |_| async {
                panic!("closed actor ran")
            })
            .await;
        assert!(matches!(result, Err(ReadError::Shutdown)));
        settle(&runtime, 0).await;
        assert_eq!(
            runtime.retained_memory_bytes(),
            ReadLimits::default().input_bytes
        );
        drop(scope);
        assert!(runtime.shutdown(Duration::from_secs(1)).await);
        assert_eq!(runtime.retained_memory_bytes(), 0);
    }

    #[tokio::test]
    async fn atomic_scope_cannot_enter_legacy_runtime() {
        let fair = ReadRuntime::new_fair(ReadLimits::default()).unwrap();
        let legacy = ReadRuntime::new(ReadLimits::default()).unwrap();
        let scope = fair
            .acquire_http(10, tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        let result = legacy
            .run_scoped(10, None, Some(scope), |_| async {
                panic!("legacy foreign actor ran")
            })
            .await;
        assert!(matches!(result, Err(ReadError::Unsupported(_))));
        assert_eq!(fair.retained_memory_bytes(), 0);
        assert_eq!(legacy.retained_memory_bytes(), 0);
    }
    #[tokio::test(start_paused = true)]
    async fn raw_component_capacity_matches_existing_legacy_contract() {
        async fn observe(runtime: ReadRuntime) -> &'static str {
            let mut tasks = Vec::new();
            let mut releases = Vec::new();
            for _ in 0..2 {
                let (started, ready) = oneshot::channel();
                let (release, wait) = oneshot::channel();
                let own = runtime.clone();
                tasks.push(tokio::spawn(async move {
                    own.run(1, move |work| async move {
                        let _ = started.send(());
                        let _ = wait.await;
                        response(&work)
                    })
                    .await
                }));
                releases.push(release);
                ready.await.unwrap();
            }
            assert_eq!(runtime.physical_workers(), 2);
            let observed = match tokio::time::timeout(
                Duration::from_millis(50),
                runtime.run(1, |work| async move { response(&work) }),
            )
            .await
            {
                Ok(Err(ReadError::Capacity)) => "Capacity",
                Ok(Err(ReadError::Deadline)) => "Deadline",
                Err(_) => "queued beyond50ms",
                _ => "other",
            };
            for release in releases {
                let _ = release.send(());
            }
            for task in tasks {
                drop(task.await.unwrap().unwrap());
            }
            assert!(runtime.shutdown(Duration::from_secs(1)).await);
            assert_eq!(runtime.retained_memory_bytes(), 0);
            observed
        }
        let legacy = observe(ReadRuntime::new(ReadLimits::default()).unwrap()).await;
        let fair = observe(ReadRuntime::new_fair(ReadLimits::default()).unwrap()).await;
        if let Ok(path) = std::env::var("FAIR_COMPAT_EVIDENCE") {
            std::fs::write(path,serde_json::to_vec_pretty(&serde_json::json!({"scope":"native internal component admission compatibility, not Original HTTP or physical SQL","legacy":legacy,"fair":fair,"cleanup":true})).unwrap()).unwrap();
        }
        assert_eq!(legacy, "Capacity");
        assert_eq!(
            fair, legacy,
            "raw component admission changed; public HTTP queue is separate"
        );
    }
    #[tokio::test]
    async fn snapshot_from_another_request_cannot_transfer_or_release_its_owner() {
        let runtime = ReadRuntime::new_fair(ReadLimits::default()).unwrap();
        let (send, recv) = oneshot::channel();
        let first = runtime
            .run(10, move |work| async move {
                let snapshot = work.freeze_snapshot(vec![0; 1024])?;
                let _ = send.send(snapshot);
                response(&work)
            })
            .await
            .unwrap();
        let snapshot = recv.await.unwrap();
        assert_eq!(runtime.physical_workers(), 1);
        let result = runtime
            .run(10, move |work| async move {
                let _bytes = work.consume_snapshot(snapshot)?;
                response(&work)
            })
            .await;
        assert!(matches!(result, Err(ReadError::Unsupported(_))));
        settle(&runtime, 0).await;
        drop(first);
        assert!(runtime.shutdown(Duration::from_secs(1)).await);
        assert_eq!(runtime.retained_memory_bytes(), 0);
    }
}

#[cfg(test)]
#[path = "vod_api_fair_physical_tests.rs"]
mod physical_tests;
