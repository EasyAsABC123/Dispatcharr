//! Code-only phase controller draft. UNCOMPILED and not connected to routes.
//! Original Dispatcharr authors retain all application/algorithm credit.
use super::{vod_api_fair_budget as budget, vod_api_phase_children as children};
use budget::{Admission, Budget, Grant, Need};
use children::{Epoch, Kind, PhysicalChild};
use std::sync::{Arc, Mutex};
#[derive(Clone, Copy)]
pub(super) struct Shape {
    pub input: usize,
    pub work: usize,
    pub response: usize,
    pub http: bool,
}
#[derive(Debug)]
pub(super) enum Error {
    Budget(budget::Error),
    Child(children::Error),
    Phase,
    Output,
    Capacity { needed: usize, cap: usize },
}
impl From<budget::Error> for Error {
    fn from(e: budget::Error) -> Self {
        Self::Budget(e)
    }
}
impl From<children::Error> for Error {
    fn from(e: children::Error) -> Self {
        Self::Child(e)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Input,
    Activating,
    Active,
    Sealing,
    Queued,
    Admitting,
    RefreshActive,
    Header,
    Resuming,
    Finishing,
    Frame,
    Done,
}
struct State {
    phase: Phase,
    epoch: Option<Arc<Epoch>>,
    output: bool,
    frame: bool,
    frame_bytes: usize,
    auth_attempt_started: bool,
}
struct Claim {
    budget: Budget,
    grant: Mutex<Grant>,
}
struct Inner {
    shape: Shape,
    claim: Arc<Claim>,
    state: Mutex<State>,
    network: Arc<Epoch>,
    snapshots: Arc<Epoch>,
    settled: tokio::sync::Notify,
}
#[derive(Clone)]
pub(super) struct Scope {
    inner: Arc<Inner>,
}
pub(super) struct Frame {
    scope: Scope,
}
impl Scope {
    pub(super) fn input(budget: Budget, shape: Shape) -> Result<Self, Error> {
        let need = Need {
            bytes: shape.input,
            queued_bytes: shape.input,
            ..Need::default()
        };
        let grant = budget.try_claim(need)?;
        Ok(Self {
            inner: Arc::new(Inner {
                shape,
                claim: Arc::new(Claim {
                    budget,
                    grant: Mutex::new(grant),
                }),
                state: Mutex::new(State {
                    phase: Phase::Input,
                    epoch: None,
                    output: false,
                    frame: false,
                    frame_bytes: 0,
                    auth_attempt_started: false,
                }),
                network: Epoch::new(1)?,
                snapshots: Epoch::new(1)?,
                settled: tokio::sync::Notify::new(),
            }),
        })
    }
    pub(super) fn from_active(budget: Budget, shape: Shape, grant: Grant) -> Result<Self, Error> {
        let expected = Need {
            bytes: shape
                .input
                .checked_add(shape.work)
                .and_then(|n| n.checked_add(shape.response))
                .ok_or(Error::Phase)?,
            workers: 1,
            http: usize::from(shape.http),
            ..Need::default()
        };
        if !budget.same(&grant) || grant.need() != expected {
            return Err(Error::Phase);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                shape,
                claim: Arc::new(Claim {
                    budget,
                    grant: Mutex::new(grant),
                }),
                state: Mutex::new(State {
                    phase: Phase::Active,
                    epoch: Some(Epoch::new(64)?),
                    output: false,
                    frame: false,
                    frame_bytes: 0,
                    auth_attempt_started: false,
                }),
                network: Epoch::new(1)?,
                snapshots: Epoch::new(1)?,
                settled: tokio::sync::Notify::new(),
            }),
        })
    }
    pub(super) fn is_active(&self) -> bool {
        self.phase() == Phase::Active
    }
    fn active_need(&self, refresh: bool) -> Result<Need, Error> {
        let s = self.inner.shape;
        Ok(Need {
            bytes: s
                .input
                .checked_add(s.work)
                .and_then(|n| n.checked_add(s.response))
                .ok_or(Error::Phase)?,
            workers: 1,
            http: usize::from(s.http),
            upstream: usize::from(refresh),
            continuation: usize::from(refresh),
            ..Need::default()
        })
    }
    fn phase(&self) -> Phase {
        self.inner.state.lock().expect("scope phase").phase
    }
    fn begin(&self, from: Phase, to: Phase) -> Result<(), Error> {
        let mut s = self.inner.state.lock().expect("scope phase");
        if s.phase != from {
            return Err(Error::Phase);
        }
        s.phase = to;
        Ok(())
    }
    async fn extend(
        &self,
        expected: Phase,
        goal: Need,
        admission: Admission,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<(), Error> {
        let current = self.inner.claim.grant.lock().expect("scope claim").need();
        let extra = Need {
            bytes: goal.bytes.checked_sub(current.bytes).ok_or(Error::Phase)?,
            workers: goal
                .workers
                .checked_sub(current.workers)
                .ok_or(Error::Phase)?,
            http: goal.http.checked_sub(current.http).ok_or(Error::Phase)?,
            upstream: goal
                .upstream
                .checked_sub(current.upstream)
                .ok_or(Error::Phase)?,
            continuation: goal
                .continuation
                .checked_sub(current.continuation)
                .ok_or(Error::Phase)?,
            queued_bytes: 0,
        };
        let mut grant = self
            .inner
            .claim
            .budget
            .acquire(extra, admission, deadline)
            .await?;
        let state = self.inner.state.lock().expect("scope phase");
        if state.phase != expected {
            return Err(Error::Phase);
        }
        let mut owned = self.inner.claim.grant.lock().expect("scope claim");
        owned.combine(&mut grant)?;
        self.inner.claim.budget.replace(&mut owned, goal)?;
        Ok(())
    }
    pub(super) async fn activate(&self, deadline: tokio::time::Instant) -> Result<(), Error> {
        self.begin(Phase::Input, Phase::Activating)?;
        self.extend(
            Phase::Activating,
            self.active_need(false)?,
            Admission::New,
            Some(deadline),
        )
        .await?;
        if self.inner.claim.budget.is_closing() {
            return Err(Error::Budget(budget::Error::Closed));
        }
        let mut s = self.inner.state.lock().expect("scope phase");
        if s.phase != Phase::Activating {
            return Err(Error::Phase);
        }
        s.epoch = Some(Epoch::new(64)?);
        s.phase = Phase::Active;
        Ok(())
    }
    pub(super) fn refresh_enabled(&self) -> bool {
        self.inner.claim.budget.capacity().upstream > 0
    }
    pub(super) fn require_refresh_active(&self) -> Result<(), Error> {
        if self.phase() == Phase::RefreshActive {
            Ok(())
        } else {
            Err(Error::Phase)
        }
    }
    pub(super) fn io_guard(&self, kind: Kind) -> Result<PhysicalChild, Error> {
        let s = self.inner.state.lock().expect("scope phase");
        if !matches!(s.phase, Phase::Active | Phase::RefreshActive) {
            return Err(Error::Phase);
        }
        Ok(s.epoch
            .as_ref()
            .ok_or(Error::Phase)?
            .register(kind, self.inner.clone())?)
    }
    /// Sticky output veto: a finished Vec cannot silently escape a Document guard.
    /// Refresh selection precedes response materialization; snapshot writers do not
    /// call this and remain within the work reservation.
    pub(super) fn mark_output(&self) -> Result<(), Error> {
        let mut s = self.inner.state.lock().expect("scope phase");
        if !matches!(s.phase, Phase::Active | Phase::RefreshActive) {
            return Err(Error::Phase);
        }
        s.output = true;
        Ok(())
    }
    async fn seal_active(
        &self,
        expected: Phase,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<(), Error> {
        let seal = {
            let mut s = self.inner.state.lock().expect("scope phase");
            if s.phase != expected {
                return Err(Error::Phase);
            }
            if s.output {
                return Err(Error::Output);
            }
            let seal = s.epoch.as_ref().ok_or(Error::Phase)?.seal();
            s.phase = Phase::Sealing;
            seal
        };
        let quiet = seal.wait(deadline).await?;
        let s = self.inner.state.lock().expect("scope phase");
        if s.phase != Phase::Sealing {
            return Err(Error::Phase);
        }
        quiet.require(s.epoch.as_ref().ok_or(Error::Phase)?)?;
        Ok(())
    }
    pub(super) async fn queue_refresh(
        &self,
        snapshot_capacity: usize,
        deadline: tokio::time::Instant,
    ) -> Result<(), Error> {
        self.seal_active(Phase::Active, Some(deadline)).await?;
        let bytes = self
            .inner
            .shape
            .input
            .checked_add(snapshot_capacity)
            .ok_or(Error::Phase)?;
        let cap = self.inner.claim.budget.capacity().queued_bytes;
        let queued = self.inner.claim.budget.used().queued_bytes;
        let required = queued.checked_add(bytes).ok_or(Error::Phase)?;
        if bytes > cap {
            return Err(Error::Capacity {
                needed: required,
                cap,
            });
        }
        let need = Need {
            bytes,
            queued_bytes: bytes,
            ..Need::default()
        };
        let mut s = self.inner.state.lock().expect("scope phase");
        if s.phase != Phase::Sealing {
            return Err(Error::Phase);
        }
        let mut owned = self.inner.claim.grant.lock().expect("scope claim");
        self.inner
            .claim
            .budget
            .replace(&mut owned, need)
            .map_err(|e| {
                if e == budget::Error::Capacity {
                    Error::Capacity {
                        needed: required,
                        cap,
                    }
                } else {
                    Error::Budget(e)
                }
            })?;
        s.epoch = None;
        s.phase = Phase::Queued;
        Ok(())
    }
    pub(super) async fn admit_refresh(&self, deadline: tokio::time::Instant) -> Result<(), Error> {
        self.begin(Phase::Queued, Phase::Admitting)?;
        self.extend(
            Phase::Admitting,
            self.active_need(true)?,
            Admission::New,
            Some(deadline),
        )
        .await?;
        if self.inner.claim.budget.is_closing() {
            return Err(Error::Budget(budget::Error::Closed));
        }
        let mut s = self.inner.state.lock().expect("scope phase");
        if s.phase != Phase::Admitting {
            return Err(Error::Phase);
        }
        s.epoch = Some(Epoch::new(64)?);
        s.phase = Phase::RefreshActive;
        Ok(())
    }
    /// Session life pins the request/work/upstream claim but is not an active-
    /// phase child. Only a Source-admitted refresh can obtain this token.
    pub(super) fn snapshot_guard(&self) -> Result<PhysicalChild, Error> {
        let s = self.inner.state.lock().expect("scope phase");
        if s.phase != Phase::Active {
            return Err(Error::Phase);
        }
        Ok(self
            .inner
            .snapshots
            .register(Kind::Other, self.inner.clone())?)
    }
    pub(super) fn transport_guard(&self) -> Result<PhysicalChild, Error> {
        let s = self.inner.state.lock().expect("scope phase");
        if !matches!(
            s.phase,
            Phase::RefreshActive | Phase::Header | Phase::Resuming
        ) {
            return Err(Error::Phase);
        }
        Ok(self
            .inner
            .network
            .register(Kind::Other, self.inner.clone())?)
    }
    /// Call immediately before polling the first auth send future, after
    /// before_headers completed. This proves an attempted supervised request,
    /// never that reqwest has written bytes to the socket.
    pub(super) fn start_auth_attempt(&self) -> Result<(), Error> {
        let mut s = self.inner.state.lock().expect("scope phase");
        if s.phase != Phase::Header || self.inner.network.counts() != [0, 0, 0, 1] {
            return Err(Error::Phase);
        }
        s.auth_attempt_started = true;
        Ok(())
    }
    pub(super) fn observer_independent_session(&self) -> bool {
        self.inner
            .state
            .lock()
            .expect("scope phase")
            .auth_attempt_started
    }
    pub(super) async fn before_headers(&self) -> Result<(), Error> {
        self.seal_active(Phase::RefreshActive, None).await?;
        let mut s = self.inner.state.lock().expect("scope phase");
        if s.phase != Phase::Sealing {
            return Err(Error::Phase);
        }
        let need = Need {
            bytes: self.inner.shape.input + self.inner.shape.work,
            upstream: 1,
            ..Need::default()
        };
        let mut owned = self.inner.claim.grant.lock().expect("scope claim");
        self.inner.claim.budget.replace(&mut owned, need)?;
        s.epoch = None;
        s.phase = Phase::Header;
        Ok(())
    }
    /// Once the admitted auth→info session is sent, losing the public observer
    /// does not cancel continuation or pretend the operation physically drained.
    pub(super) async fn resume_body(&self) -> Result<(), Error> {
        self.begin(Phase::Header, Phase::Resuming)?;
        self.extend(
            Phase::Resuming,
            self.active_need(true)?,
            Admission::ExistingContinuation,
            None,
        )
        .await?;
        let mut s = self.inner.state.lock().expect("scope phase");
        if s.phase != Phase::Resuming {
            return Err(Error::Phase);
        }
        s.epoch = Some(Epoch::new(64)?);
        s.phase = Phase::RefreshActive;
        Ok(())
    }
    /// Logical actor completion may precede an auth/cache physical child. The
    /// background finalizer retains this scope and its claim until both groups
    /// settle. Frame memory/HTTP lifetime is then reduced independently.
    pub(super) fn finish(&self, frame_capacity: Option<usize>) -> Result<Option<Frame>, Error> {
        let (active, network, snapshot) = {
            let mut s = self.inner.state.lock().expect("scope phase");
            if !matches!(s.phase, Phase::Active | Phase::RefreshActive) {
                return Err(Error::Phase);
            }
            if frame_capacity.is_some_and(|n| n > self.inner.shape.response) {
                return Err(Error::Capacity {
                    needed: frame_capacity.unwrap(),
                    cap: self.inner.shape.response,
                });
            }
            s.frame = frame_capacity.is_some();
            s.frame_bytes = frame_capacity.unwrap_or(0);
            s.phase = Phase::Finishing;
            (
                s.epoch.as_ref().ok_or(Error::Phase)?.seal(),
                self.inner.network.seal(),
                self.inner.snapshots.seal(),
            )
        };
        let own = self.clone();
        tokio::spawn(async move {
            let _active = active.wait(None).await;
            let _network = network.wait(None).await;
            let _snapshot = snapshot.wait(None).await;
            let _ = own.finalize();
        });
        Ok(frame_capacity.map(|_| Frame {
            scope: self.clone(),
        }))
    }
    fn finalize(&self) -> Result<(), Error> {
        let mut s = self.inner.state.lock().expect("scope phase");
        if s.phase != Phase::Finishing {
            return Err(Error::Phase);
        }
        let need = if s.frame {
            Need {
                bytes: s.frame_bytes + self.inner.shape.input,
                http: usize::from(self.inner.shape.http),
                ..Need::default()
            }
        } else {
            Need::default()
        };
        let mut owned = self.inner.claim.grant.lock().expect("scope claim");
        self.inner.claim.budget.replace(&mut owned, need)?;
        s.epoch = None;
        s.phase = if s.frame { Phase::Frame } else { Phase::Done };
        drop(s);
        self.inner.settled.notify_waiters();
        Ok(())
    }
    /// Error/unwind cleanup retains the entire existing claim through both
    /// registered groups. A public deadline must not call this while it would
    /// cancel a still-owned admitted transport future; the actor owns that future.
    pub(super) fn abort_after_actor_end(&self) -> Result<(), Error> {
        let (active, network, snapshot) = {
            let mut s = self.inner.state.lock().expect("scope phase");
            if matches!(s.phase, Phase::Frame | Phase::Done) {
                return Ok(());
            }
            if s.phase == Phase::Finishing {
                return Err(Error::Phase);
            }
            s.frame = false;
            s.frame_bytes = 0;
            s.phase = Phase::Finishing;
            (
                s.epoch.as_ref().map(|epoch| epoch.seal()),
                self.inner.network.seal(),
                self.inner.snapshots.seal(),
            )
        };
        let own = self.clone();
        tokio::spawn(async move {
            if let Some(active) = active {
                let _ = active.wait(None).await;
            }
            let _ = network.wait(None).await;
            let _ = snapshot.wait(None).await;
            let _ = own.finalize();
        });
        Ok(())
    }
    pub(super) async fn drained(&self) -> bool {
        loop {
            let changed = self.inner.settled.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if matches!(self.phase(), Phase::Frame | Phase::Done) {
                return true;
            }
            changed.await
        }
    }
}
impl Drop for Frame {
    fn drop(&mut self) {
        let mut state = self.scope.inner.state.lock().expect("scope phase");
        state.frame = false;
        if state.phase == Phase::Frame {
            let mut owned = self.scope.inner.claim.grant.lock().expect("scope claim");
            let _ = self
                .scope
                .inner
                .claim
                .budget
                .replace(&mut owned, Need::default());
            state.phase = Phase::Done;
            drop(state);
            self.scope.inner.settled.notify_waiters();
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    fn budget() -> Budget {
        Budget::new(
            Need {
                bytes: 192 * 1024 * 1024,
                queued_bytes: 4 * 1024 * 1024 - 4 * 64 * 1024,
                workers: 2,
                http: 2,
                upstream: 2,
                continuation: 1,
            },
            8,
        )
        .unwrap()
    }
    fn shape() -> Shape {
        Shape {
            input: 64 * 1024,
            work: 52 * 1024 * 1024,
            response: 16 * 1024 * 1024,
            http: true,
        }
    }
    fn end() -> tokio::time::Instant {
        tokio::time::Instant::now() + Duration::from_secs(600)
    }
    async fn refresh(b: &Budget) -> Scope {
        let s = Scope::input(b.clone(), shape()).unwrap();
        s.activate(end()).await.unwrap();
        s.queue_refresh(1024, end()).await.unwrap();
        s.admit_refresh(end()).await.unwrap();
        s
    }
    #[tokio::test]
    async fn headers_keep_full_work_and_lend_active_dimensions() {
        let b = budget();
        let a = refresh(&b).await;
        let network = a.transport_guard().unwrap();
        a.before_headers().await.unwrap();
        let n = b.used();
        assert_eq!(n.bytes, 52 * 1024 * 1024 + 64 * 1024);
        assert_eq!(
            (n.workers, n.http, n.upstream, n.continuation),
            (0, 0, 1, 0)
        );
        a.resume_body().await.unwrap();
        let n = b.used();
        assert_eq!(
            (n.workers, n.http, n.upstream, n.continuation),
            (1, 1, 1, 1)
        );
        drop(network);
        a.abort_after_actor_end().unwrap();
        assert!(a.drained().await);
        assert_eq!(b.used(), Need::default());
    }
    #[tokio::test]
    async fn physical_auth_subtask_prevents_premature_lending() {
        let b = budget();
        let a = refresh(&b).await;
        let token = a.io_guard(Kind::Auth).unwrap();
        let late = token.erased();
        drop(token);
        let own = a.clone();
        let waiting = tokio::spawn(async move { own.before_headers().await });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        assert_eq!(b.used().workers, 1);
        assert_eq!(b.used().bytes, 68 * 1024 * 1024 + 64 * 1024);
        drop(late);
        waiting.await.unwrap().unwrap();
        assert_eq!(b.used().workers, 0);
        a.abort_after_actor_end().unwrap();
        a.drained().await;
    }
    #[tokio::test]
    async fn response_materialization_vetoes_header_loan() {
        let b = budget();
        let a = refresh(&b).await;
        a.mark_output().unwrap();
        assert!(matches!(a.before_headers().await, Err(Error::Output)));
        assert_eq!(b.used().workers, 1);
        a.abort_after_actor_end().unwrap();
        a.drained().await;
    }
    #[tokio::test]
    async fn third_waiter_is_cheap_and_large_descriptor_rejected_before_loan() {
        let b = budget();
        let a = refresh(&b).await;
        a.before_headers().await.unwrap();
        let c = refresh(&b).await;
        c.before_headers().await.unwrap();
        let third = Scope::input(b.clone(), shape()).unwrap();
        third.activate(end()).await.unwrap();
        let cap = b.capacity().queued_bytes;
        assert!(
            matches!(third.queue_refresh(cap+1,end()).await,Err(Error::Capacity{needed,cap:limit})if needed>limit)
        );
        assert_eq!(b.used().workers, 1);
        third.abort_after_actor_end().unwrap();
        third.drained().await;
        let small = Scope::input(b.clone(), shape()).unwrap();
        small.activate(end()).await.unwrap();
        small.queue_refresh(1024, end()).await.unwrap();
        let own = small.clone();
        let queued = tokio::spawn(async move { own.admit_refresh(end()).await });
        tokio::task::yield_now().await;
        assert!(!queued.is_finished());
        assert_eq!(b.used().workers, 0);
        assert!(b.used().bytes < 105 * 1024 * 1024);
        queued.abort();
        let _ = queued.await;
        small.abort_after_actor_end().unwrap();
        small.drained().await;
        a.abort_after_actor_end().unwrap();
        c.abort_after_actor_end().unwrap();
        a.drained().await;
        c.drained().await;
        assert_eq!(b.used(), Need::default());
    }
    #[tokio::test]
    async fn frame_holds_only_response_input_http_after_subtasks_settle() {
        let b = budget();
        let a = Scope::input(b.clone(), shape()).unwrap();
        a.activate(end()).await.unwrap();
        let child = a.io_guard(Kind::Sql).unwrap();
        let frame = a.finish(Some(16 * 1024 * 1024)).unwrap().unwrap();
        assert_eq!(b.used().workers, 1);
        drop(child);
        a.drained().await;
        let n = b.used();
        assert_eq!(n.bytes, 16 * 1024 * 1024 + 64 * 1024);
        assert_eq!((n.workers, n.http, n.upstream), (0, 1, 0));
        drop(frame);
        assert_eq!(b.used(), Need::default());
    }
    #[tokio::test]
    async fn shutdown_closes_new_but_resumes_existing_session_then_waits_it() {
        let b = budget();
        let a = refresh(&b).await;
        let network = a.transport_guard().unwrap();
        a.before_headers().await.unwrap();
        b.close_new();
        assert!(matches!(
            Scope::input(b.clone(), shape()),
            Err(Error::Budget(budget::Error::Closed))
        ));
        a.resume_body().await.unwrap();
        let frame = a.finish(Some(128)).unwrap().unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(10), a.drained())
            .await
            .is_err());
        drop(network);
        a.drained().await;
        drop(frame);
        assert_eq!(b.used(), Need::default());
    }
    #[tokio::test]
    async fn late_activation_grant_cannot_reanimate_ended_claim() {
        let b = budget();
        let a = Scope::input(b.clone(), shape()).unwrap();
        let held = b
            .try_claim(Need {
                bytes: b.capacity().bytes - shape().input,
                ..Need::default()
            })
            .unwrap();
        let own = a.clone();
        let task = tokio::spawn(async move { own.activate(end()).await });
        for _ in 0..100 {
            if b.queued() == 1 {
                break;
            }
            tokio::task::yield_now().await
        }
        assert_eq!(b.queued(), 1);
        a.abort_after_actor_end().unwrap();
        a.drained().await;
        assert_eq!(a.phase(), Phase::Done);
        drop(held);
        assert!(matches!(task.await.unwrap(), Err(Error::Phase)));
        assert_eq!(b.used(), Need::default());
    }
    #[tokio::test]
    async fn late_refresh_admission_cannot_reanimate_ended_claim() {
        let b = budget();
        let a = refresh(&b).await;
        a.before_headers().await.unwrap();
        let c = refresh(&b).await;
        c.before_headers().await.unwrap();
        let third = Scope::input(b.clone(), shape()).unwrap();
        third.activate(end()).await.unwrap();
        third.queue_refresh(1024, end()).await.unwrap();
        let own = third.clone();
        let task = tokio::spawn(async move { own.admit_refresh(end()).await });
        for _ in 0..100 {
            if b.queued() == 1 {
                break;
            }
            tokio::task::yield_now().await
        }
        assert_eq!(b.queued(), 1);
        third.abort_after_actor_end().unwrap();
        third.drained().await;
        a.abort_after_actor_end().unwrap();
        a.drained().await;
        assert!(matches!(task.await.unwrap(), Err(Error::Phase)));
        assert_eq!(third.phase(), Phase::Done);
        c.abort_after_actor_end().unwrap();
        c.drained().await;
        assert_eq!(b.used(), Need::default());
    }

    #[tokio::test]
    async fn late_body_resume_grant_cannot_reanimate_ended_claim() {
        let b = budget();
        let a = refresh(&b).await;
        a.before_headers().await.unwrap();
        let held = b
            .try_claim(Need {
                bytes: b.capacity().bytes - b.used().bytes,
                ..Need::default()
            })
            .unwrap();
        let own = a.clone();
        let task = tokio::spawn(async move { own.resume_body().await });
        for _ in 0..100 {
            if b.queued() == 1 {
                break;
            }
            tokio::task::yield_now().await
        }
        assert_eq!(b.queued(), 1);
        a.abort_after_actor_end().unwrap();
        a.drained().await;
        assert!(matches!(task.await.unwrap(), Err(Error::Phase)));
        assert_eq!(a.phase(), Phase::Done);
        drop(held);
        assert_eq!(b.used(), Need::default());
    }

    #[tokio::test]
    async fn auth_attempt_marker_requires_quiesced_header_phase_and_owned_session() {
        let b = budget();
        let a = refresh(&b).await;
        assert!(!a.observer_independent_session());
        assert!(matches!(a.start_auth_attempt(), Err(Error::Phase)));
        let session = a.transport_guard().unwrap();
        a.before_headers().await.unwrap();
        assert!(!a.observer_independent_session());
        a.start_auth_attempt().unwrap();
        assert!(a.observer_independent_session());
        a.resume_body().await.unwrap();
        assert!(a.observer_independent_session());
        drop(session);
        a.abort_after_actor_end().unwrap();
        a.drained().await;
    }
}
