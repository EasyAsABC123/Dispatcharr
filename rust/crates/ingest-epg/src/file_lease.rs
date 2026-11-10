//! Retain a task/file lease until physical child work and its release complete.
//! Original Dispatcharr application/algorithm credit belongs to its authors.
use dispatcharr_coordination::{TaskLease, TaskLockDetailedError};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Notify;
#[derive(Clone, Debug)]
pub(crate) enum ReleaseOutcome {
    Released,
    NotOwner,
    Failed(Arc<TaskLockDetailedError>),
    RuntimeUnavailable,
}
#[derive(Clone, Debug)]
pub(crate) enum ReleaseWait {
    Complete(ReleaseOutcome),
    TimedOut,
}
struct Completion {
    outcome: Mutex<Option<ReleaseOutcome>>,
    changed: Notify,
}
#[derive(Clone)]
pub(crate) struct Released {
    completion: Arc<Completion>,
}
pub(crate) struct RetainedLease {
    lease: Option<TaskLease>,
    caller: Option<Arc<dyn Send + Sync>>,
    completion: Arc<Completion>,
}
impl RetainedLease {
    pub(crate) fn new(lease: TaskLease, caller: Arc<dyn Send + Sync>) -> (Arc<Self>, Released) {
        let completion = Arc::new(Completion {
            outcome: Mutex::new(None),
            changed: Notify::new(),
        });
        (
            Arc::new(Self {
                lease: Some(lease),
                caller: Some(caller),
                completion: completion.clone(),
            }),
            Released { completion },
        )
    }
}
impl Drop for RetainedLease {
    fn drop(&mut self) {
        let lease = self.lease.take().unwrap();
        let caller = self.caller.take();
        let completion = self.completion.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let result = lease.release_owned_detailed().await;
                drop(caller);
                *completion.outcome.lock().unwrap() = Some(match result {
                    Ok(true) => ReleaseOutcome::Released,
                    Ok(false) => ReleaseOutcome::NotOwner,
                    Err(error) => ReleaseOutcome::Failed(Arc::new(error)),
                });
                completion.changed.notify_waiters();
            });
        } else {
            // A dead runtime cannot claim successful release. The coordination
            // primitive retains its normal crash-recovery TTL semantics.
            drop(lease);
            drop(caller);
            *completion.outcome.lock().unwrap() = Some(ReleaseOutcome::RuntimeUnavailable);
            completion.changed.notify_waiters();
        }
    }
}
impl Released {
    pub(crate) async fn wait(&self, timeout: Duration) -> bool {
        matches!(
            self.wait_detailed(timeout).await,
            ReleaseWait::Complete(ReleaseOutcome::Released)
        )
    }
    pub(crate) async fn wait_detailed(&self, timeout: Duration) -> ReleaseWait {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.completion.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(outcome) = self.completion.outcome.lock().unwrap().clone() {
                return ReleaseWait::Complete(outcome);
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self
                    .completion
                    .outcome
                    .lock()
                    .unwrap()
                    .clone()
                    .map(ReleaseWait::Complete)
                    .unwrap_or(ReleaseWait::TimedOut);
            }
        }
    }
}
