//! Retain a task/file lease until physical child work and its release complete.
//! Original Dispatcharr application/algorithm credit belongs to its authors.
use dispatcharr_coordination::TaskLease;
use std::{
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;
struct Completion {
    status: AtomicU8,
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
            status: AtomicU8::new(0),
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
                let result = lease.release_owned().await;
                drop(caller);
                completion.status.store(
                    if matches!(result, Ok(true)) { 1 } else { 2 },
                    Ordering::Release,
                );
                completion.changed.notify_waiters();
            });
        } else {
            // A dead runtime cannot claim successful release. The coordination
            // primitive retains its normal crash-recovery TTL semantics.
            drop(lease);
            drop(caller);
            completion.status.store(2, Ordering::Release);
            completion.changed.notify_waiters();
        }
    }
}
impl Released {
    pub(crate) async fn wait(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.completion.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.completion.status.load(Ordering::Acquire) {
                1 => return true,
                2 => return false,
                _ => {}
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.completion.status.load(Ordering::Acquire) == 1;
            }
        }
    }
}
