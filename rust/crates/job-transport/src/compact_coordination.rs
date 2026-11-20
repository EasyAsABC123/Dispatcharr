//! Cache-only ownership for compact channel numbering.
//! Original Dispatcharr authors retain application and algorithm credit.
use dispatcharr_contracts::BoxFuture;
use dispatcharr_coordination::{
    TaskLease, TaskLockConfig, TaskLockDetailedError, TaskLockError, TaskLocks,
};
use dispatcharr_store::compact_numbering::{Coordinator, Failure, Lease, LockResult, Owner};
use std::{
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{OnceCell, Semaphore};

const LEASES: u32 = 4;
const CLOSED: u8 = 1;
const USED: u8 = 2;
const ACCOUNT_REFRESH: &str = "refresh_single_m3u_account";

enum Cache {
    Shared(TaskLocks),
    Lazy {
        url: String,
        manager: OnceCell<TaskLocks>,
    },
}

impl Cache {
    async fn locks(&self) -> Result<&TaskLocks, Failure> {
        match self {
            Self::Shared(locks) => Ok(locks),
            Self::Lazy { url, manager } => manager
                .get_or_try_init(|| TaskLocks::connect(url, TaskLockConfig::default()))
                .await
                .map_err(|error| match error {
                    TaskLockError::Invalid => Failure::Unsupported,
                    // Manager construction has not sent an account lock SET.
                    _ => Failure::Settled,
                }),
        }
    }
}

struct Inner {
    cache: Cache,
    state: AtomicU8,
    slots: Arc<Semaphore>,
    uncertain: Mutex<Vec<Owner>>,
}

/// A bounded lock owner. It does not create a Celery client or start a worker.
pub struct CompactCoordinator {
    inner: Arc<Inner>,
}

impl CompactCoordinator {
    fn new(cache: Cache) -> Self {
        Self {
            inner: Arc::new(Inner {
                cache,
                state: AtomicU8::new(0),
                slots: Arc::new(Semaphore::new(LEASES as usize)),
                uncertain: Mutex::new(Vec::with_capacity(LEASES as usize)),
            }),
        }
    }

    pub fn shared(locks: TaskLocks) -> Self {
        Self::new(Cache::Shared(locks))
    }

    pub fn lazy(url: String) -> Self {
        Self::new(Cache::Lazy {
            url,
            manager: OnceCell::new(),
        })
    }

    pub fn was_used(&self) -> bool {
        self.inner.state.load(Ordering::Acquire) & USED != 0
    }

    pub fn close_admission(&self) {
        self.inner.state.fetch_or(CLOSED, Ordering::AcqRel);
    }

    fn admit(&self, caller: Owner) -> Result<Owner, Failure> {
        if self.inner.state.load(Ordering::Acquire) & CLOSED != 0 {
            return Err(Failure::Capacity);
        }
        let slot = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Failure::Capacity)?;
        // Register use and verify admission in one atomic operation.
        // Shutdown cannot observe an unused owner after an admitted request
        // has passed this gate, even if it has not sent the Redis SET yet.
        self.inner
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                (state & CLOSED == 0).then_some(state | USED)
            })
            .map_err(|_| Failure::Capacity)?;
        // TaskLocks retains this physical gate through SET and DEL, even
        // if the request or a release waiter is canceled.
        Ok(Arc::new((caller, slot)))
    }

    pub async fn shutdown(&self, duration: Duration) -> bool {
        self.close_admission();
        let slots = tokio::time::timeout(
            duration,
            self.inner.slots.clone().acquire_many_owned(LEASES),
        )
        .await;
        matches!(slots, Ok(Ok(_)))
            && self
                .inner
                .uncertain
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty()
    }
}

fn classify(inner: &Inner, owner: Owner, error: TaskLockDetailedError) -> Failure {
    match error {
        TaskLockDetailedError::Operational(TaskLockError::Capacity) => Failure::Capacity,
        TaskLockDetailedError::Operational(TaskLockError::Invalid) => Failure::Unsupported,
        TaskLockDetailedError::Redis(cause)
            if cause.kind == redis::ErrorKind::ResponseError
                || (cause.code.is_some() && cause.io_kind.is_none()) =>
        {
            Failure::Settled
        }
        _ => {
            let mut held = inner.uncertain.lock().unwrap_or_else(|e| e.into_inner());
            if !held.iter().any(|value| Arc::ptr_eq(value, &owner)) {
                held.push(owner);
            }
            Failure::Unsettled
        }
    }
}

struct CompactLease {
    lease: TaskLease,
    inner: Arc<Inner>,
    owner: Owner,
}

impl Lease for CompactLease {
    fn release(self: Box<Self>) -> BoxFuture<'static, Result<(), Failure>> {
        Box::pin(async move {
            let Self {
                lease,
                inner,
                owner,
            } = *self;
            lease
                .release_owned_detailed()
                .await
                .map(|_| ())
                .map_err(|error| classify(&inner, owner, error))
        })
    }
}

impl Coordinator for CompactCoordinator {
    fn acquire(&self, account: i64, caller: Owner) -> BoxFuture<'_, LockResult> {
        Box::pin(async move {
            let owner = self.admit(caller)?;
            let locks = self.inner.cache.locks().await?;
            match locks
                .acquire_retained_detailed(ACCOUNT_REFRESH, account, owner.clone())
                .await
            {
                Ok(lease) => Ok(Some(Box::new(CompactLease {
                    lease,
                    inner: self.inner.clone(),
                    owner,
                }) as Box<dyn Lease>)),
                Err(TaskLockDetailedError::Operational(TaskLockError::Held)) => Ok(None),
                Err(error) => Err(classify(&self.inner, owner, error)),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_does_not_initialize_unused_cache_or_take_new_work() {
        let lock = CompactCoordinator::lazy("unopened fixture cache".into());
        assert!(!lock.was_used());
        assert!(lock.shutdown(Duration::from_millis(10)).await);
        assert!(matches!(
            lock.acquire(9, Arc::new(())).await,
            Err(Failure::Capacity)
        ));
        assert!(!lock.was_used());
        assert!(matches!(&lock.inner.cache, Cache::Lazy { manager, .. } if !manager.initialized()));
    }

    #[test]
    fn closing_cannot_report_unused_after_admitting_a_lock_owner() {
        for _ in 0..32 {
            let lock = CompactCoordinator::lazy("unopened fixture cache".into());
            let barrier = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| {
                    barrier.wait();
                    lock.admit(Arc::new(())).is_ok()
                });
                barrier.wait();
                lock.close_admission();
                let observed_used = lock.was_used();
                assert!(!worker.join().unwrap() || observed_used);
            });
        }
    }

    #[tokio::test]
    async fn retained_admission_blocks_shutdown_until_its_real_owner_releases() {
        let lock = CompactCoordinator::lazy("unopened fixture cache".into());
        let slots: Vec<_> = (0..LEASES)
            .map(|_| lock.admit(Arc::new(())).unwrap())
            .collect();
        assert!(lock.was_used());
        assert!(matches!(lock.admit(Arc::new(())), Err(Failure::Capacity)));
        assert!(!lock.shutdown(Duration::from_millis(1)).await);
        assert!(matches!(lock.admit(Arc::new(())), Err(Failure::Capacity)));
        drop(slots);
        assert!(lock.shutdown(Duration::from_millis(10)).await);
    }
}
