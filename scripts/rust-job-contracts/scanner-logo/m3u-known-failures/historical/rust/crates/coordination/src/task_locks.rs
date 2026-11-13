//! Bounded, owner-checked job leases on original task_lock_* Redis keys.
//! Original Dispatcharr authors retain application and algorithm credit.
use redis::{
    aio::{ConnectionManager, ConnectionManagerConfig},
    Script,
};
use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Weak,
    },
    time::Duration,
};
use tokio::sync::{oneshot, watch, OwnedSemaphorePermit, Semaphore};

const RELEASE: &str =
    "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end";
const RENEW: &str = "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('EXPIRE',KEYS[1],ARGV[2]) else return 0 end";

#[derive(Clone, Debug)]
pub struct TaskLockConfig {
    pub ttl: Duration,
    pub renewal_interval: Duration,
    pub max_leases: usize,
    pub max_commands: usize,
}
impl Default for TaskLockConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(300),
            renewal_interval: Duration::from_secs(120),
            max_leases: 4,
            max_commands: 16,
        }
    }
}
#[derive(Debug, PartialEq, Eq)]
pub enum TaskLockError {
    Invalid,
    Capacity,
    Held,
    RedisUnavailable,
}
impl fmt::Display for TaskLockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for TaskLockError {}
/// Bounded diagnostic cause from a physically completed Redis command.
/// This is not a Python exception classifier; connector and task stages differ.
#[derive(Clone, Debug)]
pub struct TaskLockRedisCause {
    pub kind: redis::ErrorKind,
    pub code: Option<String>,
    pub code_oversized: bool,
    pub io_kind: Option<std::io::ErrorKind>,
    pub raw_os_error: Option<i32>,
    pub detail: Option<String>,
    pub detail_oversized: bool,
}
impl TaskLockRedisCause {
    fn capture(error: &redis::RedisError) -> Self {
        let io = std::error::Error::source(error)
            .and_then(|value| value.downcast_ref::<std::io::Error>());
        let detail = error.detail();
        let code = error.code();
        Self {
            kind: error.kind(),
            code: code.filter(|value| value.len() <= 64).map(str::to_owned),
            code_oversized: code.is_some_and(|value| value.len() > 64),
            io_kind: io.map(std::io::Error::kind),
            raw_os_error: io.and_then(std::io::Error::raw_os_error),
            detail: detail
                .filter(|value| value.len() <= 16 * 1024)
                .map(str::to_owned),
            detail_oversized: detail.is_some_and(|value| value.len() > 16 * 1024),
        }
    }
}
#[derive(Debug)]
pub enum TaskLockDetailedError {
    Operational(TaskLockError),
    Redis(TaskLockRedisCause),
    Acquisition {
        cause: TaskLockRedisCause,
        cleanup: TaskLockCleanupOutcome,
    },
}
#[derive(Clone, Debug)]
pub enum TaskLockCleanupOutcome {
    Released,
    NotOwner,
    Failed(TaskLockRedisCause),
    Unavailable,
}
impl TaskLockDetailedError {
    fn coarse(self) -> TaskLockError {
        match self {
            Self::Operational(error) => error,
            Self::Redis(_) | Self::Acquisition { .. } => TaskLockError::RedisUnavailable,
        }
    }
}
impl From<TaskLockError> for TaskLockDetailedError {
    fn from(error: TaskLockError) -> Self {
        Self::Operational(error)
    }
}

struct Locks {
    connection: ConnectionManager,
    config: TaskLockConfig,
    commands: Arc<Semaphore>,
    leases: Arc<Semaphore>,
}
#[derive(Clone)]
pub struct TaskLocks {
    inner: Arc<Locks>,
}
struct Held {
    locks: Arc<Locks>,
    key: String,
    token: String,
    stop: watch::Sender<bool>,
    lost: watch::Sender<bool>,
    releasing: AtomicBool,
    // This physical reservation survives a cancelled acquisition receiver and
    // any asynchronous release until the actual Redis command has completed.
    _admission: OwnedSemaphorePermit,
    _caller: Option<Arc<dyn Send + Sync>>,
}
pub struct TaskLease {
    inner: Arc<Held>,
}
impl TaskLocks {
    pub async fn connect(url: &str, config: TaskLockConfig) -> Result<Self, TaskLockError> {
        if config.ttl.as_secs() == 0
            || config.renewal_interval.is_zero()
            || config.renewal_interval >= Duration::from_secs(config.ttl.as_secs())
            || config.max_leases == 0
            || config.max_commands == 0
            || config.max_leases > 1024
            || config.max_commands > 1024
        {
            return Err(TaskLockError::Invalid);
        }
        let client = redis::Client::open(url).map_err(|_| TaskLockError::Invalid)?;
        // The manager reconnects a shared future for subsequent commands; it
        // returns the failed command error without replaying an uncertain SET
        // or DEL. A single connection attempt is bounded; no response timeout
        // may release physical owners while a queued Redis reply is pending.
        let connection = client
            .get_connection_manager_with_config(
                ConnectionManagerConfig::new()
                    .set_number_of_retries(0)
                    .set_connection_timeout(Duration::from_secs(2)),
            )
            .await
            .map_err(|_| TaskLockError::RedisUnavailable)?;
        Ok(Self {
            inner: Arc::new(Locks {
                connection,
                commands: Arc::new(Semaphore::new(config.max_commands)),
                leases: Arc::new(Semaphore::new(config.max_leases)),
                config,
            }),
        })
    }
    pub fn available_leases(&self) -> usize {
        self.inner.leases.available_permits()
    }
    pub async fn acquire(&self, task: &str, id: i64) -> Result<TaskLease, TaskLockError> {
        self.acquire_impl(task, id, None, false)
            .await
            .map_err(TaskLockDetailedError::coarse)
    }
    /// Same supervised physical acquisition as acquire, preserving its bounded
    /// Redis cause. Cancellation still queues owner-checked cleanup after SET.
    pub async fn acquire_detailed(
        &self,
        task: &str,
        id: i64,
    ) -> Result<TaskLease, TaskLockDetailedError> {
        self.acquire_impl(task, id, None, true).await
    }
    /// Retain the caller through SET, any ambiguous-acquisition cleanup, and
    /// the successful lease's eventual physical release. No source exception
    /// class is inferred from these transport diagnostics.
    pub async fn acquire_retained_detailed(
        &self,
        task: &str,
        id: i64,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<TaskLease, TaskLockDetailedError> {
        self.acquire_impl(task, id, Some(caller), true).await
    }
    /// Source SeriesRules uses the fixed literal `all` suffix and has no
    /// renewer. Keep that authority typed; callers cannot supply arbitrary keys.
    /// SET/cleanup/release retain the caller through actual physical replies.
    pub async fn acquire_series_rules_retained(
        &self,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<TaskLease, TaskLockDetailedError> {
        self.acquire_suffix_impl("evaluate_series_rules", "all", Some(caller), true, false)
            .await
    }
    async fn acquire_impl(
        &self,
        task: &str,
        id: i64,
        caller: Option<Arc<dyn Send + Sync>>,
        settle_failure: bool,
    ) -> Result<TaskLease, TaskLockDetailedError> {
        let id = id.to_string();
        self.acquire_suffix_impl(task, &id, caller, settle_failure, true)
            .await
    }
    async fn acquire_suffix_impl(
        &self,
        task: &str,
        suffix: &str,
        caller: Option<Arc<dyn Send + Sync>>,
        settle_failure: bool,
        renewing: bool,
    ) -> Result<TaskLease, TaskLockDetailedError> {
        if task.is_empty()
            || task.len() > 128
            || !task.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Err(TaskLockError::Invalid.into());
        }
        let admission = self
            .inner
            .leases
            .clone()
            .try_acquire_owned()
            .map_err(|_| TaskLockError::Capacity)?;
        let command = self
            .inner
            .commands
            .clone()
            .try_acquire_owned()
            .map_err(|_| TaskLockError::Capacity)?;
        let (stop, _) = watch::channel(false);
        let (lost, _) = watch::channel(false);
        let held = Arc::new(Held {
            locks: self.inner.clone(),
            key: format!("task_lock_{task}_{suffix}"),
            token: format!("rust:{}", uuid::Uuid::new_v4()),
            stop,
            lost,
            releasing: AtomicBool::new(false),
            _admission: admission,
            _caller: caller,
        });
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let _command = command;
            let mut connection = held.locks.connection.clone();
            let result: Result<Option<String>, _> = redis::cmd("SET")
                .arg(&held.key)
                .arg(&held.token)
                .arg("NX")
                .arg("EX")
                .arg(held.locks.config.ttl.as_secs())
                .query_async(&mut connection)
                .await;
            let lease = TaskLease { inner: held };
            match result {
                Ok(Some(_)) => {
                    if renewing {
                        tokio::spawn(renew(Arc::downgrade(&lease.inner)));
                    }
                    // A dropped receiver drops the returned lease, which
                    // queues owner-checked cleanup after the SET response.
                    let _ = sender.send(Ok(lease));
                }
                Ok(None) => {
                    if settle_failure {
                        // A definite NX miss never owned a token to clean up.
                        lease.inner.releasing.store(true, Ordering::Release);
                    }
                    let _ = sender.send(Err(TaskLockError::Held.into()));
                    drop(lease);
                }
                Err(error) => {
                    let cause = TaskLockRedisCause::capture(&error);
                    if settle_failure {
                        // An I/O error can hide a successful remote SET. Settle
                        // owner-checked cleanup before reporting its outcome.
                        lease.inner.releasing.store(true, Ordering::Release);
                        drop(_command);
                        let cleanup = match release_detailed(lease.inner.clone()).await {
                            Ok(true) => TaskLockCleanupOutcome::Released,
                            Ok(false) => TaskLockCleanupOutcome::NotOwner,
                            Err(TaskLockDetailedError::Redis(error)) => {
                                TaskLockCleanupOutcome::Failed(error)
                            }
                            Err(_) => TaskLockCleanupOutcome::Unavailable,
                        };
                        let _ =
                            sender.send(Err(TaskLockDetailedError::Acquisition { cause, cleanup }));
                    } else {
                        let _ = sender.send(Err(TaskLockDetailedError::Redis(cause)));
                    }
                    drop(lease);
                }
            }
        });
        receiver
            .await
            .map_err(|_| TaskLockDetailedError::Operational(TaskLockError::RedisUnavailable))?
    }
}
async fn release(held: Arc<Held>) -> Result<bool, TaskLockError> {
    release_detailed(held)
        .await
        .map_err(TaskLockDetailedError::coarse)
}
async fn release_detailed(held: Arc<Held>) -> Result<bool, TaskLockDetailedError> {
    held.stop.send_replace(true);
    let _command = held
        .locks
        .commands
        .acquire()
        .await
        .map_err(|_| TaskLockDetailedError::Operational(TaskLockError::RedisUnavailable))?;
    let mut connection = held.locks.connection.clone();
    let removed: i64 = Script::new(RELEASE)
        .key(&held.key)
        .arg(&held.token)
        .invoke_async(&mut connection)
        .await
        .map_err(|error| TaskLockDetailedError::Redis(TaskLockRedisCause::capture(&error)))?;
    Ok(removed == 1)
}
impl TaskLease {
    /// Preserve only a failed scanner M3U-groups lease for its existing TTL.
    /// This deliberately stops renewal rather than copying Source's leaked daemon.
    /// No DEL/EXPIRE occurs; a replaced or uncertain owner cannot authorize a result.
    /// Physical commands retain the original lease/caller after waiter cancellation.
    pub async fn preserve_failed_m3u_groups_until_expiry(
        self,
    ) -> Result<bool, TaskLockDetailedError> {
        let suffix = self
            .inner
            .key
            .strip_prefix("task_lock_refresh_m3u_account_groups_");
        if !suffix.is_some_and(|v| {
            v.parse::<i32>()
                .is_ok_and(|id| id > 0 && id.to_string() == v)
        }) || self.inner.locks.config.ttl != Duration::from_secs(300)
            || self.inner.locks.config.renewal_interval != Duration::from_secs(120)
        {
            return Err(TaskLockError::Invalid.into());
        }
        if self.inner.releasing.swap(true, Ordering::AcqRel) {
            return Ok(false);
        }
        self.inner.stop.send_replace(true);
        let held = self.inner.clone();
        tokio::spawn(async move {
            let _command = held.locks.commands.acquire().await
                .map_err(|_| TaskLockDetailedError::Operational(TaskLockError::RedisUnavailable))?;
            let mut connection = held.locks.connection.clone();
            // Bound GET before allocation and confirm owner+finite TTL atomically.
            const CONFIRM: &str = "if redis.call('TYPE',KEYS[1]).ok~='string' then return 0 end;if redis.call('STRLEN',KEYS[1])~=#ARGV[1] then return 0 end;if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end;local t=redis.call('TTL',KEYS[1]);if t>0 and t<=300 then return 1 else return 0 end";
            let confirmed: i64 = Script::new(CONFIRM).key(&held.key).arg(&held.token)
                .invoke_async(&mut connection).await
                .map_err(|e| TaskLockDetailedError::Redis(TaskLockRedisCause::capture(&e)))?;
            Ok(confirmed == 1)
        }).await.map_err(|_| TaskLockDetailedError::Operational(TaskLockError::RedisUnavailable))?
    }
    pub fn key(&self) -> &str {
        &self.inner.key
    }
    pub fn is_lost(&self) -> bool {
        *self.inner.lost.borrow()
    }
    pub fn loss_signal(&self) -> watch::Receiver<bool> {
        self.inner.lost.subscribe()
    }
    pub async fn release(self) -> Result<(), TaskLockError> {
        self.release_owned().await.map(|_| ())
    }
    /// Return whether the actual owner-checked DEL removed our lease. False
    /// includes a missing/replaced owner; it cannot authorize successor work.
    /// The physical task/admission survives cancellation of this waiter.
    pub async fn release_owned(self) -> Result<bool, TaskLockError> {
        self.release_owned_detailed()
            .await
            .map_err(TaskLockDetailedError::coarse)
    }
    /// Preserve actual Redis reply diagnostics while retaining the same owner
    /// check and physical cleanup task through cancellation of its waiter.
    pub async fn release_owned_detailed(self) -> Result<bool, TaskLockDetailedError> {
        if self.inner.releasing.swap(true, Ordering::AcqRel) {
            return Ok(false);
        }
        // Spawn before awaiting: cancelling this waiter cannot cancel cleanup.
        let inner = self.inner.clone();
        tokio::spawn(release_detailed(inner))
            .await
            .map_err(|_| TaskLockDetailedError::Operational(TaskLockError::RedisUnavailable))?
    }
}
impl Drop for TaskLease {
    fn drop(&mut self) {
        if self.inner.releasing.swap(true, Ordering::AcqRel) {
            return;
        }
        self.inner.stop.send_replace(true);
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(release(self.inner.clone()));
        }
        // Abrupt runtime/process death recovers through the shared Redis TTL.
    }
}
async fn renew(weak: Weak<Held>) {
    let Some(initial) = weak.upgrade() else {
        return;
    };
    let interval = initial.locks.config.renewal_interval;
    let mut stop = initial.stop.subscribe();
    drop(initial);
    loop {
        if *stop.borrow() {
            return;
        }
        tokio::select! {_=stop.changed()=>return, _=tokio::time::sleep(interval)=>{}}
        let Some(held) = weak.upgrade() else { return };
        let command = tokio::select! {
            _=stop.changed()=>return,
            result=held.locks.commands.acquire()=>result,
        };
        let Ok(_command) = command else { return };
        let mut connection = held.locks.connection.clone();
        let result: Result<i64, _> = Script::new(RENEW)
            .key(&held.key)
            .arg(&held.token)
            .arg(held.locks.config.ttl.as_secs())
            .invoke_async(&mut connection)
            .await;
        if !matches!(result, Ok(1)) {
            held.lost.send_replace(true);
            return;
        }
    }
}

#[cfg(test)]
mod detailed_cause_tests {
    use super::*;
    #[test]
    fn detailed_server_error_does_not_retain_oversized_reply_text() {
        let raw =
            redis::RedisError::from((redis::ErrorKind::ResponseError, "owned", "x".repeat(16385)));
        let cause = TaskLockRedisCause::capture(&raw);
        assert_eq!(cause.kind, redis::ErrorKind::ResponseError);
        assert!(cause.detail.is_none());
        assert!(cause.detail_oversized);
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn detailed_io_cause_keeps_original_errno_without_inventing_python_exception() {
        let raw = redis::RedisError::from(std::io::Error::from_raw_os_error(111));
        let cause = TaskLockRedisCause::capture(&raw);
        assert_eq!(cause.kind, redis::ErrorKind::IoError);
        assert_eq!(cause.io_kind, Some(std::io::ErrorKind::ConnectionRefused));
        assert_eq!(cause.raw_os_error, Some(111));
        assert!(cause.detail.is_none());
        assert!(!cause.detail_oversized);
    }
}
