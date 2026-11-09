//! Bounded, owner-checked job leases on original task_lock_* Redis keys.
//! Original Dispatcharr authors retain application and algorithm credit.
use redis::{aio::MultiplexedConnection, Script};
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
struct Locks {
    connection: MultiplexedConnection,
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
        let connection = redis::Client::open(url)
            .map_err(|_| TaskLockError::Invalid)?
            .get_multiplexed_async_connection()
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
        if task.is_empty()
            || task.len() > 128
            || !task.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Err(TaskLockError::Invalid);
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
            key: format!("task_lock_{task}_{id}"),
            token: format!("rust:{}", uuid::Uuid::new_v4()),
            stop,
            lost,
            releasing: AtomicBool::new(false),
            _admission: admission,
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
                    tokio::spawn(renew(Arc::downgrade(&lease.inner)));
                    // A dropped receiver drops the returned lease, which
                    // queues owner-checked cleanup after the SET response.
                    let _ = sender.send(Ok(lease));
                }
                Ok(None) => {
                    let _ = sender.send(Err(TaskLockError::Held));
                    drop(lease);
                }
                Err(_) => {
                    let _ = sender.send(Err(TaskLockError::RedisUnavailable));
                    drop(lease);
                }
            }
        });
        receiver
            .await
            .map_err(|_| TaskLockError::RedisUnavailable)?
    }
}
async fn release(held: Arc<Held>) -> Result<(), TaskLockError> {
    held.stop.send_replace(true);
    let _command = held
        .locks
        .commands
        .acquire()
        .await
        .map_err(|_| TaskLockError::RedisUnavailable)?;
    let mut connection = held.locks.connection.clone();
    let _: i64 = Script::new(RELEASE)
        .key(&held.key)
        .arg(&held.token)
        .invoke_async(&mut connection)
        .await
        .map_err(|_| TaskLockError::RedisUnavailable)?;
    Ok(())
}
impl TaskLease {
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
        if self.inner.releasing.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        // Spawn before awaiting: cancelling this waiter cannot cancel cleanup.
        let inner = self.inner.clone();
        tokio::spawn(release(inner))
            .await
            .map_err(|_| TaskLockError::RedisUnavailable)?
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
