//! Fenced Redis owner/provider reservations for the derivative Dispatcharr port.
//! Original application credit: upstream Dispatcharr authors and contributors.
mod scripts;
mod task_locks;
pub use task_locks::{
    TaskLease, TaskLockCleanupOutcome, TaskLockConfig, TaskLockDetailedError, TaskLockError,
    TaskLockRedisCause, TaskLocks,
};

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
use tokio::{
    sync::{watch, Semaphore},
    time,
};

#[derive(Clone, Copy)]
enum OwnerScope {
    Live,
    Vod,
    Timeshift,
}

#[derive(Clone, Debug)]
pub struct LeaseConfig {
    /// Rust ledger namespace; shared Python owner/profile keys are never prefixed.
    pub namespace: String,
    pub ttl: Duration,
    /// None is useful only for externally driven heartbeat or crash tests.
    pub heartbeat_interval: Option<Duration>,
    pub reap_interval: Option<Duration>,
    pub command_timeout: Duration,
    pub max_inflight_commands: usize,
    pub reap_batch: usize,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            namespace: "rust:coordination".into(),
            ttl: Duration::from_secs(30),
            heartbeat_interval: Some(Duration::from_secs(5)),
            reap_interval: Some(Duration::from_secs(1)),
            command_timeout: Duration::from_secs(2),
            max_inflight_commands: 32,
            reap_batch: 128,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CredentialPool {
    /// Exact legacy server_group_connections:{group_id}:{fingerprint16} key.
    pub counter_key: String,
    pub max_streams: u32,
}

#[derive(Clone, Debug)]
pub struct AcquireRequest {
    pub channel_id: String,
    pub provider_profile_id: Option<i64>,
    /// Zero is unlimited and does not increment legacy profile counters.
    pub provider_max_streams: u32,
    pub credential_pool: Option<CredentialPool>,
}

#[derive(Clone, Debug)]
pub struct ProviderReservation {
    pub profile_id: Option<i64>,
    pub max_streams: u32,
    pub credential_pool: Option<CredentialPool>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    InvalidConfiguration,
    InvalidRequest,
    RedisUnavailable,
    OwnerHeld,
    ProfileFull,
    CredentialFull,
    CredentialConflict,
    AlreadyReserved,
    Lost,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}
impl std::error::Error for Error {}

struct Inner {
    connection: ConnectionManager,
    config: LeaseConfig,
    commands: Arc<Semaphore>,
    acquire: Script,
    heartbeat: Script,
    release: Script,
    reap: Script,
    clear_provider: Script,
    reserve_provider: Script,
}

#[derive(Clone)]
pub struct Coordinator {
    inner: Arc<Inner>,
}

#[derive(Clone)]
struct Keys {
    owner: String,
    ledger: String,
    index: String,
}

struct LeaseInner {
    coordinator: Weak<Inner>,
    keys: Keys,
    token: String,
    fence: u64,
    stop: watch::Sender<bool>,
    lost: watch::Sender<bool>,
    release_started: AtomicBool,
}

/// One owner/provider reservation. Drop attempts async release; Redis TTL plus
/// the persistent expiry ledger recover it if the process cannot finish cleanup.
pub struct Lease {
    inner: Arc<LeaseInner>,
}

// Created before ACQUIRE is queued. Its permit travels with the oneshot
// result, so canceling a receiver cannot release admission before cleanup.
struct AcquireDelivery {
    coordinator: Coordinator,
    keys: Keys,
    token: String,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    lease: Option<Lease>,
    armed: bool,
}
impl AcquireDelivery {
    fn finish(mut self) -> Lease {
        self.armed = false;
        self.lease.take().expect("successful lease delivery")
    }
}
impl Drop for AcquireDelivery {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(lease) = self.lease.take() {
            lease.inner.release_started.store(true, Ordering::Release);
            lease.inner.stop.send_replace(true);
            lease.inner.lost.send_replace(true);
        }
        let Some(permit) = self.permit.take() else {
            return;
        };
        let coordinator = self.coordinator.clone();
        let keys = self.keys.clone();
        let token = self.token.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _admission = permit;
                let mut connection = coordinator.inner.connection.clone();
                // No caller deadline around this future: the actual Redis
                // response/disconnect releases its bounded operation budget.
                let _: Result<i64, redis::RedisError> = coordinator
                    .inner
                    .release
                    .key(keys.owner)
                    .key(keys.ledger)
                    .key(keys.index)
                    .arg(token)
                    .invoke_async(&mut connection)
                    .await;
            });
        }
    }
}

impl Coordinator {
    pub async fn connect(url: &str, config: LeaseConfig) -> Result<Self, Error> {
        if config.namespace.is_empty()
            || config.namespace.len() > 128
            || config.ttl.as_millis() < 10
            || config.ttl.as_millis() > i64::MAX as u128
            || config.command_timeout.is_zero()
            || config.max_inflight_commands == 0
            || config.max_inflight_commands > Semaphore::MAX_PERMITS
            || config.reap_batch == 0
            || config.reap_batch > 1024
            || config
                .heartbeat_interval
                .is_some_and(|interval| interval.is_zero() || interval >= config.ttl)
            || config
                .reap_interval
                .is_some_and(|interval| interval.is_zero())
        {
            return Err(Error::InvalidConfiguration);
        }
        let client = redis::Client::open(url).map_err(|_| Error::InvalidConfiguration)?;
        let connection = time::timeout(
            config.command_timeout,
            client.get_connection_manager_with_config(
                ConnectionManagerConfig::new()
                    .set_number_of_retries(0)
                    .set_connection_timeout(config.command_timeout),
            ),
        )
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)?;
        let inner = Arc::new(Inner {
            commands: Arc::new(Semaphore::new(config.max_inflight_commands)),
            connection,
            config,
            acquire: Script::new(&format!("{}{}", scripts::REAP, scripts::ACQUIRE)),
            heartbeat: Script::new(&format!("{}{}", scripts::REAP, scripts::HEARTBEAT)),
            release: Script::new(&format!("{}{}", scripts::REAP, scripts::RELEASE)),
            reap: Script::new(&format!("{}{}", scripts::REAP, scripts::REAP_ONLY)),
            clear_provider: Script::new(&format!("{}{}", scripts::REAP, scripts::CLEAR_PROVIDER)),
            reserve_provider: Script::new(&format!(
                "{}{}",
                scripts::REAP,
                scripts::RESERVE_PROVIDER
            )),
        });
        if let Some(interval) = inner.config.reap_interval {
            let weak = Arc::downgrade(&inner);
            tokio::spawn(async move {
                loop {
                    time::sleep(interval).await;
                    let Some(inner) = weak.upgrade() else { return };
                    let coordinator = Self { inner };
                    let _ = coordinator.reap_expired().await;
                }
            });
        }
        Ok(Self { inner })
    }

    /// Snapshot of admitted Redis work, including detached acquisition cleanup.
    /// Shutdown callers must first stop new admissions in their service scopes.
    pub fn is_idle(&self) -> bool {
        self.inner.commands.available_permits() == self.inner.config.max_inflight_commands
    }

    pub async fn acquire(&self, request: AcquireRequest) -> Result<Lease, Error> {
        self.acquire_scoped(request, OwnerScope::Live).await
    }

    /// VOD sessions share provider counters but never masquerade as live channel
    /// owners. The same fenced ledger/reaper handles both owner namespaces.
    pub async fn acquire_vod(&self, request: AcquireRequest) -> Result<Lease, Error> {
        self.acquire_scoped(request, OwnerScope::Vod).await
    }

    /// Archived playback shares provider quotas without creating a live owner.
    pub async fn acquire_timeshift(&self, request: AcquireRequest) -> Result<Lease, Error> {
        self.acquire_scoped(request, OwnerScope::Timeshift).await
    }

    async fn acquire_scoped(
        &self,
        request: AcquireRequest,
        scope: OwnerScope,
    ) -> Result<Lease, Error> {
        if request.channel_id.is_empty()
            || request.channel_id.len() > 256
            || request
                .provider_profile_id
                .is_some_and(|profile| profile <= 0)
            || request.provider_max_streams > 0 && request.provider_profile_id.is_none()
            || request.credential_pool.as_ref().is_some_and(|pool| {
                pool.max_streams == 0
                    || !pool.counter_key.starts_with("server_group_connections:")
                    || pool.counter_key.len() > 256
            })
        {
            return Err(Error::InvalidRequest);
        }
        // Only admitted operations get a detached supervisor. The supervisor
        // owns its command budget until the real reply and fenced cleanup,
        // even when the caller cancels or its deadline expires.
        let permit = self.permit().await?;
        let coordinator = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = coordinator.acquire_admitted(request, scope, permit).await;
            let _ = sender.send(result); // Undelivered guard performs fenced cleanup.
        });
        let delivery = time::timeout(self.inner.config.command_timeout, receiver)
            .await
            .map_err(|_| Error::RedisUnavailable)?
            .map_err(|_| Error::RedisUnavailable)??;
        Ok(delivery.finish())
    }

    async fn acquire_admitted(
        &self,
        request: AcquireRequest,
        scope: OwnerScope,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<AcquireDelivery, Error> {
        let token = format!("rust:{}", uuid::Uuid::new_v4());
        let keys = Keys {
            owner: match scope {
                OwnerScope::Live => format!("live:channel:{}:owner", request.channel_id),
                OwnerScope::Vod | OwnerScope::Timeshift => {
                    let name = match scope {
                        OwnerScope::Vod => "vod",
                        _ => "timeshift",
                    };
                    format!(
                        "{}:{name}:session:{}:owner",
                        self.inner.config.namespace, request.channel_id
                    )
                }
            },
            ledger: format!("{}:lease:{}", self.inner.config.namespace, token),
            index: format!("{}:leases", self.inner.config.namespace),
        };
        let profile = request.provider_profile_id.unwrap_or(0);
        let credential_key = request
            .credential_pool
            .as_ref()
            .map_or("", |pool| pool.counter_key.as_str());
        let credential_maximum = request
            .credential_pool
            .as_ref()
            .map_or(0, |pool| pool.max_streams);
        let mut connection = self.inner.connection.clone();
        let mut delivery = AcquireDelivery {
            coordinator: self.clone(),
            keys: keys.clone(),
            token: token.clone(),
            permit: Some(permit),
            lease: None,
            armed: true,
        };
        let result: (i64, u64) = self
            .inner
            .acquire
            .key(&keys.owner)
            .key(&keys.ledger)
            .key(&keys.index)
            .key(format!("profile_connections:{profile}"))
            .key(credential_key)
            .key(format!("profile_credential_release:{profile}"))
            .key(format!("{}:fence", self.inner.config.namespace))
            .arg(&token)
            .arg(self.inner.config.ttl.as_millis() as u64)
            .arg(request.provider_max_streams)
            .arg(credential_maximum)
            .arg(self.inner.config.reap_batch)
            .invoke_async(&mut connection)
            .await
            .map_err(|_| Error::RedisUnavailable)?;
        if result.0 != 1 {
            // A definitive negative result allocated nothing. An I/O failure
            // above leaves the guard armed because Redis may have applied Lua.
            delivery.armed = false;
            return Err(match result.0 {
                -1 => Error::OwnerHeld,
                -2 => Error::ProfileFull,
                -3 => Error::CredentialFull,
                -4 => Error::CredentialConflict,
                _ => Error::RedisUnavailable,
            });
        }
        let (stop, _) = watch::channel(false);
        let (lost, _) = watch::channel(false);
        let inner = Arc::new(LeaseInner {
            coordinator: Arc::downgrade(&self.inner),
            keys,
            token,
            fence: result.1,
            stop,
            lost,
            release_started: AtomicBool::new(false),
        });
        if let Some(interval) = self.inner.config.heartbeat_interval {
            let weak = Arc::downgrade(&inner);
            let mut stopping = inner.stop.subscribe();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = stopping.changed() => return,
                        _ = time::sleep(interval) => {},
                    }
                    let Some(lease) = weak.upgrade() else { return };
                    if lease.release_started.load(Ordering::Acquire) {
                        return;
                    }
                    if heartbeat_inner(&lease).await != Ok(true) {
                        lease.lost.send_replace(true);
                        lease.stop.send_replace(true);
                        return;
                    }
                }
            });
        }
        delivery.lease = Some(Lease { inner });
        Ok(delivery)
    }

    async fn permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, Error> {
        time::timeout(
            self.inner.config.command_timeout,
            self.inner.commands.clone().acquire_owned(),
        )
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)
    }

    pub async fn reap_expired(&self) -> Result<u64, Error> {
        let _permit = self.permit().await?;
        let mut connection = self.inner.connection.clone();
        time::timeout(
            self.inner.config.command_timeout,
            self.inner
                .reap
                .key(format!("{}:leases", self.inner.config.namespace))
                .arg(self.inner.config.reap_batch)
                .invoke_async(&mut connection),
        )
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)
    }
}

impl Lease {
    pub fn token(&self) -> &str {
        &self.inner.token
    }
    pub fn fence(&self) -> u64 {
        self.inner.fence
    }
    pub fn lost(&self) -> watch::Receiver<bool> {
        self.inner.lost.subscribe()
    }

    /// Free only the current provider allocation, retaining the fenced owner.
    /// Call after closing its upstream and before selecting another profile.
    pub async fn release_provider(&self) -> Result<bool, Error> {
        if self.inner.release_started.load(Ordering::Acquire) {
            return Err(Error::Lost);
        }
        let inner = self.inner.coordinator.upgrade().ok_or(Error::Lost)?;
        let coordinator = Coordinator { inner };
        let _permit = coordinator.permit().await?;
        let mut connection = coordinator.inner.connection.clone();
        let result: i64 = time::timeout(
            coordinator.inner.config.command_timeout,
            coordinator
                .inner
                .clear_provider
                .key(&self.inner.keys.owner)
                .key(&self.inner.keys.ledger)
                .key(&self.inner.keys.index)
                .arg(&self.inner.token)
                .invoke_async(&mut connection),
        )
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)?;
        if result < 0 {
            self.inner.lost.send_replace(true);
            return Err(Error::Lost);
        }
        Ok(result == 1)
    }

    /// Atomically reserve a provider under the existing owner. The previous
    /// provider must already be cleared; failed attempts allocate nothing.
    pub async fn reserve_provider(&self, provider: ProviderReservation) -> Result<(), Error> {
        if self.inner.release_started.load(Ordering::Acquire) {
            return Err(Error::Lost);
        }
        if provider.profile_id.is_some_and(|profile| profile <= 0)
            || provider.max_streams > 0 && provider.profile_id.is_none()
            || provider.credential_pool.as_ref().is_some_and(|pool| {
                pool.max_streams == 0
                    || !pool.counter_key.starts_with("server_group_connections:")
                    || pool.counter_key.len() > 256
            })
        {
            return Err(Error::InvalidRequest);
        }
        let inner = self.inner.coordinator.upgrade().ok_or(Error::Lost)?;
        let coordinator = Coordinator { inner };
        let _permit = coordinator.permit().await?;
        let mut connection = coordinator.inner.connection.clone();
        let profile = provider.profile_id.unwrap_or(0);
        let credential_key = provider
            .credential_pool
            .as_ref()
            .map_or("", |pool| pool.counter_key.as_str());
        let credential_maximum = provider
            .credential_pool
            .as_ref()
            .map_or(0, |pool| pool.max_streams);
        let result: i64 = time::timeout(
            coordinator.inner.config.command_timeout,
            coordinator
                .inner
                .reserve_provider
                .key(&self.inner.keys.owner)
                .key(&self.inner.keys.ledger)
                .key(&self.inner.keys.index)
                .key(format!("profile_connections:{profile}"))
                .key(credential_key)
                .key(format!("profile_credential_release:{profile}"))
                .arg(&self.inner.token)
                .arg(provider.max_streams)
                .arg(credential_maximum)
                .invoke_async(&mut connection),
        )
        .await
        .map_err(|_| Error::RedisUnavailable)?
        .map_err(|_| Error::RedisUnavailable)?;
        match result {
            1 => Ok(()),
            -1 => {
                self.inner.lost.send_replace(true);
                Err(Error::Lost)
            }
            -2 => Err(Error::ProfileFull),
            -3 => Err(Error::CredentialFull),
            -4 => Err(Error::CredentialConflict),
            -5 => Err(Error::AlreadyReserved),
            _ => Err(Error::RedisUnavailable),
        }
    }
    pub async fn heartbeat(&self) -> Result<bool, Error> {
        if self.inner.release_started.load(Ordering::Acquire) {
            return Err(Error::Lost);
        }
        let result = heartbeat_inner(&self.inner).await;
        if result != Ok(true) {
            self.inner.lost.send_replace(true);
            self.inner.stop.send_replace(true);
        }
        result
    }
    pub async fn release(&self) -> Result<bool, Error> {
        if self.inner.release_started.swap(true, Ordering::AcqRel) {
            return Ok(false);
        }
        self.inner.stop.send_replace(true);
        self.inner.lost.send_replace(true);
        release_inner(&self.inner).await
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.inner.stop.send_replace(true);
        if self.inner.release_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let inner = self.inner.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = release_inner(&inner).await;
            });
        }
    }
}

async fn heartbeat_inner(lease: &LeaseInner) -> Result<bool, Error> {
    let inner = lease.coordinator.upgrade().ok_or(Error::Lost)?;
    let coordinator = Coordinator { inner };
    let _permit = coordinator.permit().await?;
    let mut connection = coordinator.inner.connection.clone();
    let result: i64 = time::timeout(
        coordinator.inner.config.command_timeout,
        coordinator
            .inner
            .heartbeat
            .key(&lease.keys.owner)
            .key(&lease.keys.ledger)
            .key(&lease.keys.index)
            .arg(&lease.token)
            .arg(coordinator.inner.config.ttl.as_millis() as u64)
            .invoke_async(&mut connection),
    )
    .await
    .map_err(|_| Error::RedisUnavailable)?
    .map_err(|_| Error::RedisUnavailable)?;
    Ok(result == 1)
}

async fn release_inner(lease: &LeaseInner) -> Result<bool, Error> {
    let inner = lease.coordinator.upgrade().ok_or(Error::Lost)?;
    let coordinator = Coordinator { inner };
    let _permit = coordinator.permit().await?;
    let mut connection = coordinator.inner.connection.clone();
    let result: i64 = time::timeout(
        coordinator.inner.config.command_timeout,
        coordinator
            .inner
            .release
            .key(&lease.keys.owner)
            .key(&lease.keys.ledger)
            .key(&lease.keys.index)
            .arg(&lease.token)
            .invoke_async(&mut connection),
    )
    .await
    .map_err(|_| Error::RedisUnavailable)?
    .map_err(|_| Error::RedisUnavailable)?;
    Ok(result == 1)
}
