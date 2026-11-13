//! Fixture-authorized publisher/result reader; no consumer or application route.
//! Original Dispatcharr authors retain application credit and AGPL licensing.
#[cfg(test)]
#[path = "stats_transport.rs"]
mod stats_transport;
use crate::{
    decode::{self, Metadata},
    protocol::{self, Published, Task},
};
use dispatcharr_output_store::transport::{ClassifiedError, RedisTransport, TransportConfig};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Closed,
    Capacity,
    Backend,
    Corrupt,
    Decode(decode::DecodeError),
    Protocol,
    UnsupportedTask,
    ClaimRace,
    AmbiguousClaim(Uuid),
    AmbiguousResult(Uuid),
    AmbiguousPublication(Uuid),
    DrainTimeout,
    LostReservation,
    LostMutex,
    AmbiguousReservation { task_id: Uuid, delivery_tag: Uuid },
    AmbiguousAck(Uuid),
    AmbiguousRestore(Uuid),
}
#[derive(Clone, Debug)]
pub struct Config {
    pub operations: usize,
    pub command_timeout: Duration,
    pub max_queued_jobs: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            operations: 4,
            command_timeout: Duration::from_secs(2),
            max_queued_jobs: 1000,
        }
    }
}
struct Lease {
    _slot: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
}
// A reserve timeout is logical only. The unique marker remains inside the
// physical command's Lease until its real reply/disconnection has completed.
struct PhysicalCompletion(Option<tokio::sync::oneshot::Sender<()>>);
impl Drop for PhysicalCompletion {
    fn drop(&mut self) {
        if let Some(done) = self.0.take() {
            let _ = done.send(());
        }
    }
}
struct Inner {
    broker: RedisTransport,
    result: RedisTransport,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
    config: Config,
    database: String,
    authority: Authority,
    eta: std::sync::Mutex<Option<Arc<crate::eta::Pool>>>,
    cleanup_receipts: std::sync::Mutex<Option<Arc<cleanup_reservation_transport::Pool>>>,
}
#[derive(Clone)]
pub struct Jobs {
    inner: Arc<Inner>,
}
fn endpoint(raw: &str, db: i64) -> Result<(), Error> {
    let client = redis::Client::open(raw).map_err(|_| Error::Configuration)?;
    let info = client.get_connection_info();
    if info.redis.db != db || info.redis.password.as_deref() != Some("playback-fixture-only") {
        return Err(Error::Configuration);
    }
    match &info.addr {
        redis::ConnectionAddr::Tcp(host, port)
            if matches!(host.as_str(), "127.0.0.1" | "localhost" | "::1")
                && (*port == 6379 || (61000..=65000).contains(port)) =>
        {
            Ok(())
        }
        _ => Err(Error::Configuration),
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Authority {
    Fixture,
    Index,
    Refresh,
    Series,
    DefaultServices,
    Backup,
}
impl Jobs {
    /// Test-only authority: copied DB + isolated loopback RedisDB8/9. There is
    /// deliberately no production constructor or implicit default namespace.
    pub async fn connect_fixture(
        broker: &str,
        result: &str,
        database: &str,
        config: Config,
    ) -> Result<Self, Error> {
        if std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref() != Ok("playback-fixture-only")
            || std::env::var("POSTGRES_DB").as_deref() != Ok(database)
        {
            return Err(Error::Configuration);
        }
        if !database.starts_with("rust_probe_backup_")
            || database.len() > 63
            || !database
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            || config.operations == 0
            || config.operations > 16
            || config.command_timeout.is_zero()
            || config.command_timeout > Duration::from_secs(30)
            || config.max_queued_jobs == 0
            || config.max_queued_jobs > 10000
        {
            return Err(Error::Configuration);
        }
        endpoint(broker, 8)?;
        endpoint(result, 9)?;
        // Require the same physical endpoint with independently selected DBs.
        let b = redis::Client::open(broker).map_err(|_| Error::Configuration)?;
        let r = redis::Client::open(result).map_err(|_| Error::Configuration)?;
        if b.get_connection_info().addr != r.get_connection_info().addr {
            return Err(Error::Configuration);
        }
        Self::connect_owned(
            broker,
            result,
            config,
            database.to_owned(),
            Authority::Fixture,
        )
        .await
    }
    /// Explicit create/status-only authority. Starts no consumer; grants no
    /// restore/index/refresh/default family or implicit Main registration.
    pub async fn connect_backup(broker: &str, result: &str, config: Config) -> Result<Self, Error> {
        Self::connect_owned(broker, result, config, String::new(), Authority::Backup).await
    }
    pub(crate) fn allows_backup(&self) -> bool {
        matches!(self.inner.authority, Authority::Fixture | Authority::Backup)
    }
    /// Production connection authority for programme-index tasks only. This
    /// neither starts a consumer nor grants backup/restore publication/execution.
    /// Configured broker/backend may use different endpoints or the same DB,
    /// matching Source's separately configurable URLs. Errors never downgrade TLS.
    pub(crate) fn operation_capacity(&self) -> usize {
        self.inner.config.operations
    }
    pub async fn connect_index(broker: &str, result: &str, config: Config) -> Result<Self, Error> {
        Self::connect_owned(broker, result, config, String::new(), Authority::Index).await
    }
    /// Explicit refresh-family authority. This grants no backup/restore publication
    /// or consumption and starts no worker by itself. Unported queue heads remain
    /// untouched and are reported as UnsupportedTask.
    pub async fn connect_refresh(
        broker: &str,
        result: &str,
        config: Config,
    ) -> Result<Self, Error> {
        Self::connect_owned(broker, result, config, String::new(), Authority::Refresh).await
    }
    pub(crate) fn allows_refresh(&self) -> bool {
        matches!(
            self.inner.authority,
            Authority::Fixture | Authority::Refresh
        )
    }
    /// Narrow no-recording Series authority; no backup/refresh/index publisher
    /// or default consumer is enabled by opening this connection.
    /// Explicit bounded default-task authority. Starts no consumer. Other task
    /// families remain refused; callers own family drain before shared close.
    pub async fn connect_defaults(
        broker: &str,
        result: &str,
        config: Config,
    ) -> Result<Self, Error> {
        Self::connect_owned(
            broker,
            result,
            config,
            String::new(),
            Authority::DefaultServices,
        )
        .await
    }
    pub(crate) fn allows_defaults(&self) -> bool {
        matches!(
            self.inner.authority,
            Authority::Fixture | Authority::DefaultServices
        )
    }
    pub async fn connect_series(broker: &str, result: &str, config: Config) -> Result<Self, Error> {
        Self::connect_owned(broker, result, config, String::new(), Authority::Series).await
    }
    pub(crate) fn allows_series(&self) -> bool {
        matches!(self.inner.authority, Authority::Fixture | Authority::Series)
    }
    async fn connect_owned(
        broker: &str,
        result: &str,
        config: Config,
        database: String,
        authority: Authority,
    ) -> Result<Self, Error> {
        if broker.len() > 4096
            || result.len() > 4096
            || config.operations == 0
            || config.operations > 16
            || config.command_timeout.is_zero()
            || config.command_timeout > Duration::from_secs(30)
            || config.max_queued_jobs == 0
            || config.max_queued_jobs > 10000
        {
            return Err(Error::Configuration);
        }
        redis::Client::open(broker).map_err(|_| Error::Configuration)?;
        redis::Client::open(result).map_err(|_| Error::Configuration)?;
        let options = TransportConfig {
            max_operations: config.operations,
            max_queued_operations: config.operations,
            command_timeout: config.command_timeout,
        };
        let broker = RedisTransport::connect_classified(broker, options.clone())
            .await
            .map_err(map)?;
        let result = RedisTransport::connect_classified(result, options)
            .await
            .map_err(map)?;
        Ok(Self {
            inner: Arc::new(Inner {
                broker,
                result,
                slots: Arc::new(Semaphore::new(config.operations)),
                closed: AtomicBool::new(false),
                config,
                database,
                authority,
                eta: std::sync::Mutex::new(None),
                cleanup_receipts: std::sync::Mutex::new(None),
            }),
        })
    }
    fn lease(&self, caller: Arc<dyn Send + Sync>) -> Result<Lease, Error> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let slot = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        Ok(Lease {
            _slot: slot,
            _caller: caller,
        })
    }
    pub(crate) fn eta_pool(
        &self,
        limits: crate::eta::Limits,
    ) -> Result<Arc<crate::eta::Pool>, Error> {
        if matches!(
            self.inner.authority,
            Authority::Series | Authority::DefaultServices | Authority::Backup
        ) {
            return Err(Error::UnsupportedTask);
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let mut existing = self.inner.eta.lock().unwrap();
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if let Some(pool) = existing.as_ref() {
            if pool.limits != limits {
                return Err(Error::Configuration);
            };
            return Ok(pool.clone());
        }
        let pool = Arc::new(crate::eta::Pool::new(limits)?);
        *existing = Some(pool.clone());
        Ok(pool)
    }
    pub fn eta_available_slots(&self) -> Option<usize> {
        self.inner
            .eta
            .lock()
            .unwrap()
            .as_ref()
            .map(|pool| pool.count.available_permits())
    }
    pub(crate) fn owns_reservation(
        &self,
        reservation: &crate::eta::Reservation,
    ) -> Result<(), Error> {
        if self
            .inner
            .eta
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|pool| Arc::ptr_eq(pool, &reservation.pool))
        {
            Ok(())
        } else {
            Err(Error::Configuration)
        }
    }
    fn cleanup_lease(&self, caller: Arc<dyn Send + Sync>) -> Result<Lease, Error> {
        let slot = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        Ok(Lease {
            _slot: slot,
            _caller: caller,
        })
    }
    pub fn available_slots(&self) -> usize {
        self.inner.slots.available_permits()
    }
    pub fn copied_database(&self) -> Option<&str> {
        (self.inner.authority == Authority::Fixture).then_some(self.inner.database.as_str())
    }
    pub async fn publish(
        &self,
        task: &Task,
        id: Uuid,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        if !self.allows_backup()
            || (self.inner.authority == Authority::Backup && !matches!(task, Task::CreateBackup))
        {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.lease(caller)?;
        let bytes = protocol::encode(
            task,
            id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "rust-probe@native-component",
        )
        .map_err(|_| Error::Protocol)?;
        let limit = self.inner.config.max_queued_jobs.to_string();
        let keys = vec!["celery".to_owned(), "_kombu.binding.celery".to_owned()];
        let value = self
            .inner
            .broker
            .command_classified::<i64, _>(PUBLISH, &keys, &[&bytes, limit.as_bytes()], lease)
            .await;
        match value {
            Ok((n, _lease)) if n > 0 => Ok(Published {
                task_id: id,
                queue_length: n as u64,
            }),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(id)),
            Err(e) => Err(map(e)),
        }
    }
    /// Typed refresh publication. Index-only connections never gain another
    /// family merely because refresh support is compiled.
    pub async fn publish_refresh(
        &self,
        message: &crate::refresh_protocol::RefreshMessage,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        if !self.allows_refresh() {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.lease(caller)?;
        let bytes = crate::refresh_protocol::encode(
            message,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "rust-probe@refresh-component",
        )
        .map_err(|_| Error::Protocol)?;
        if crate::refresh_protocol::parse(&bytes).map_err(|_| Error::Protocol)? != *message {
            return Err(Error::Protocol);
        }
        let id = message.task_id;
        let limit = self.inner.config.max_queued_jobs.to_string();
        let keys = vec!["celery".to_owned(), "_kombu.binding.celery".to_owned()];
        match self
            .inner
            .broker
            .command_classified::<i64, _>(PUBLISH, &keys, &[&bytes, limit.as_bytes()], lease)
            .await
        {
            Ok((n, _held)) if n > 0 => Ok(Published {
                task_id: id,
                queue_length: n as u64,
            }),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(id)),
            Err(error) => Err(map(error)),
        }
    }
    pub async fn read_refresh(
        &self,
        id: Uuid,
        source_id: i64,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Option<crate::refresh_result::RefreshSuccess>, Error> {
        if !self.allows_refresh() {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.lease(caller)?;
        let keys = vec![format!("celery-tasks:celery-task-meta-{id}")];
        let (reply, _held) = self
            .inner
            .result
            .command_classified::<(i64, Vec<u8>), _>(READ, &keys, &[], lease)
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => crate::refresh_result::parse_success(&reply.1, id, source_id)
                .map(Some)
                .map_err(|_| Error::Protocol),
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) async fn publish_refresh_child_fixture(
        &self,
        message: &crate::refresh_dispatch::ChildMessage,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        if !self.allows_refresh() {
            return Err(Error::UnsupportedTask);
        }
        // Manual refresh deferral uses the independently tested refresh helper.
        if matches!(
            message.child,
            crate::refresh_dispatch::ChildPublication::Refresh { .. }
        ) {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.lease(caller)?;
        let bytes = crate::refresh_dispatch::encode_child(
            &message.child,
            message.id,
            message.root,
            message.parent,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "rust-probe@refresh-child",
        )
        .map_err(|_| Error::Protocol)?;
        crate::refresh_dispatch::parse_native_child(&bytes).map_err(|_| Error::Protocol)?;
        let keys = vec!["celery".to_owned(), "_kombu.binding.celery".to_owned()];
        let limit = self.inner.config.max_queued_jobs.to_string();
        match self
            .inner
            .broker
            .command_classified::<i64, _>(PUBLISH, &keys, &[&bytes, limit.as_bytes()], lease)
            .await
        {
            Ok((n, _)) if n > 0 => Ok(Published {
                task_id: message.id,
                queue_length: n as u64,
            }),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(message.id)),
            Err(error) => Err(map(error)),
        }
    }
    pub(crate) async fn claim_refresh_fixture(
        &self,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Option<crate::refresh_protocol::RefreshMessage>, Error> {
        if !self.allows_refresh() {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.lease(caller.clone())?;
        let keys = vec!["celery".to_owned()];
        let (reply, held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(PEEK_CREATE, &keys, &[], lease)
            .await
            .map_err(map)?;
        if reply.0 == 0 {
            return Ok(None);
        }
        if reply.0 != 1 {
            return Err(Error::Corrupt);
        }
        let message =
            crate::refresh_protocol::parse(&reply.1).map_err(|_| Error::UnsupportedTask)?;
        if message.eta.is_some() || message.parent.is_some() {
            return Err(Error::UnsupportedTask);
        }
        drop(held);
        let lease = self.lease(caller)?;
        match self
            .inner
            .broker
            .command_classified::<i64, _>(CLAIM_CREATE, &keys, &[&reply.1], lease)
            .await
        {
            Ok((1, _)) => Ok(Some(message)),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(message.task_id)),
            Err(error) => Err(map(error)),
        }
    }
    pub(crate) async fn write_refresh_success_fixture(
        &self,
        message: &crate::refresh_protocol::RefreshMessage,
        result: crate::refresh_worker::NormalReturn,
        children: Vec<Uuid>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !self.allows_refresh() {
            return Err(Error::UnsupportedTask);
        }
        if message.eta.is_some()
            || message.parent.is_some()
            || message.source_id <= 0
            || children.len() > 64
        {
            return Err(Error::Protocol);
        }
        let value = match result {
            crate::refresh_worker::NormalReturn::Null => serde_json::Value::Null,
            crate::refresh_worker::NormalReturn::MissingSource => serde_json::json!(format!(
                "EPG source {} does not exist, task cleaned up",
                message.source_id
            )),
        };
        self.write_result(message.task_id, value, None, children, caller)
            .await
    }
    /// Fixture-scoped Series preflight: no pop or unrelated-family transfer.
    pub(crate) async fn peek_series(
        &self,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Option<(Vec<u8>, crate::series_protocol::SeriesMessage)>, Error> {
        if !self.allows_series() {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.lease(caller)?;
        let keys = vec!["celery".to_owned()];
        let (reply, _held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(PEEK_CREATE, &keys, &[], lease)
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => {
                let message =
                    crate::series_protocol::parse(&reply.1).map_err(|_| Error::UnsupportedTask)?;
                Ok(Some((reply.1, message)))
            }
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) async fn claim_series(
        &self,
        bytes: &[u8],
        caller: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if !self.allows_series() {
            return Err(Error::UnsupportedTask);
        }
        let message = crate::series_protocol::parse(bytes).map_err(|_| Error::UnsupportedTask)?;
        let lease = self.lease(caller)?;
        match self
            .inner
            .broker
            .command_classified::<i64, _>(CLAIM_CREATE, &["celery".to_owned()], &[bytes], lease)
            .await
        {
            Ok((1, _)) => Ok(()),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(message.task_id)),
            Err(error) => Err(map(error)),
        }
    }
    pub(crate) async fn write_series_success(
        &self,
        message: &crate::series_protocol::SeriesMessage,
        result: crate::series_protocol::KnownReturn,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !self.allows_series() {
            return Err(Error::UnsupportedTask);
        }
        if message.parent.is_none() && message.root != message.task_id {
            return Err(Error::Protocol);
        }
        self.write_result(
            message.task_id,
            result.value(),
            message.parent,
            Vec::new(),
            caller,
        )
        .await
    }
    pub async fn publish_index(
        &self,
        task: &crate::index_protocol::IndexTask,
        id: Uuid,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        if matches!(
            self.inner.authority,
            Authority::Series | Authority::DefaultServices | Authority::Backup
        ) {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.lease(caller)?;
        let bytes = crate::index_protocol::encode(
            task,
            id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "rust-probe@native-index",
        )
        .map_err(|_| Error::Protocol)?;
        let limit = self.inner.config.max_queued_jobs.to_string();
        let keys = vec!["celery".to_owned(), "_kombu.binding.celery".to_owned()];
        match self
            .inner
            .broker
            .command_classified::<i64, _>(PUBLISH, &keys, &[&bytes, limit.as_bytes()], lease)
            .await
        {
            Ok((n, _)) if n > 0 => Ok(Published {
                task_id: id,
                queue_length: n as u64,
            }),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(id)),
            Err(e) => Err(map(e)),
        }
    }
    pub async fn read_index(
        &self,
        id: Uuid,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Option<crate::index_protocol::IndexResult>, Error> {
        if matches!(
            self.inner.authority,
            Authority::Series | Authority::DefaultServices | Authority::Backup
        ) {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.lease(caller)?;
        let keys = vec![format!("celery-tasks:celery-task-meta-{id}")];
        let (reply, _held) = self
            .inner
            .result
            .command_classified::<(i64, Vec<u8>), _>(READ, &keys, &[], lease)
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => crate::index_protocol::parse_result(&reply.1, id)
                .map(Some)
                .map_err(|_| Error::Protocol),
            _ => Err(Error::Corrupt),
        }
    }
    pub async fn read(
        &self,
        id: Uuid,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Option<Metadata>, Error> {
        let lease = self.lease(caller)?;
        let keys = vec![format!("celery-tasks:celery-task-meta-{id}")];
        let (reply, _lease) = self
            .inner
            .result
            .command_classified::<(i64, Vec<u8>), _>(READ, &keys, &[], lease)
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => decode::decode(&reply.1, id)
                .map(Some)
                .map_err(Error::Decode),
            _ => Err(Error::Corrupt),
        }
    }
    /// Source accepts opaque task IDs. Read the exact key even when an ID is
    /// not a UUID; absent values remain PENDING. Existing non-UUID result
    /// payload grammar is deliberately unsupported rather than fabricated.
    pub async fn read_backup(
        &self,
        id: &str,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Option<Metadata>, Error> {
        if !self.allows_backup() {
            return Err(Error::UnsupportedTask);
        }
        if id.is_empty() || id.len() > 1024 || id.contains('\0') {
            return Err(Error::Protocol);
        }
        let lease = self.lease(caller)?;
        let keys = vec![format!("celery-tasks:celery-task-meta-{id}")];
        let (reply, _lease) = self
            .inner
            .result
            .command_classified::<(i64, Vec<u8>), _>(READ, &keys, &[], lease)
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => {
                let expected = Uuid::parse_str(id).map_err(|_| Error::UnsupportedTask)?;
                decode::decode(&reply.1, expected)
                    .map(Some)
                    .map_err(Error::Decode)
            }
            _ => Err(Error::Corrupt),
        }
    }
    /// Reserve only captured manual index ETA children. Count/tuple-byte limits
    /// cover the shared Source unacked namespace, including foreign reservations.
    pub async fn peek_index_candidate(
        &self,
        caller: Arc<dyn Send + Sync>,
        limits: crate::eta::Limits,
    ) -> Result<Option<crate::eta::Candidate>, Error> {
        if matches!(
            self.inner.authority,
            Authority::Series | Authority::DefaultServices | Authority::Backup
        ) {
            return Err(Error::UnsupportedTask);
        }
        let pool = self.eta_pool(limits)?;
        let budget = Arc::new(pool.acquire()?);
        let guard = Arc::new((caller, budget.clone()));
        let lease = self.lease(guard)?;
        let keys = vec!["celery".to_owned()];
        let (reply, _held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(PEEK_CREATE, &keys, &[], lease)
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => crate::eta::Receipt::prepare(&reply.1)
                .map(|receipt| {
                    Some(crate::eta::Candidate {
                        receipt: Arc::new(receipt),
                        budget,
                        pool,
                    })
                })
                .map_err(|_| Error::UnsupportedTask),
            _ => Err(Error::Corrupt),
        }
    }
    pub async fn reserve_index_candidate(
        &self,
        candidate: crate::eta::Candidate,
        caller: Arc<dyn Send + Sync>,
        limits: crate::eta::Limits,
    ) -> Result<crate::eta::Reservation, Error> {
        if matches!(
            self.inner.authority,
            Authority::Series | Authority::DefaultServices | Authority::Backup
        ) {
            return Err(Error::UnsupportedTask);
        }
        let pool = self.eta_pool(limits)?;
        if !Arc::ptr_eq(&pool, &candidate.pool) {
            return Err(Error::Configuration);
        }
        let budget = candidate.budget.clone();
        self.reserve_prepared(candidate, caller, limits, budget)
            .await
    }
    /// Driver-only supervised reserve: never execute on an ambiguous logical
    /// timeout. Keep the same frame credit, wait for the physical command, then
    /// reconcile the exact owner tuple before handing a receipt to execution.
    /// The intake supervisor must retain/poll this future through shutdown.
    pub async fn reserve_index_candidate_settled(
        &self,
        candidate: crate::eta::Candidate,
        caller: Arc<dyn Send + Sync>,
        limits: crate::eta::Limits,
    ) -> Result<crate::eta::Reservation, Error> {
        if matches!(
            self.inner.authority,
            Authority::Series | Authority::DefaultServices | Authority::Backup
        ) {
            return Err(Error::UnsupportedTask);
        }
        let expected = crate::eta::Reservation::new_shared(
            candidate.receipt.clone(),
            candidate.budget.clone(),
            candidate.pool.clone(),
        );
        let (done, completed) = tokio::sync::oneshot::channel();
        let physical: Arc<dyn Send + Sync> =
            Arc::new((caller.clone(), PhysicalCompletion(Some(done))));
        match self
            .reserve_index_candidate(candidate, physical, limits)
            .await
        {
            Err(Error::AmbiguousReservation { .. }) => {
                // Completion closes only when the actual command drops its
                // unique guard, including after a detached timeout.
                let _ = completed.await;
                loop {
                    match self.reservation_present(&expected, caller.clone()).await {
                        Ok(true) => return Ok(expected),
                        Ok(false) => return Err(Error::ClaimRace),
                        Err(Error::Backend | Error::Capacity) => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            result => result,
        }
    }
    async fn reservation_present(
        &self,
        reservation: &crate::eta::Reservation,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        self.owns_reservation(reservation)?;
        let lease = self.cleanup_lease(Arc::new((caller, reservation.clone())))?;
        let keys = vec!["unacked".to_owned(), "unacked_index".to_owned()];
        let tag = reservation.delivery_tag().to_string();
        let (reply, _held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(READ_ETA, &keys, &[tag.as_bytes()], lease)
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(false),
            1 if reply.1 == reservation.tuple_bytes() => Ok(true),
            1 => Err(Error::LostReservation),
            _ => Err(Error::Corrupt),
        }
    }
    #[cfg(test)]
    pub(crate) async fn broker_settled<T: redis::FromRedisValue + Send + 'static>(
        &self,
        source: &str,
        keys: &[String],
        args: &[&[u8]],
        caller: Arc<dyn Send + Sync>,
    ) -> Result<T, Error> {
        let (done, completed) = tokio::sync::oneshot::channel();
        let guard: Arc<dyn Send + Sync> = Arc::new((caller, PhysicalCompletion(Some(done))));
        let lease = self.cleanup_lease(guard)?;
        match self
            .inner
            .broker
            .command_classified::<T, _>(source, keys, args, lease)
            .await
        {
            Ok((value, _held)) => Ok(value),
            Err(ClassifiedError::Backend) => {
                let _ = completed.await;
                Err(Error::Backend)
            }
            Err(error) => Err(map(error)),
        }
    }
    #[cfg(test)]
    pub(crate) async fn restore_visible_owned(
        &self,
        receipt: &crate::eta::Reservation,
        score: &str,
        ceiling: &str,
        token: &str,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        self.owns_reservation(receipt)?;
        let tag = receipt.delivery_tag().to_string();
        let keys = vec![
            "celery".to_owned(),
            "unacked".to_owned(),
            "unacked_index".to_owned(),
            "unacked_mutex".to_owned(),
        ];
        let caller: Arc<dyn Send + Sync> = Arc::new((owner, receipt.clone()));
        match self
            .broker_settled::<i64>(
                crate::visibility::RESTORE_VISIBLE,
                &keys,
                &[
                    tag.as_bytes(),
                    receipt.tuple_bytes(),
                    &receipt.receipt.restored_bytes,
                    score.as_bytes(),
                    ceiling.as_bytes(),
                    token.as_bytes(),
                ],
                caller,
            )
            .await
        {
            Ok(1) => Ok(true),
            Ok(0) => Ok(false),
            Ok(-5) => Err(Error::LostMutex),
            Ok(_) => Err(Error::Corrupt),
            Err(Error::Backend) => Err(Error::AmbiguousRestore(receipt.message().task_id)),
            Err(error) => Err(error),
        }
    }
    async fn reserve_prepared(
        &self,
        candidate: crate::eta::Candidate,
        caller: Arc<dyn Send + Sync>,
        limits: crate::eta::Limits,
        budget: Arc<crate::eta::Budget>,
    ) -> Result<crate::eta::Reservation, Error> {
        let reservation =
            crate::eta::Reservation::new_shared(candidate.receipt, budget, candidate.pool);
        let guard = Arc::new((caller, reservation.clone()));
        let lease = self.lease(guard)?;
        let keys = vec![
            "celery".to_owned(),
            "unacked".to_owned(),
            "unacked_index".to_owned(),
        ];
        let tag = reservation.delivery_tag().to_string();
        let score = format!(
            "{:.6}",
            chrono::Utc::now().timestamp_micros() as f64 / 1_000_000.0
        );
        let count = limits.max_pending.to_string();
        let bytes = limits.max_pending_bytes.to_string();
        let r = &reservation.receipt;
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                crate::eta::RESERVE,
                &keys,
                &[
                    &r.queued_bytes,
                    tag.as_bytes(),
                    &r.tuple_bytes,
                    score.as_bytes(),
                    count.as_bytes(),
                    bytes.as_bytes(),
                ],
                lease,
            )
            .await
        {
            Ok((1, _)) => Ok(reservation),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok((-4, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousReservation {
                task_id: reservation.message().task_id,
                delivery_tag: reservation.delivery_tag(),
            }),
            Err(e) => Err(map(e)),
        }
    }
    pub async fn reserve_index_eta(
        &self,
        caller: Arc<dyn Send + Sync>,
        limits: crate::eta::Limits,
    ) -> Result<Option<crate::eta::Reservation>, Error> {
        if matches!(
            self.inner.authority,
            Authority::Series | Authority::DefaultServices | Authority::Backup
        ) {
            return Err(Error::UnsupportedTask);
        }
        let Some(candidate) = self.peek_index_candidate(caller.clone(), limits).await? else {
            return Ok(None);
        };
        if candidate.message().eta.is_none() {
            return Err(Error::UnsupportedTask);
        }
        self.reserve_index_candidate(candidate, caller, limits)
            .await
            .map(Some)
    }
    pub async fn read_index_eta(
        &self,
        tag: Uuid,
        caller: Arc<dyn Send + Sync>,
        limits: crate::eta::Limits,
    ) -> Result<Option<crate::eta::Reservation>, Error> {
        if matches!(
            self.inner.authority,
            Authority::Series | Authority::DefaultServices | Authority::Backup
        ) {
            return Err(Error::UnsupportedTask);
        }
        let pool = self.eta_pool(limits)?;
        let budget = Arc::new(pool.acquire()?);
        let guard = Arc::new((caller, budget.clone()));
        let lease = self.lease(guard)?;
        let keys = vec!["unacked".to_owned(), "unacked_index".to_owned()];
        let field = tag.to_string();
        let (reply, _held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(READ_ETA, &keys, &[field.as_bytes()], lease)
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => crate::eta::Receipt::from_stored(&reply.1, tag)
                .map(|r| Some(crate::eta::Reservation::new(r, budget, pool)))
                .map_err(|_| Error::Corrupt),
            _ => Err(Error::Corrupt),
        }
    }
    /// Cleanup remains permitted after close: a failed final drain cannot strand
    /// an admitted reservation by forbidding its owned ACK/restore operations.
    pub async fn ack_index_eta(
        &self,
        reservation: &crate::eta::Reservation,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if matches!(
            self.inner.authority,
            Authority::Series | Authority::DefaultServices | Authority::Backup
        ) {
            return Err(Error::UnsupportedTask);
        }
        self.owns_reservation(reservation)?;
        let lease = self.cleanup_lease(Arc::new((caller, reservation.clone())))?;
        let tag = reservation.delivery_tag().to_string();
        let keys = vec![
            "celery".to_owned(),
            "unacked".to_owned(),
            "unacked_index".to_owned(),
        ];
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                crate::eta::ACK,
                &keys,
                &[tag.as_bytes(), reservation.tuple_bytes()],
                lease,
            )
            .await
        {
            Ok((1, _)) => Ok(()),
            Ok((0, _)) => Err(Error::LostReservation),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => {
                Err(Error::AmbiguousAck(reservation.message().task_id))
            }
            Err(e) => Err(map(e)),
        }
    }
    pub async fn restore_index_eta(
        &self,
        reservation: &crate::eta::Reservation,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if matches!(
            self.inner.authority,
            Authority::Series | Authority::DefaultServices | Authority::Backup
        ) {
            return Err(Error::UnsupportedTask);
        }
        self.owns_reservation(reservation)?;
        let lease = self.cleanup_lease(Arc::new((caller, reservation.clone())))?;
        let tag = reservation.delivery_tag().to_string();
        let keys = vec![
            "celery".to_owned(),
            "unacked".to_owned(),
            "unacked_index".to_owned(),
        ];
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                crate::eta::RESTORE,
                &keys,
                &[
                    tag.as_bytes(),
                    reservation.tuple_bytes(),
                    &reservation.receipt.restored_bytes,
                ],
                lease,
            )
            .await
        {
            Ok((1, _)) => Ok(()),
            Ok((0, _)) => Err(Error::LostReservation),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => {
                Err(Error::AmbiguousRestore(reservation.message().task_id))
            }
            Err(e) => Err(map(e)),
        }
    }
    pub(crate) async fn claim_create(
        &self,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Option<Uuid>, Error> {
        if !self.allows_backup() {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.lease(caller.clone())?;
        let keys = vec!["celery".to_owned()];
        let (reply, held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(PEEK_CREATE, &keys, &[], lease)
            .await
            .map_err(map)?;
        if reply.0 == 0 {
            return Ok(None);
        }
        if reply.0 != 1 {
            return Err(Error::Corrupt);
        }
        let id = crate::worker::create_identity(&reply.1).map_err(|_| Error::UnsupportedTask)?;
        drop(held);
        let lease = self.lease(caller)?;
        match self
            .inner
            .broker
            .command_classified::<i64, _>(CLAIM_CREATE, &keys, &[&reply.1], lease)
            .await
        {
            Ok((1, _)) => Ok(Some(id)),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(id)),
            Err(e) => Err(map(e)),
        }
    }
    pub(crate) async fn claim_index(
        &self,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Option<crate::index_protocol::IndexMessage>, Error> {
        if matches!(
            self.inner.authority,
            Authority::Series | Authority::DefaultServices | Authority::Backup
        ) {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.lease(caller.clone())?;
        let keys = vec!["celery".to_owned()];
        let (reply, held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(PEEK_CREATE, &keys, &[], lease)
            .await
            .map_err(map)?;
        if reply.0 == 0 {
            return Ok(None);
        }
        if reply.0 != 1 {
            return Err(Error::Corrupt);
        }
        let message = crate::index_protocol::parse(&reply.1).map_err(|_| Error::UnsupportedTask)?;
        if message.eta.is_some() || message.parent.is_some() {
            return Err(Error::UnsupportedTask);
        }
        drop(held);
        let lease = self.lease(caller)?;
        match self
            .inner
            .broker
            .command_classified::<i64, _>(CLAIM_CREATE, &keys, &[&reply.1], lease)
            .await
        {
            Ok((1, _)) => Ok(Some(message)),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(message.task_id)),
            Err(e) => Err(map(e)),
        }
    }
    pub(crate) async fn write_index_success(
        &self,
        id: Uuid,
        parent: Option<Uuid>,
        children: Vec<Uuid>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if matches!(
            self.inner.authority,
            Authority::Series | Authority::DefaultServices | Authority::Backup
        ) {
            return Err(Error::UnsupportedTask);
        }
        if children.len() > 1 {
            return Err(Error::Protocol);
        }
        self.write_result(id, serde_json::Value::Null, parent, children, caller)
            .await
    }
    pub(crate) async fn write_success(
        &self,
        id: Uuid,
        result: serde_json::Value,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        self.write_result(id, result, None, Vec::new(), caller)
            .await
    }
    pub(crate) async fn write_index_terminal(
        &self,
        id: Uuid,
        parent: Option<Uuid>,
        children: Vec<Uuid>,
        terminal: crate::index_terminal::Terminal,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if matches!(
            self.inner.authority,
            Authority::Series | Authority::DefaultServices | Authority::Backup
        ) {
            return Err(Error::UnsupportedTask);
        }
        match terminal {
            crate::index_terminal::Terminal::NullSuccess => {
                self.write_index_success(id, parent, children, caller).await
            }
            crate::index_terminal::Terminal::Raised(failure) => {
                let date = chrono::Utc::now()
                    .format("%Y-%m-%dT%H:%M:%S%.6f+00:00")
                    .to_string();
                let bytes = failure
                    .metadata(id, parent, &children, &date)
                    .map_err(|_| Error::Protocol)?;
                self.write_metadata(id, bytes, caller).await
            }
        }
    }
    async fn write_result(
        &self,
        id: Uuid,
        result: serde_json::Value,
        parent: Option<Uuid>,
        children: Vec<Uuid>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        let child_tuples: Vec<serde_json::Value> = children
            .into_iter()
            .map(|child| serde_json::json!([[child.to_string(), null], null]))
            .collect();
        let mut meta = serde_json::json!({"status":"SUCCESS", "result":result, "traceback":null,
            "children":child_tuples, "date_done":chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string()});
        if let Some(parent) = parent {
            meta["parent_id"] = serde_json::json!(parent.to_string());
        }
        meta["task_id"] = serde_json::json!(id.to_string());
        let bytes = serde_json::to_vec(&meta).map_err(|_| Error::Protocol)?;
        if bytes.len() > decode::MAX_RESULT_BYTES {
            return Err(Error::Protocol);
        }
        self.write_metadata(id, bytes, caller).await
    }
    #[cfg(test)]
    pub(crate) async fn write_observed_index_failure(
        &self,
        expected: Uuid,
        failure: &crate::index_failure::ObservedFailure,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if expected != failure.task_id {
            return Err(Error::Protocol);
        }
        let bytes = failure.encode().map_err(|_| Error::Protocol)?;
        self.write_metadata(expected, bytes, caller).await
    }
    async fn write_metadata(
        &self,
        id: Uuid,
        bytes: Vec<u8>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if bytes.len() > decode::MAX_RESULT_BYTES {
            return Err(Error::Protocol);
        }
        let lease = self.lease(caller)?;
        let keys = vec![format!("celery-tasks:celery-task-meta-{id}")];
        match self
            .inner
            .result
            .command_classified::<i64, _>(WRITE_SUCCESS, &keys, &[&bytes], lease)
            .await
        {
            Ok((1, _)) => Ok(true),
            Ok((0, _)) => Ok(false),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousResult(id)),
            Err(e) => Err(map(e)),
        }
    }
    /// A deadline is an explicit failed drain, never proof that physical owners
    /// have been released. Detached commands retain their Lease until reply/drop.
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.inner.closed.store(true, Ordering::Release);
        let deadline = tokio::time::Instant::now() + timeout;
        let eta = self.inner.eta.lock().unwrap().clone();
        let _eta = if let Some(pool) = eta {
            Some(
                tokio::time::timeout(
                    deadline.saturating_duration_since(tokio::time::Instant::now()),
                    pool.count
                        .clone()
                        .acquire_many_owned(pool.limits.max_pending as u32),
                )
                .await
                .map_err(|_| Error::DrainTimeout)?
                .map_err(|_| Error::Backend)?,
            )
        } else {
            None
        };
        let cleanup = self
            .inner
            .cleanup_receipts
            .lock()
            .map_err(|_| Error::Backend)?
            .clone();
        let _cleanup = if let Some(pool) = cleanup {
            Some(
                tokio::time::timeout(
                    deadline.saturating_duration_since(tokio::time::Instant::now()),
                    pool.count.clone().acquire_many_owned(pool.maximum as u32),
                )
                .await
                .map_err(|_| Error::DrainTimeout)?
                .map_err(|_| Error::Backend)?,
            )
        } else {
            None
        };
        let all = self
            .inner
            .slots
            .clone()
            .acquire_many_owned(self.inner.config.operations as u32);
        let _held = tokio::time::timeout(
            deadline.saturating_duration_since(tokio::time::Instant::now()),
            all,
        )
        .await
        .map_err(|_| Error::DrainTimeout)?
        .map_err(|_| Error::Backend)?;
        Ok(())
    }
}
fn map(error: ClassifiedError) -> Error {
    match error {
        ClassifiedError::Configuration => Error::Configuration,
        ClassifiedError::Capacity => Error::Capacity,
        ClassifiedError::Backend => Error::Backend,
        ClassifiedError::Server => Error::Corrupt,
    }
}
const PUBLISH: &str = r#"
local q=redis.call('TYPE',KEYS[1]).ok;local b=redis.call('TYPE',KEYS[2]).ok
if(q~='none'and q~='list')or(b~='none'and b~='set')then return -3 end
if #ARGV[1]>65536 then return -1 end
if redis.call('LLEN',KEYS[1])>=tonumber(ARGV[2])then return -2 end
redis.call('SADD',KEYS[2],'celery'..string.char(6,22)..string.char(6,22)..'celery')
return redis.call('LPUSH',KEYS[1],ARGV[1])
"#;
const READ: &str = r#"
local t=redis.call('TYPE',KEYS[1]).ok
if t=='none'then return{0,''}end
if t~='string'or redis.call('STRLEN',KEYS[1])>262144 then return{-1,''}end
return{1,redis.call('GET',KEYS[1])}
"#;

// Peek never reserves/reorders another task family; compare/pop is the fixture
// early-ACK boundary. General Kombu unacked recovery is intentionally absent.
const PEEK_CREATE: &str = r#"
local t=redis.call('TYPE',KEYS[1]).ok
if t=='none'then return{0,''}end
if t~='list'or redis.call('LLEN',KEYS[1])>10000 then return{-1,''}end
local raw=redis.call('LINDEX',KEYS[1],-1)
if not raw then return{0,''}end
if #raw>65536 then return{-1,''}end
return{1,raw}
"#;
const CLAIM_CREATE: &str = r#"
if redis.call('TYPE',KEYS[1]).ok~='list'then return 0 end
if redis.call('LINDEX',KEYS[1],-1)~=ARGV[1]then return 0 end
redis.call('RPOP',KEYS[1]);return 1
"#;
const WRITE_SUCCESS: &str = r#"
local t=redis.call('TYPE',KEYS[1]).ok
if t~='none'and t~='string'then return -1 end
if #ARGV[1]>262144 then return -1 end
if t=='string'then
 if redis.call('STRLEN',KEYS[1])>262144 then return -1 end
 local ok,old=pcall(cjson.decode,redis.call('GET',KEYS[1]))
 if not ok or type(old)~='table'then return -1 end
 if old.status=='SUCCESS'then return 0 end
end
redis.call('SETEX',KEYS[1],3600,ARGV[1]);redis.call('PUBLISH',KEYS[1],ARGV[1]);return 1
"#;

const READ_ETA: &str = r#"
local h=redis.call('TYPE',KEYS[1]).ok;local z=redis.call('TYPE',KEYS[2]).ok
if h=='none'and z=='none'then return{0,''}end
if h~='hash'or z~='zset'then return{-1,''}end
if redis.call('HSTRLEN',KEYS[1],ARGV[1])>131072 then return{-1,''}end
local raw=redis.call('HGET',KEYS[1],ARGV[1]);if not raw then return{0,''}end
if not redis.call('ZSCORE',KEYS[2],ARGV[1])then return{-1,''}end
return{1,raw}
"#;

#[path = "refresh_eta_transport.rs"]
mod refresh_eta_transport;

#[path = "scheduler_transport.rs"]
mod scheduler_transport;

// Private fixture-only two-family draft.
#[path = "default_transport.rs"]
mod default_transport;

#[path = "cleanup_reservation_transport.rs"]
mod cleanup_reservation_transport;
pub(crate) use cleanup_reservation_transport::Reservation as CleanupReservation;

#[path = "scanner_files_io.rs"]
mod scanner_files_io;
pub use scanner_files_io::{ScannerFilesPorts, ScannerIoError, ScannerRoute, ScannerServer};

#[path = "scanner_refresh_io.rs"]
mod scanner_refresh_io;

#[path = "scanner_m3u_io.rs"]
mod scanner_m3u_io;
pub use scanner_m3u_io::M3uPorts;
