//! Fixture-authorized publisher/result reader; no consumer or application route.
//! Original Dispatcharr authors retain application credit and AGPL licensing.
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
struct Inner {
    broker: RedisTransport,
    result: RedisTransport,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
    config: Config,
    database: String,
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
                database: database.to_owned(),
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
    pub fn available_slots(&self) -> usize {
        self.inner.slots.available_permits()
    }
    pub fn copied_database(&self) -> &str {
        &self.inner.database
    }
    pub async fn publish(
        &self,
        task: &Task,
        id: Uuid,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
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
    pub async fn publish_index(
        &self,
        task: &crate::index_protocol::IndexTask,
        id: Uuid,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
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
    pub(crate) async fn claim_create(
        &self,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Option<Uuid>, Error> {
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
    async fn write_result(
        &self,
        id: Uuid,
        result: serde_json::Value,
        parent: Option<Uuid>,
        children: Vec<Uuid>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        let lease = self.lease(caller)?;
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
        let all = self
            .inner
            .slots
            .clone()
            .acquire_many_owned(self.inner.config.operations as u32);
        let _held = tokio::time::timeout(timeout, all)
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
