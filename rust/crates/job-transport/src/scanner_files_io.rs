//! Scanner-only physical ports. Legacy Jobs/default/index intake is unchanged.
//! Original Dispatcharr/Celery/Kombu/Redis authors retain behavior/code credit.
use super::*;
use crate::{
    default_protocol::{self, Kind},
    scanner_files_protocol,
};
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use serde_json::{json, Value};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScannerRoute {
    Default,
    OwnedFixtureParent,
}
impl ScannerRoute {
    fn name(self) -> &'static str {
        match self {
            Self::Default => "celery",
            Self::OwnedFixtureParent => "rust-owned-scanner-parents",
        }
    }
}
#[derive(Clone, Debug)]
pub struct ScannerServer {
    pub code: String,
    pub detail: String,
}
#[derive(Debug)]
pub enum ScannerIoError {
    HeadChanged,
    Operational,
    Unsupported,
    Capacity,
    Transport(Error),
    Server(ScannerServer),
}
fn known(error: redis::RedisError) -> ScannerIoError {
    match (error.code(), error.detail()) {
        (Some(code), Some(detail)) if code.len() <= 64 && detail.len() <= 8192 => {
            ScannerIoError::Server(ScannerServer {
                code: code.into(),
                detail: detail.into(),
            })
        }
        _ => ScannerIoError::Operational,
    }
}
struct PortsInner {
    jobs: Jobs,
    broker: ConnectionManager,
    cache: ConnectionManager,
    markers: RedisTransport,
    route: ScannerRoute,
    reply: Uuid,
    attempts: std::sync::Mutex<Vec<Value>>,
    identity: Option<dispatcharr_store::ProducerIdentitySource>,
    committed: std::sync::Mutex<Vec<ScannerPublication>>,
}
/// Opaque capability: accepts only scanner parent frames and three observed
/// child names. It never grants old DefaultWorker/Index broader authority.
#[derive(Clone)]
pub struct ScannerFilesPorts {
    inner: Arc<PortsInner>,
}
#[derive(Clone, Copy)]
pub(crate) enum ScannerRoot {
    Scan(Uuid),
    CompatBeat(Uuid),
}
impl ScannerRoot {
    pub(crate) fn task_id(self) -> Uuid {
        match self {
            Self::Scan(task_id) | Self::CompatBeat(task_id) => task_id,
        }
    }
}
pub(crate) struct ScannerCandidate {
    pub root: ScannerRoot,
    bytes: Vec<u8>,
    held: Arc<Lease>,
    origin: Arc<Inner>,
    route: ScannerRoute,
}
impl ScannerCandidate {
    pub(crate) fn matches_raw(&self, expected: &[u8]) -> bool {
        self.bytes == expected
    }
}
impl ScannerFilesPorts {
    pub async fn connect(
        broker: &str,
        result: &str,
        cache: &str,
        route: ScannerRoute,
        config: Config,
    ) -> Result<Self, ScannerIoError> {
        if route == ScannerRoute::OwnedFixtureParent
            && (std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref() != Ok("playback-fixture-only")
                || !std::env::var("POSTGRES_DB")
                    .unwrap_or_default()
                    .starts_with("rust_probe_backup_jobs_"))
        {
            return Err(ScannerIoError::Unsupported);
        }
        let jobs = Jobs::connect_defaults(broker, result, config.clone())
            .await
            .map_err(ScannerIoError::Transport)?;
        let make = |url: &str| redis::Client::open(url).map_err(|_| ScannerIoError::Unsupported);
        let cache_client = make(cache)?;
        let info = cache_client.get_connection_info();
        if info.redis.db != 0 {
            return Err(ScannerIoError::Unsupported);
        }
        match info.addr {
            redis::ConnectionAddr::Tcp(ref host, _)
                if matches!(host.as_str(), "localhost" | "127.0.0.1" | "::1") => {}
            _ => return Err(ScannerIoError::Unsupported),
        }
        let manager = ConnectionManagerConfig::new()
            .set_number_of_retries(0)
            .set_connection_timeout(config.command_timeout);
        let broker = make(broker)?
            .get_connection_manager_with_config(manager.clone())
            .await
            .map_err(known)?;
        let cache_connection = cache_client
            .get_connection_manager_with_config(manager)
            .await
            .map_err(known)?;
        let markers = RedisTransport::connect(
            cache,
            TransportConfig {
                max_operations: config.operations,
                max_queued_operations: config.operations,
                command_timeout: config.command_timeout,
            },
        )
        .await
        .map_err(|_| ScannerIoError::Operational)?;
        Ok(Self {
            inner: Arc::new(PortsInner {
                jobs,
                broker,
                cache: cache_connection,
                markers,
                route,
                reply: Uuid::new_v4(),
                attempts: std::sync::Mutex::new(Vec::new()),
                identity: None,
                committed: std::sync::Mutex::new(Vec::new()),
            }),
        })
    }
    /// Normal shared queue/cache assembly: configured Redis addresses are not
    /// restricted to the historical loopback fixture, and no second Jobs exists.
    pub async fn connect_shared(
        jobs: Jobs,
        broker: &str,
        cache: &str,
        config: Config,
        identity: dispatcharr_store::ProducerIdentitySource,
    ) -> Result<Self, ScannerIoError> {
        let make = |url: &str| redis::Client::open(url).map_err(|_| ScannerIoError::Unsupported);
        let manager = ConnectionManagerConfig::new()
            .set_number_of_retries(0)
            .set_connection_timeout(config.command_timeout);
        let broker = make(broker)?
            .get_connection_manager_with_config(manager.clone())
            .await
            .map_err(known)?;
        let cache_connection = make(cache)?
            .get_connection_manager_with_config(manager)
            .await
            .map_err(known)?;
        let markers = RedisTransport::connect(
            cache,
            TransportConfig {
                max_operations: config.operations,
                max_queued_operations: config.operations,
                command_timeout: config.command_timeout,
            },
        )
        .await
        .map_err(|_| ScannerIoError::Operational)?;
        Ok(Self {
            inner: Arc::new(PortsInner {
                jobs,
                broker,
                cache: cache_connection,
                markers,
                route: ScannerRoute::Default,
                reply: Uuid::new_v4(),
                attempts: std::sync::Mutex::new(Vec::new()),
                identity: Some(identity),
                committed: std::sync::Mutex::new(Vec::new()),
            }),
        })
    }
    pub(crate) fn identity_source(&self) -> Option<dispatcharr_store::ProducerIdentitySource> {
        self.inner.identity.clone()
    }
    pub(crate) fn committed_publications(&self) -> Result<Vec<ScannerPublication>, ScannerIoError> {
        self.inner
            .committed
            .lock()
            .map(|v| v.clone())
            .map_err(|_| ScannerIoError::Operational)
    }
    pub(crate) async fn peek(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<ScannerCandidate>, ScannerIoError> {
        self.peek_expected(None, owner).await
    }
    pub(crate) async fn peek_expected(
        &self,
        expected: Option<Arc<[u8]>>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<ScannerCandidate>, ScannerIoError> {
        let jobs = &self.inner.jobs;
        let held = Arc::new(jobs.lease(owner).map_err(ScannerIoError::Transport)?);
        let ((state, bytes), _) = jobs
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(
                PEEK_CREATE,
                &[self.inner.route.name().into()],
                &[],
                held.clone(),
            )
            .await
            .map_err(|_| ScannerIoError::Operational)?;
        if expected
            .as_ref()
            .is_some_and(|selected| state != 1 || selected.as_ref() != bytes.as_slice())
        {
            return Err(ScannerIoError::HeadChanged);
        }
        if state == 0 {
            return Ok(None);
        }
        if state != 1 {
            return Err(ScannerIoError::Unsupported);
        }
        let compat = if expected.is_some()
            && self.inner.route == ScannerRoute::Default
            && self.inner.identity.is_some()
        {
            scanner_files_protocol::compat_beat_identity(&bytes).ok()
        } else {
            None
        };
        if let Some(task_id) = compat {
            return Ok(Some(ScannerCandidate {
                root: ScannerRoot::CompatBeat(task_id),
                bytes,
                held,
                origin: jobs.inner.clone(),
                route: self.inner.route,
            }));
        }
        let message = if self.inner.route == ScannerRoute::Default {
            default_protocol::parse(&bytes)
        } else {
            let mut value: Value =
                serde_json::from_slice(&bytes).map_err(|_| ScannerIoError::Unsupported)?;
            if value["properties"]["delivery_info"]
                != json!({"exchange":"","routing_key":self.inner.route.name()})
            {
                return Err(ScannerIoError::Unsupported);
            }
            // Explicit fixture route validation; legacy parser and raw CAS bytes remain
            // unchanged. This is not treating another HTTP/task method as equivalent.
            value["properties"]["delivery_info"]["routing_key"] = json!("celery");
            default_protocol::parse(
                &serde_json::to_vec(&value).map_err(|_| ScannerIoError::Unsupported)?,
            )
        }
        .map_err(|_| ScannerIoError::Unsupported)?;
        if message.kind != Kind::LogoScanner || message.expires.is_some() {
            return Err(ScannerIoError::Unsupported);
        }
        Ok(Some(ScannerCandidate {
            root: ScannerRoot::Scan(message.task_id),
            bytes,
            held,
            origin: jobs.inner.clone(),
            route: self.inner.route,
        }))
    }
    pub(crate) async fn claim(
        &self,
        candidate: ScannerCandidate,
    ) -> Result<ScannerRoot, ScannerIoError> {
        let jobs = &self.inner.jobs;
        if !Arc::ptr_eq(&jobs.inner, &candidate.origin) || self.inner.route != candidate.route {
            return Err(ScannerIoError::Unsupported);
        }
        let reply = jobs
            .inner
            .broker
            .command_classified::<i64, _>(
                CLAIM_CREATE,
                &[self.inner.route.name().into()],
                &[&candidate.bytes],
                candidate.held,
            )
            .await;
        match reply {
            Ok((1, _)) => Ok(candidate.root),
            Ok((0, _)) => Err(ScannerIoError::Transport(Error::ClaimRace)),
            _ => Err(ScannerIoError::Operational),
        }
    }
    pub(crate) async fn marker(
        &self,
        path: &str,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<String>, ScannerIoError> {
        self.inner.markers.command_classified::<Option<String>,_>("local n=redis.call('STRLEN',KEYS[1]); if n>64 then return redis.error_reply('OWNED_MARKER_OVERSIZE') end; return redis.call('GET',KEYS[1])",&[format!("processed_file:{path}")],&[],owner).await.map(|(v,_)|v).map_err(|_|ScannerIoError::Operational)
    }
    pub(crate) async fn set_marker(
        &self,
        path: &str,
        value: &str,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), ScannerIoError> {
        if path.len() > 4096 || value.len() > 64 {
            return Err(ScannerIoError::Unsupported);
        }
        let _physical = self
            .inner
            .jobs
            .lease(owner.clone())
            .map_err(ScannerIoError::Transport)?;
        let mut cache = self.inner.cache.clone();
        let reply: String = redis::cmd("SET")
            .arg(format!("processed_file:{path}"))
            .arg(value)
            .arg("EX")
            .arg(259200)
            .query_async(&mut cache)
            .await
            .map_err(known)?;
        if reply != "OK" {
            return Err(ScannerIoError::Operational);
        }
        Ok(())
    }
    pub(crate) async fn publish(
        &self,
        kind: scanner_files_protocol::Kind,
        object: i64,
        parent: Uuid,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Uuid, ScannerIoError> {
        let _physical = self
            .inner
            .jobs
            .lease(owner.clone())
            .map_err(ScannerIoError::Transport)?;
        let message = scanner_files_protocol::Message {
            kind,
            object,
            parent,
            id: Uuid::new_v4(),
        };
        let actual = match &self.inner.identity {
            Some(source) => Some(source.current().map_err(|_| ScannerIoError::Operational)?),
            None => None,
        };
        let bytes = scanner_files_protocol::encode(
            message,
            actual.as_ref().map_or(self.inner.reply, |v| v.reply_to),
            Uuid::new_v4(),
            actual
                .as_ref()
                .map_or("rust@mapped-scanner", |v| v.origin.as_str()),
        )
        .map_err(|_| ScannerIoError::Unsupported)?;
        scanner_files_protocol::parse_native(&bytes).map_err(|_| ScannerIoError::Unsupported)?;
        if self.inner.identity.is_some() {
            use sha2::Digest;
            {
                let committed = self
                    .inner
                    .committed
                    .lock()
                    .map_err(|_| ScannerIoError::Operational)?;
                if committed.len() >= 32 {
                    return Err(ScannerIoError::Capacity);
                }
            }
            let lease = self
                .inner
                .jobs
                .lease(owner.clone())
                .map_err(ScannerIoError::Transport)?;
            let limit = self.inner.jobs.inner.config.max_queued_jobs.to_string();
            let (reply, _held) = self
                .inner
                .jobs
                .inner
                .broker
                .command_classified::<i64, _>(
                    PUBLISH,
                    &["celery".into(), "_kombu.binding.celery".into()],
                    &[&bytes, limit.as_bytes()],
                    lease,
                )
                .await
                .map_err(|error| ScannerIoError::Transport(map(error)))?;
            match reply {
                n if n > 0 => {}
                -2 => return Err(ScannerIoError::Capacity),
                _ => return Err(ScannerIoError::Operational),
            }
            self.inner
                .committed
                .lock()
                .map_err(|_| ScannerIoError::Operational)?
                .push(ScannerPublication {
                    message,
                    digest: sha2::Sha256::digest(&bytes).into(),
                });
        } else {
            let mut broker = self.inner.broker.clone();
            let n: usize = redis::cmd("LLEN")
                .arg("celery")
                .query_async(&mut broker)
                .await
                .map_err(known)?;
            // The initial domain has one scanner publisher and no concurrent external
            // producers. Broader atomic admission requires its own contract/gate.
            if n >= self.inner.jobs.inner.config.max_queued_jobs {
                return Err(ScannerIoError::Capacity);
            }
            let _: i64 = redis::cmd("SADD")
                .arg("_kombu.binding.celery")
                .arg("celery\x06\x16\x06\x16celery")
                .query_async(&mut broker)
                .await
                .map_err(known)?;
            {
                let mut attempts = self
                    .inner
                    .attempts
                    .lock()
                    .map_err(|_| ScannerIoError::Operational)?;
                if attempts.len() == 32 {
                    return Err(ScannerIoError::Capacity);
                }
                attempts
                    .push(serde_json::from_slice(&bytes).map_err(|_| ScannerIoError::Operational)?);
            }
            let _: i64 = redis::cmd("LPUSH")
                .arg("celery")
                .arg(bytes)
                .query_async(&mut broker)
                .await
                .map_err(known)?;
        }
        Ok(message.id)
    }
    pub(crate) fn begin_run(&self) -> Result<(), ScannerIoError> {
        self.inner
            .committed
            .lock()
            .map_err(|_| ScannerIoError::Operational)?
            .clear();
        self.inner
            .attempts
            .lock()
            .map_err(|_| ScannerIoError::Operational)?
            .clear();
        Ok(())
    }
    pub fn publication_attempts(&self) -> Result<Vec<Value>, ScannerIoError> {
        self.inner
            .attempts
            .lock()
            .map(|v| v.clone())
            .map_err(|_| ScannerIoError::Operational)
    }
    pub(crate) fn marker_transport(&self) -> RedisTransport {
        self.inner.markers.clone()
    }
    pub(crate) fn reply_id(&self) -> Uuid {
        self.inner.reply
    }
    pub(crate) fn origin(&self) -> &'static str {
        "rust@mapped-scanner"
    }
    pub(crate) fn jobs(&self) -> Jobs {
        self.inner.jobs.clone()
    }
    pub(crate) async fn write(
        &self,
        id: Uuid,
        bytes: Vec<u8>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, ScannerIoError> {
        crate::decode::decode(&bytes, id).map_err(|_| ScannerIoError::Unsupported)?;
        self.inner
            .jobs
            .write_metadata(id, bytes, owner)
            .await
            .map_err(ScannerIoError::Transport)
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), ScannerIoError> {
        self.inner
            .jobs
            .shutdown(timeout)
            .await
            .map_err(ScannerIoError::Transport)
    }
}
