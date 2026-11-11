use axum::{body::Body, http::Response};
use bytes::Bytes;
use dispatcharr_clients::{ClientLease, ClientStore};
use dispatcharr_contracts::{
    PlaybackCandidate, PlaybackError, PlaybackSource, PlaybackViewer, UserLimitPolicy,
};
use dispatcharr_coordination::{AcquireRequest, Coordinator, ProviderReservation};
use dispatcharr_input_store::InputStore;
use dispatcharr_stream_core::ts::{DiscontinuityMarker, PacketAssembler, PACKET_SIZE};
use dispatcharr_stream_runtime::{Channel, Viewer};
#[path = "health.rs"]
mod health;
#[path = "history.rs"]
mod history;
use futures_util::StreamExt;
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashMap},
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{watch, Notify, OwnedSemaphorePermit, Semaphore},
    time::{self, Instant},
};

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub max_channels: usize,
    pub max_viewers_per_channel: usize,
    pub retained_bytes_per_channel: usize,
    /// Recent in-process payload cache when shared authoritative history exists.
    /// The backing input store retains its independent full history contract.
    pub shared_hot_cache_bytes: usize,
    pub retention: Duration,
    pub chunk_bytes: usize,
    pub flush_interval: Duration,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub startup_timeout: Duration,
    /// Original manager's health clocks, distinct from HTTP/readiness timeouts.
    pub health_startup_timeout: Duration,
    pub health_connection_timeout: Duration,
    pub health_initialization_grace: Duration,
    pub health_check_interval: Duration,
    /// Number of canonical input chunks required before startup is ready.
    pub initial_buffer_chunks: u64,
    /// Low-latency component mode. The application disables periodic and EOF
    /// partial-chunk writes to match the original full-chunk input buffer.
    pub flush_partial_chunks: bool,
    /// Original native TS initialization waits, media error packets and
    /// post-readiness positioning. Internal format processors keep errors typed.
    pub source_initialization: bool,
    pub retry_delay: Duration,
    /// Retries after the initial request; a complete upstream EOF also counts.
    pub max_retries: usize,
    pub retry_window: Duration,
    pub stable_connection_threshold: Duration,
    pub rotation_cooldown: Duration,
    pub max_stream_switches: usize,
    pub max_candidates: usize,
    pub keepalive_interval: Duration,
    pub max_keepalive_duration: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_channels: 32,
            max_viewers_per_channel: 400,
            retained_bytes_per_channel: 16 * 1024 * 1024,
            shared_hot_cache_bytes: 4 * 1024 * 1024,
            retention: Duration::from_secs(30),
            chunk_bytes: PACKET_SIZE * 1361,
            flush_interval: Duration::from_millis(50),
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(10),
            startup_timeout: Duration::from_secs(30),
            health_startup_timeout: Duration::from_secs(8),
            health_connection_timeout: Duration::from_secs(10),
            health_initialization_grace: Duration::from_secs(60),
            health_check_interval: Duration::from_secs(5),
            initial_buffer_chunks: 1,
            flush_partial_chunks: true,
            source_initialization: false,
            retry_delay: Duration::from_millis(500),
            // Python MAX_RETRIES=3 counts total attempts; this field counts
            // retries after the initial attempt, so 2 gives the same budget.
            max_retries: 2,
            retry_window: Duration::from_secs(1800),
            stable_connection_threshold: Duration::from_secs(30),
            rotation_cooldown: Duration::from_secs(60),
            max_stream_switches: 10,
            max_candidates: 256,
            keepalive_interval: Duration::from_millis(500),
            max_keepalive_duration: Duration::from_secs(300),
        }
    }
}

impl EngineConfig {
    fn health_timing(&self) -> health::Timing {
        health::Timing {
            startup: self.health_startup_timeout,
            connection: self.health_connection_timeout,
            initialization: self.health_initialization_grace,
            interval: self.health_check_interval,
        }
    }
    fn local_cache_bytes(&self, shared: bool) -> usize {
        if shared {
            self.retained_bytes_per_channel
                .min(self.shared_hot_cache_bytes.max(self.chunk_bytes))
        } else {
            self.retained_bytes_per_channel
        }
    }
}

#[derive(Clone)]
pub struct StreamEngine {
    pub(crate) inner: Arc<Inner>,
}

pub struct MediaFrame {
    pub data: Bytes,
    pub discontinuity: bool,
}
pub type MediaStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<MediaFrame, io::Error>> + Send>>;

pub(crate) struct Inner {
    config: EngineConfig,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    slots: Arc<Semaphore>,
    coordinator: Option<Coordinator>,
    pub(crate) clients: Option<ClientStore>,
    pub(crate) inputs: Option<InputStore>,
    workers: AtomicUsize,
    workers_done: Notify,
    pub(crate) statistics_reader:
        Mutex<Option<Weak<dyn dispatcharr_contracts::LiveStatsSnapshotReader>>>,
}

#[derive(Clone, Debug)]
enum Startup {
    Pending,
    Ready,
    Failed(PlaybackError),
}

struct Session {
    source: PlaybackSource,
    channel: Channel,
    stop: watch::Sender<bool>,
    startup: watch::Sender<Startup>,
    received_bytes: AtomicU64,
    reported_bytes: AtomicU64,
    last_bytes_report_bits: AtomicU64,
    reconnects: AtomicU64,
    buffer_index: AtomicU64,
    last_data_ms: AtomicU64,
    started_at: f64,
    current: Mutex<PlaybackCandidate>,
    connecting: AtomicBool,
    following: AtomicBool,
    manager_visible: AtomicBool,
    health: Mutex<health::Health>,
    idle_watching: AtomicBool,
    native_initializing: AtomicBool,
    upstream_admitted: AtomicBool,
    ready_requested: AtomicBool,
    metadata_wake: Notify,
    publisher: Mutex<Option<(InputStore, String)>>,
    input_owner: Mutex<Option<String>>,
    _permit: OwnedSemaphorePermit,
}

#[derive(Clone, Debug, Serialize)]
pub struct StreamStatus {
    pub channel_id: String,
    pub channel_name: String,
    pub stream_id: i64,
    pub profile_id: i64,
    pub provider_profile_id: Option<i64>,
    pub state: &'static str,
    pub viewer_count: usize,
    pub retained_bytes: usize,
    pub received_bytes: u64,
    pub reconnects: u64,
}
pub(crate) struct LocalManagerStatus {
    pub healthy: bool,
    pub connected: bool,
    pub last_data_time: f64,
}

impl StreamEngine {
    pub(crate) fn local_manager_status(&self, id: &str) -> Option<LocalManagerStatus> {
        let session = self.inner.sessions.lock().unwrap().get(id)?.clone();
        if !session.manager_visible.load(Ordering::Acquire)
            || session.following.load(Ordering::Acquire)
        {
            return None;
        }
        let health = session.health.lock().unwrap();
        Some(LocalManagerStatus {
            healthy: health.healthy,
            connected: health.connected,
            last_data_time: health.last_data_time,
        })
    }
    pub fn new(config: EngineConfig) -> Result<Self, PlaybackError> {
        Self::new_with_coordinator(config, None)
    }

    pub fn new_with_coordinator(
        config: EngineConfig,
        coordinator: Option<Coordinator>,
    ) -> Result<Self, PlaybackError> {
        Self::new_with_services(config, coordinator, None)
    }

    pub fn new_with_services(
        config: EngineConfig,
        coordinator: Option<Coordinator>,
        clients: Option<ClientStore>,
    ) -> Result<Self, PlaybackError> {
        Self::new_with_shared_inputs(config, coordinator, clients, None)
    }

    pub fn new_with_shared_inputs(
        config: EngineConfig,
        coordinator: Option<Coordinator>,
        clients: Option<ClientStore>,
        inputs: Option<InputStore>,
    ) -> Result<Self, PlaybackError> {
        if inputs.is_some() && (coordinator.is_none() || clients.is_none()) {
            return Err(PlaybackError::new(
                500,
                "Shared input requires coordination and client metadata",
            ));
        }
        if config.max_channels == 0
            || config.max_channels > Semaphore::MAX_PERMITS
            || config.max_viewers_per_channel == 0
            || config.max_viewers_per_channel > Semaphore::MAX_PERMITS
            || config.chunk_bytes < PACKET_SIZE
            || !config.chunk_bytes.is_multiple_of(PACKET_SIZE)
            || config.chunk_bytes > config.retained_bytes_per_channel
            || config.shared_hot_cache_bytes == 0
            || config.flush_interval.is_zero()
            || config.retention.is_zero()
            || config.connect_timeout.is_zero()
            || config.read_timeout.is_zero()
            || config.startup_timeout.is_zero()
            || config.health_check_interval.is_zero()
            || config.initial_buffer_chunks == 0
            || config.retry_window.is_zero()
            || config.stable_connection_threshold.is_zero()
            || config.rotation_cooldown.is_zero()
            || config.max_candidates == 0
            || config.keepalive_interval.is_zero()
            || config.max_keepalive_duration.is_zero()
        {
            return Err(PlaybackError::new(500, "Invalid streaming resource limits"));
        }
        // Validate the HTTP builder at construction, but each provider attempt
        // owns its own client/session cookie jar, matching HTTPStreamReader.
        crate::upstream::Attempt::new(config.connect_timeout, config.read_timeout)
            .map_err(|_| PlaybackError::new(500, "HTTP client initialization failed"))?;
        let slots = Arc::new(Semaphore::new(config.max_channels));
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                sessions: Mutex::new(HashMap::new()),
                slots,
                coordinator,
                clients,
                inputs,
                workers: AtomicUsize::new(0),
                workers_done: Notify::new(),
                statistics_reader: Mutex::new(None),
            }),
        })
    }

    /// Authorization/resolution must run before calling this method for each viewer.
    pub async fn play(&self, source: PlaybackSource) -> Result<Response<Body>, PlaybackError> {
        let stream = self.open_stream(source, true).await?;
        Response::builder()
            .header("Content-Type", "video/mp2t")
            .header("Cache-Control", "no-cache")
            .header("Connection", "close")
            .body(Body::from_stream(
                stream.map(|result| result.map(|frame| frame.data)),
            ))
            .map_err(|_| PlaybackError::new(500, "Stream response initialization failed"))
    }

    /// Internal output processors subscribe without creating a phantom client
    /// record. Their real HTTP/capability clients must be admitted separately.
    /// The runtime reader still holds its bounded transport/channel admission.
    pub async fn open_input(
        &self,
        mut source: PlaybackSource,
    ) -> Result<MediaStream, PlaybackError> {
        source.viewer = None;
        self.open_stream(source, false).await
    }

    async fn open_stream(
        &self,
        source: PlaybackSource,
        register_client: bool,
    ) -> Result<MediaStream, PlaybackError> {
        if source
            .viewer
            .as_ref()
            .is_some_and(|viewer| viewer.stream_limit > 0)
            && self.inner.clients.is_none()
        {
            return Err(PlaybackError::new(
                501,
                "Per-user stream-slot accounting requires Redis client metadata",
            ));
        }
        if source.alternates.len() >= self.inner.config.max_candidates {
            return Err(PlaybackError::new(503, "Stream candidate capacity reached"));
        }
        if self.inner.coordinator.is_none()
            && source.provider_max_streams != 0
            && source
                .alternates
                .iter()
                .all(|candidate| candidate.provider_max_streams != 0)
        {
            // A limited provider must never silently bypass the shared Redis
            // reservation when this engine has no coordinator configured.
            return Err(PlaybackError::new(
                503,
                "Capacity-limited provider reservation is not implemented",
            ));
        }
        let key = source.channel_id.clone();
        let startup_behind = source.startup_behind;
        let client = if let Some(clients) = self.inner.clients.as_ref().filter(|_| register_client)
        {
            let viewer = source.viewer.clone().unwrap_or(PlaybackViewer {
                user_id: None,
                username: None,
                ip_address: "0.0.0.0".parse().unwrap(),
                user_agent: "unknown".into(),
                stream_limit: 0,
                limit_policy: UserLimitPolicy::default(),
            });
            Some(
                clients
                    .register(&key, &viewer, "mpegts")
                    .await
                    .map_err(|error| client_error(error, viewer.stream_limit))?,
            )
        } else {
            None
        };
        let (session, viewer, created) = {
            let mut sessions = self.inner.sessions.lock().unwrap();
            if let Some(session) = sessions.get(&key) {
                if register_client && self.inner.config.source_initialization {
                    session.native_initializing.store(true, Ordering::Release);
                }
                if session.source.profile_id != source.profile_id {
                    return Err(PlaybackError::new(
                        409,
                        "Channel already uses another profile",
                    ));
                }
                let viewer = session
                    .channel
                    .subscribe(source.startup_behind)
                    .map_err(runtime_error)?;
                (session.clone(), viewer, false)
            } else {
                let permit = self
                    .inner
                    .slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| PlaybackError::new(503, "Channel capacity reached"))?;
                let cache_bytes = self
                    .inner
                    .config
                    .local_cache_bytes(self.inner.inputs.is_some());
                let channel = Channel::new(
                    cache_bytes,
                    self.inner.config.retention,
                    self.inner.config.max_viewers_per_channel,
                )
                .map_err(runtime_error)?;
                let viewer = channel
                    .subscribe(source.startup_behind)
                    .map_err(runtime_error)?;
                let (stop, _) = watch::channel(false);
                let (startup, _) = watch::channel(Startup::Pending);
                let current = Mutex::new(source.primary());
                let session = Arc::new(Session {
                    source,
                    channel,
                    stop,
                    startup,
                    received_bytes: AtomicU64::new(0),
                    reported_bytes: AtomicU64::new(0),
                    last_bytes_report_bits: AtomicU64::new(epoch_seconds().to_bits()),
                    reconnects: AtomicU64::new(0),
                    buffer_index: AtomicU64::new(0),
                    last_data_ms: AtomicU64::new(0),
                    started_at: epoch_seconds(),
                    current,
                    connecting: AtomicBool::new(true),
                    following: AtomicBool::new(false),
                    manager_visible: AtomicBool::new(false),
                    health: Mutex::new(health::Health::new(epoch_seconds())),
                    idle_watching: AtomicBool::new(false),
                    native_initializing: AtomicBool::new(
                        register_client && self.inner.config.source_initialization,
                    ),
                    upstream_admitted: AtomicBool::new(false),
                    ready_requested: AtomicBool::new(false),
                    metadata_wake: Notify::new(),
                    publisher: Mutex::new(None),
                    input_owner: Mutex::new(None),
                    _permit: permit,
                });
                sessions.insert(key.clone(), session.clone());
                // Count before releasing the registry lock so shutdown cannot
                // report quiescence before this newly created worker starts.
                self.inner.workers.fetch_add(1, Ordering::AcqRel);
                (session, viewer, true)
            }
        };
        let mut state = TransportState {
            viewer,
            client,
            _guard: ViewerGuard {
                inner: Arc::downgrade(&self.inner),
                session: session.clone(),
                key: key.clone(),
            },
        };
        if created {
            let weak = Arc::downgrade(&self.inner);
            let running = session.clone();
            let worker = WorkerCount(self.inner.clone());
            tokio::spawn(async move {
                let _worker = worker;
                run_upstream(weak, key, running).await
            });
        }
        let mut startup = session.startup.subscribe();
        let ready = async {
            loop {
                let status = startup.borrow_and_update().clone();
                match status {
                    Startup::Ready => return Ok(()),
                    Startup::Failed(failure) => return Err(failure),
                    Startup::Pending => startup
                        .changed()
                        .await
                        .map_err(|_| PlaybackError::new(502, "Upstream initialization stopped"))?,
                }
            }
        };
        if register_client && self.inner.config.source_initialization {
            tokio::pin!(ready);
            let mut client_stop = state.client.as_ref().map(ClientLease::stopped);
            let stopped = async {
                match client_stop.as_mut() {
                    Some(control) => {
                        if !*control.borrow() {
                            let _ = control.changed().await;
                        }
                    }
                    None => std::future::pending::<()>().await,
                }
            };
            let outcome = tokio::select! {
                biased;
                _=stopped=>{
                    if !session.upstream_admitted.load(Ordering::Acquire) {
                        time::timeout(self.inner.config.startup_timeout,&mut ready).await
                            .map_err(|_|PlaybackError::new(504,"Stream initialization timed out"))??;
                    }
                    Some("Error: Client disconnected")
                },
                result=time::timeout(self.inner.config.startup_timeout,&mut ready)=>match result {
                    Ok(Ok(()))=>None,
                    Ok(Err(_)) if session.upstream_admitted.load(Ordering::Acquire)=>Some("Error: Channel is stopping"),
                    Ok(Err(failure))=>return Err(failure),
                    Err(_)=>Some("Error: Initialization timeout"),
                }
            };
            if let Some(message) = outcome {
                let frame = InitializationFrame {
                    data: initialization_packet(message),
                    _state: state,
                };
                return Ok(Box::pin(futures_util::stream::once(async move {
                    Ok(MediaFrame {
                        data: Bytes::from_owner(frame),
                        discontinuity: false,
                    })
                })));
            }
            state
                .viewer
                .reposition(startup_behind)
                .map_err(runtime_error)?;
        } else {
            time::timeout(self.inner.config.startup_timeout, ready)
                .await
                .map_err(|_| PlaybackError::new(504, "Stream initialization timed out"))??;
        }
        let history = match &self.inner.inputs {
            Some(store) => Some(history::History {
                store: store.clone(),
                channel: session.source.channel_id.clone(),
                owner: session
                    .input_owner
                    .lock()
                    .unwrap()
                    .clone()
                    .ok_or_else(|| PlaybackError::new(503, "Shared input owner unavailable"))?,
                initial_behind_chunks: self.inner.config.initial_buffer_chunks,
                read_timeout: self.inner.config.read_timeout,
            }),
            None => None,
        };
        if let Some(history) = &history {
            history
                .position(&mut state.viewer, startup_behind)
                .await
                .map_err(|_| PlaybackError::new(503, "Shared input position unavailable"))?;
        }
        let keepalive_interval = self.inner.config.keepalive_interval;
        let max_keepalive_duration = self.inner.config.max_keepalive_duration;
        let stream = async_stream::stream! {
            let mut state = state;
            let mut stopped=state.client.as_ref().map(ClientLease::stopped);
            let mut last_media=Instant::now();
            'transport: loop {
                if stopped.as_ref().is_some_and(|control|*control.borrow()) {break;}
                // Keep a historical read alive across keepalive ticks. Dropping
                // and recreating it each tick would queue cancelled Redis reads.
                let waiting=history::next(&mut state.viewer,history.as_ref());
                tokio::pin!(waiting);
                let next=loop {
                    let client_stopped=async {
                        match stopped.as_mut() {
                            Some(control)=>{let _=control.changed().await;},
                            None=>std::future::pending::<()>().await,
                        }
                    };
                    tokio::select! {
                        _=client_stopped=>break 'transport,
                        next=&mut waiting=>break next,
                        _=time::sleep(keepalive_interval)=>{
                            if !register_client {continue;}
                            if last_media.elapsed()>=max_keepalive_duration {break 'transport;}
                            if let Some(client)=&state.client {client.record_keepalive(188);}
                            let frame=KeepaliveFrame {_client:state.client.clone(),_guard:state._guard.clone()};
                            yield Ok::<MediaFrame,io::Error>(MediaFrame {data:Bytes::from_owner(frame),discontinuity:false});
                        }
                    }
                };
                match next {
                    Ok(Some(chunk)) => {
                        last_media=Instant::now();
                        if let Some(client)=&state.client {client.record_bytes(chunk.as_ref().len());}
                        let discontinuity=chunk.discontinuity();
                        let frame = FrameOwner { chunk, _client:state.client.clone(), _guard: state._guard.clone() };
                        yield Ok::<MediaFrame, io::Error>(MediaFrame {data:Bytes::from_owner(frame),discontinuity});
                    }
                    Ok(None)=>break,
                    Err(error)=>{yield Err(error);break;},
                }
            }
        };
        Ok(Box::pin(stream))
    }

    /// Versioned administrative diagnostics; not the source-compatible status API.
    pub async fn memory_diagnostics(&self) -> Result<serde_json::Value, PlaybackError> {
        let all = self.status();
        let total = all.len();
        let mut channels = Vec::new();
        let mut local = 0u64;
        let mut redis_logical = 0u64;
        let mut redis_allocated = 0u64;
        for status in all.into_iter().take(64) {
            local += status.retained_bytes as u64;
            let shared = if let Some(inputs) = &self.inner.inputs {
                let value = inputs
                    .retained_diagnostics(&status.channel_id)
                    .await
                    .map_err(|_| PlaybackError::new(503, "Retention diagnostics unavailable"))?;
                redis_logical += value["logical_payload_bytes"].as_u64().unwrap_or(0);
                redis_allocated += value["redis_known_key_allocation_bytes"]
                    .as_u64()
                    .unwrap_or(0);
                value
            } else {
                serde_json::Value::Null
            };
            channels.push(serde_json::json!({"channel_id":status.channel_id,"state":status.state,
                "local_subscriptions":status.viewer_count,"local_retained_payload_bytes":status.retained_bytes,
                "received_bytes":status.received_bytes,"shared_input":shared}));
        }
        Ok(
            serde_json::json!({"format_version":1,"channel_count":total,"sampled_channels":channels.len(),"truncated":total>64,
            "local_retained_payload_bytes":local,"redis_logical_payload_bytes":redis_logical,
            "redis_known_key_allocation_bytes":redis_allocated,
            "configured_local_limit_bytes_per_channel":self.inner.config.local_cache_bytes(self.inner.inputs.is_some()),
            "configured_authoritative_history_limit_bytes_per_channel":self.inner.config.retained_bytes_per_channel,
            "configured_retention_seconds":self.inner.config.retention.as_secs_f64(),
            "shared_input_configured":self.inner.inputs.is_some(),"channels":channels,
            "scope":"Retained payload/key allocations only, excludes HTTP in-flight frames and allocator overhead; shared Redis bytes must not be summed across replica reports"}),
        )
    }

    pub fn status(&self) -> Vec<StreamStatus> {
        let sessions = self.inner.sessions.lock().unwrap();
        let mut status: Vec<_> = sessions
            .values()
            .map(|session| {
                let current = session.current.lock().unwrap();
                StreamStatus {
                    channel_id: session.source.channel_id.clone(),
                    channel_name: session.source.channel_name.clone(),
                    stream_id: current.stream_id,
                    profile_id: session.source.profile_id,
                    provider_profile_id: current.provider_profile_id,
                    state: match *session.startup.borrow() {
                        Startup::Pending if session.upstream_admitted.load(Ordering::Acquire) => {
                            "connecting"
                        }
                        Startup::Pending => "initializing",
                        Startup::Ready if session.connecting.load(Ordering::Relaxed) => "buffering",
                        Startup::Ready => "active",
                        Startup::Failed(_) => "error",
                    },
                    viewer_count: session.channel.viewer_count(),
                    retained_bytes: session.channel.retained_bytes(),
                    received_bytes: session.received_bytes.load(Ordering::Relaxed),
                    reconnects: session.reconnects.load(Ordering::Relaxed),
                }
            })
            .collect();
        status.sort_by(|a, b| a.channel_id.cmp(&b.channel_id));
        status
    }

    pub fn stop(&self, channel_id: &str) -> bool {
        let session = self.inner.sessions.lock().unwrap().remove(channel_id);
        if let Some(session) = session {
            session.stop.send_replace(true);
            session.channel.stop();
            true
        } else {
            false
        }
    }

    pub fn shutdown(&self) {
        self.inner.slots.close();
        let sessions = std::mem::take(&mut *self.inner.sessions.lock().unwrap());
        for session in sessions.into_values() {
            session.stop.send_replace(true);
            session.channel.stop();
        }
    }

    pub async fn shutdown_wait(&self, timeout: Duration) -> bool {
        self.shutdown();
        time::timeout(timeout, async {
            loop {
                let notified = self.inner.workers_done.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.inner.workers.load(Ordering::Acquire) == 0
                    && self
                        .inner
                        .coordinator
                        .as_ref()
                        .is_none_or(Coordinator::is_idle)
                {
                    return;
                }
                tokio::select! {_=notified=>{},_=time::sleep(Duration::from_millis(20))=>{}}
            }
        })
        .await
        .is_ok()
    }

    pub async fn stop_global(&self, channel: &str) -> Result<bool, PlaybackError> {
        let Some(clients) = self.inner.clients.as_ref() else {
            return Ok(self.stop(channel));
        };
        let shared = clients.request_channel_stop(channel).await;
        let local = self.stop(channel);
        match shared {
            Ok(shared) => Ok(shared || local),
            Err(_) if local => Ok(true),
            Err(error) => Err(client_error(error, 0)),
        }
    }

    pub async fn has_channel(&self, channel: &str) -> Result<bool, PlaybackError> {
        if self.inner.sessions.lock().unwrap().contains_key(channel) {
            return Ok(true);
        }
        match &self.inner.clients {
            Some(clients) => clients
                .channel_snapshot(channel)
                .await
                .map(|snapshot| snapshot.is_some())
                .map_err(|error| client_error(error, 0)),
            None => Ok(false),
        }
    }

    pub async fn stop_client(&self, channel: &str, id: &str) -> Result<bool, PlaybackError> {
        let clients = self
            .inner
            .clients
            .as_ref()
            .ok_or_else(|| PlaybackError::new(503, "Redis client metadata unavailable"))?;
        clients
            .stop_client(channel, id)
            .await
            .map_err(|error| client_error(error, 0))
    }
}

fn client_error(error: dispatcharr_clients::Error, limit: u32) -> PlaybackError {
    match error {
        dispatcharr_clients::Error::UserLimit => PlaybackError::new(
            429,
            &format!("Stream limit exceeded ({limit} concurrent streams allowed)"),
        ),
        dispatcharr_clients::Error::InvalidClient => {
            PlaybackError::new(400, "Invalid client metadata")
        }
        dispatcharr_clients::Error::Capacity => PlaybackError::new(503, "Client capacity reached"),
        _ => PlaybackError::new(503, "Redis client metadata unavailable"),
    }
}

fn runtime_error(error: dispatcharr_stream_runtime::Error) -> PlaybackError {
    match error {
        dispatcharr_stream_runtime::Error::AtCapacity => {
            PlaybackError::new(503, "Viewer capacity reached")
        }
        dispatcharr_stream_runtime::Error::Stopped => {
            PlaybackError::new(503, "Channel is stopping")
        }
        _ => PlaybackError::new(500, "Streaming buffer initialization failed"),
    }
}

struct TransportState {
    viewer: Viewer,
    client: Option<ClientLease>,
    _guard: ViewerGuard,
}

struct FrameOwner {
    chunk: history::Payload,
    _client: Option<ClientLease>,
    _guard: ViewerGuard,
}

struct InitializationFrame {
    data: [u8; 188],
    _state: TransportState,
}
impl AsRef<[u8]> for InitializationFrame {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
fn initialization_packet(message: &str) -> [u8; 188] {
    let mut packet = KEEPALIVE_PACKET;
    let bytes = message.as_bytes();
    let count = bytes.len().min(180);
    packet[4..4 + count].copy_from_slice(&bytes[..count]);
    packet
}

impl AsRef<[u8]> for FrameOwner {
    fn as_ref(&self) -> &[u8] {
        self.chunk.as_ref()
    }
}

// Exact bytes of the existing create_ts_packet('keepalive') helper. Its null
// PID packet is shared statically; admission leases still follow each frame.
const KEEPALIVE_PACKET: [u8; 188] = {
    let mut packet = [0; 188];
    packet[0] = 0x47;
    packet[1] = 0x1f;
    packet[2] = 0xff;
    packet
};
struct KeepaliveFrame {
    _client: Option<ClientLease>,
    _guard: ViewerGuard,
}
impl AsRef<[u8]> for KeepaliveFrame {
    fn as_ref(&self) -> &[u8] {
        &KEEPALIVE_PACKET
    }
}

#[derive(Clone)]
struct ViewerGuard {
    inner: Weak<Inner>,
    session: Arc<Session>,
    key: String,
}

impl Drop for ViewerGuard {
    fn drop(&mut self) {
        if *self.session.stop.borrow()
            || self.session.channel.viewer_count() != 0
            || self.session.idle_watching.swap(true, Ordering::AcqRel)
        {
            return;
        }
        let weak = self.inner.clone();
        let session = self.session.clone();
        let key = self.key.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let mut stop = session.stop.subscribe();
                let mut empty_since = None;
                let idle_began = Instant::now();
                loop {
                    if *stop.borrow() {
                        break;
                    }
                    if session.channel.viewer_count() > 0 {
                        session.idle_watching.store(false, Ordering::Release);
                        // Close the last-drop/reset race without creating duplicate watchers.
                        if session.channel.viewer_count() == 0
                            && session
                                .idle_watching
                                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                                .is_ok()
                        {
                            continue;
                        }
                        return;
                    }
                    let Some(inner) = weak.upgrade() else {
                        session.channel.stop();
                        break;
                    };
                    let shared_owner =
                        inner.inputs.is_some() && !session.following.load(Ordering::Acquire);
                    let remote_demand = if shared_owner {
                        if let Some(clients) = &inner.clients {
                            match clients.channel_client_count(&key).await {
                                Ok(count) => count > 0,
                                Err(_) => false,
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if remote_demand {
                        empty_since = None;
                    } else {
                        let began = *empty_since.get_or_insert_with(Instant::now);
                        if began.elapsed() >= session.source.shutdown_delay {
                            let mut sessions = inner.sessions.lock().unwrap();
                            if session.channel.viewer_count() == 0
                                && sessions
                                    .get(&key)
                                    .is_some_and(|current| Arc::ptr_eq(current, &session))
                            {
                                sessions.remove(&key);
                                session.stop.send_replace(true);
                                session.channel.stop();
                                break;
                            }
                        }
                    }
                    let remote_poll = if idle_began.elapsed() < Duration::from_millis(100) {
                        Duration::from_millis(5)
                    } else {
                        Duration::from_secs(1)
                    };
                    let wait = empty_since.map_or(remote_poll, |began| {
                        session
                            .source
                            .shutdown_delay
                            .saturating_sub(began.elapsed())
                            .min(Duration::from_secs(1))
                    });
                    tokio::select! {_=stop.changed()=>break,_=time::sleep(wait)=>{}}
                }
                session.idle_watching.store(false, Ordering::Release);
            });
        } else {
            self.session.idle_watching.store(false, Ordering::Release);
        }
    }
}

async fn run_follower(
    inner: &Inner,
    session: &Arc<Session>,
    stop: &mut watch::Receiver<bool>,
) -> PlaybackError {
    let store = inner
        .inputs
        .as_ref()
        .expect("shared follower requires input store");
    let mut owner: Option<String> = None;
    let mut cursor: Option<u64> = None;
    let mut last_snapshot: Option<Instant> = None;
    let mut last_media = Instant::now();
    loop {
        if *stop.borrow() {
            return PlaybackError::new(503, "Stream stopped");
        }
        if inner.config.source_initialization
            && session.ready_requested.load(Ordering::Acquire)
            && matches!(*session.startup.borrow(), Startup::Pending)
        {
            if let Some(clients) = &inner.clients {
                match clients.channel_snapshot(&session.source.channel_id).await {
                    Ok(Some((fields, _)))
                        if fields.get("state").is_some_and(|state| {
                            state == "active" || state == "waiting_for_clients"
                        }) =>
                    {
                        clients.announce_channel_statistics(&session.source.channel_id);
                        session.startup.send_replace(Startup::Ready);
                    }
                    Ok(_) => {}
                    Err(_) => return PlaybackError::new(503, "Channel metadata unavailable"),
                }
            }
        }
        if cursor.is_none()
            || last_snapshot.is_none_or(|time| time.elapsed() >= Duration::from_secs(1))
        {
            let snapshot = tokio::select! {_=stop.changed()=>return PlaybackError::new(503,"Stream stopped"),snapshot=store.snapshot(&session.source.channel_id)=>snapshot};
            match snapshot {
                Ok(snapshot) => {
                    let Some(current) = &snapshot.owner else {
                        if let Some(previous) = &owner {
                            if let Some(clients) = &inner.clients {
                                let _ = clients
                                    .stop_abandoned_channel(&session.source.channel_id, previous)
                                    .await;
                            }
                            let _ = store
                                .cleanup_abandoned(&session.source.channel_id, previous)
                                .await;
                        }
                        return PlaybackError::new(503, "Channel owner stopped");
                    };
                    if owner.as_ref().is_some_and(|previous| previous != current) {
                        return PlaybackError::new(503, "Channel owner changed");
                    }
                    if owner.is_none() {
                        if let Some(clients) = &inner.clients {
                            if clients
                                .bind_local_owner(&session.source.channel_id, current)
                                .await
                                .is_err()
                            {
                                return PlaybackError::new(
                                    503,
                                    "Channel admission binding unavailable",
                                );
                            }
                        }
                    }
                    if cursor.is_none() {
                        cursor = Some(if session.source.startup_behind.is_zero() {
                            snapshot.index
                        } else {
                            snapshot
                                .cursor_for_age(
                                    session.source.startup_behind.as_secs_f64(),
                                    epoch_seconds(),
                                )
                                .unwrap_or(snapshot.index)
                        });
                    }
                    if let Some(first) = snapshot.chunks.iter().min_by_key(|stamp| stamp.sequence) {
                        if first.sequence > cursor.unwrap().saturating_add(1) {
                            cursor = Some(first.sequence - 1);
                        }
                    }
                    if snapshot.index < cursor.unwrap() {
                        return PlaybackError::new(503, "Channel input restarted");
                    }
                    if owner.is_none() {
                        if let Some(clients) = &inner.clients {
                            if clients
                                .channel_snapshot(&session.source.channel_id)
                                .await
                                .ok()
                                .flatten()
                                .is_some_and(|(fields, _)| {
                                    fields.get("state").is_some_and(|state| {
                                        !matches!(state.as_str(), "initializing" | "error")
                                    })
                                })
                            {
                                clients.announce_channel_statistics(&session.source.channel_id);
                            }
                        }
                    }
                    owner = Some(current.clone());
                    *session.input_owner.lock().unwrap() = Some(current.clone());
                    last_snapshot = Some(Instant::now());
                }
                Err(dispatcharr_input_store::Error::Capacity) => {
                    tokio::select! {_=stop.changed()=>return PlaybackError::new(503,"Stream stopped"),_=time::sleep(inner.config.flush_interval)=>{}}
                    continue;
                }
                Err(_) => return PlaybackError::new(503, "Shared channel input unavailable"),
            }
        }
        let next = cursor.unwrap().saturating_add(1);
        let read = tokio::select! {_=stop.changed()=>return PlaybackError::new(503,"Stream stopped"),read=store.get(&session.source.channel_id,next)=>read};
        match read {
            Ok(Some(bytes)) => {
                if owner.as_deref() != Some(bytes.owner.as_str()) {
                    return PlaybackError::new(503, "Channel owner changed");
                }
                let data: Arc<[u8]> = Arc::from(bytes.as_ref());
                let sequence = bytes.sequence;
                let timestamp = bytes.received_at;
                let discontinuity = bytes.discontinuity;
                // The ring has its own admission/retention bound. Release the
                // transient Redis read lease before publishing the bounded copy.
                drop(bytes);
                let now = Instant::now().into_std();
                let age = (epoch_seconds() - timestamp).clamp(0.0, 3600.0);
                let received = now.checked_sub(Duration::from_secs_f64(age)).unwrap_or(now);
                session
                    .received_bytes
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
                if session
                    .channel
                    .publish_numbered(data, discontinuity, received, sequence)
                    .is_err()
                {
                    return PlaybackError::new(503, "Stream stopped");
                }
                cursor = Some(sequence);
                session.buffer_index.store(sequence, Ordering::Relaxed);
                session
                    .last_data_ms
                    .store((timestamp * 1000.0) as u64, Ordering::Relaxed);
                session.connecting.store(false, Ordering::Relaxed);
                if sequence >= inner.config.initial_buffer_chunks {
                    session.request_ready(inner);
                }
                last_media = Instant::now();
                continue;
            }
            Ok(None) | Err(dispatcharr_input_store::Error::Capacity) => {}
            Err(_) => return PlaybackError::new(503, "Shared channel input unavailable"),
        }
        if last_media.elapsed() >= inner.config.max_keepalive_duration {
            return PlaybackError::new(504, "Shared channel input timed out");
        }
        tokio::select! {_=stop.changed()=>return PlaybackError::new(503,"Stream stopped"),_=time::sleep(inner.config.flush_interval)=>{}}
    }
}

struct WorkerCount(Arc<Inner>);
impl Drop for WorkerCount {
    fn drop(&mut self) {
        self.0.workers.fetch_sub(1, Ordering::AcqRel);
        self.0.workers_done.notify_waiters();
    }
}

async fn run_upstream(weak: Weak<Inner>, key: String, session: Arc<Session>) {
    let Some(inner) = weak.upgrade() else { return };
    let mut stop = session.stop.subscribe();
    // Ownership remains fenced across provider switches. A provider reservation
    // is acquired separately, after the old upstream and slot are released.
    let lease = if let Some(coordinator) = &inner.coordinator {
        let acquired = tokio::select! {
            _ = stop.changed() => Err(dispatcharr_coordination::Error::Lost),
            result = coordinator.acquire(AcquireRequest {
                channel_id: session.source.channel_id.clone(),
                provider_profile_id: None, provider_max_streams: 0, credential_pool: None,
            }) => result,
        };
        match acquired {
            Ok(lease) => Some(lease),
            Err(dispatcharr_coordination::Error::OwnerHeld) if inner.inputs.is_some() => {
                session.following.store(true, Ordering::Release);
                let failure = run_follower(&inner, &session, &mut stop).await;
                if let Some(clients) = &inner.clients {
                    let _ = clients.stop_local_channel(&key).await;
                }
                finish_session(&inner, &key, &session, failure);
                return;
            }
            Err(error) => {
                finish_session(&inner, &key, &session, coordination_error(error));
                return;
            }
        }
    } else {
        None
    };
    // A pending or foreign-owner subscription is not a local StreamManager.
    session.manager_visible.store(true, Ordering::Release);
    if let (Some(clients), Some(lease)) = (&inner.clients, &lease) {
        if clients.bind_local_owner(&key, lease.token()).await.is_err() {
            session.stop.send_replace(true);
        }
    }
    if let (Some(inputs), Some(lease)) = (&inner.inputs, &lease) {
        *session.publisher.lock().unwrap() = Some((inputs.clone(), lease.token().to_owned()));
        *session.input_owner.lock().unwrap() = Some(lease.token().to_owned());
    }
    let metadata_task = inner.clients.as_ref().map(|clients| {
        let clients = clients.clone();
        let session = session.clone();
        let owner = lease
            .as_ref()
            .map_or(String::new(), |lease| lease.token().to_owned());
        let shared = inner.inputs.is_some();
        let health_timing = inner.config.health_timing();
        tokio::spawn(async move {
            channel_metadata_worker(clients, session, owner, shared, health_timing).await
        })
    });
    let (_unleased_guard, unleased_watch) = watch::channel(false);
    let mut lost = lease.as_ref().map_or(unleased_watch, |lease| lease.lost());
    let mut candidates = vec![session.source.primary()];
    for candidate in &session.source.alternates {
        if !candidates.iter().any(|existing| {
            existing.stream_id == candidate.stream_id
                && existing.provider_profile_id == candidate.provider_profile_id
                && existing.url == candidate.url
        }) {
            candidates.push(candidate.clone());
        }
    }
    let mut retry = crate::retry::RetryState::new();
    let mut current = 0;
    let mut attempts = 0u64;
    let mut last_candidate = None;
    let mut input = InputAccumulator::new(inner.config.chunk_bytes);
    let mut last_error = PlaybackError::new(502, "Upstream did not provide stream data");
    'session: loop {
        if *stop.borrow() || *lost.borrow() {
            break;
        }
        retry.new_source(current);
        let candidate = &candidates[current];
        session.connecting.store(true, Ordering::Relaxed);
        let valid_url = reqwest::Url::parse(&candidate.url)
            .is_ok_and(|url| matches!(url.scheme(), "http" | "https"));
        let reservation = if !valid_url {
            Err(PlaybackError::new(
                502,
                "Invalid or unsupported upstream URL",
            ))
        } else if let Some(lease) = &lease {
            match lease
                .reserve_provider(ProviderReservation {
                    profile_id: candidate.provider_profile_id,
                    max_streams: candidate.provider_max_streams,
                    credential_pool: None,
                })
                .await
            {
                Ok(()) => Ok(()),
                Err(
                    dispatcharr_coordination::Error::ProfileFull
                    | dispatcharr_coordination::Error::CredentialFull
                    | dispatcharr_coordination::Error::CredentialConflict,
                ) => Err(PlaybackError::new(
                    503,
                    "All active M3U profiles have reached maximum connection limits",
                )),
                Err(error) => {
                    last_error = coordination_error(error);
                    break 'session;
                }
            }
        } else if candidate.provider_max_streams > 0 {
            Err(PlaybackError::new(
                503,
                "Capacity-limited provider requires Redis coordination",
            ))
        } else {
            Ok(())
        };
        match reservation {
            Err(error) => last_error = error,
            Ok(()) => {
                session.upstream_admitted.store(true, Ordering::Release);
                session.metadata_wake.notify_one();
                *session.current.lock().unwrap() = candidate.clone();
                loop {
                    if *stop.borrow() || *lost.borrow() {
                        break 'session;
                    }
                    if retry.failures > 0 {
                        tokio::select! {
                            _ = stop.changed() => break 'session,
                            _ = lost.changed() => break 'session,
                            _ = time::sleep(inner.config.retry_delay) => {},
                        }
                    }
                    if attempts > 0 {
                        session.reconnects.fetch_add(1, Ordering::Relaxed);
                    }
                    let discontinuity = if inner.config.source_initialization {
                        last_candidate.is_some_and(|previous| previous != current)
                    } else {
                        attempts > 0
                    };
                    last_candidate = Some(current);
                    attempts = attempts.saturating_add(1);
                    drive_candidate(
                        &inner,
                        &session,
                        candidate,
                        current,
                        discontinuity,
                        &mut input,
                        &mut retry,
                        &mut stop,
                        &mut lost,
                    )
                    .await;
                    session.connecting.store(true, Ordering::Relaxed);
                    if *stop.borrow() || *lost.borrow() {
                        break 'session;
                    }
                    last_error = PlaybackError::new(502, "Upstream did not provide stream data");
                    if retry.failure(Instant::now(), inner.config.retry_window)
                        > inner.config.max_retries
                    {
                        break;
                    }
                }
                // drive_candidate returned, so its Response/socket is dropped
                // before the old slot is freed. Keep the same owner/fence.
                let final_initializing_source = session.native_initializing.load(Ordering::Acquire)
                    && matches!(*session.startup.borrow(), Startup::Pending)
                    && (0..candidates.len()).all(|index| retry.tried.contains(&index));
                if let Some(lease) = &lease {
                    if !final_initializing_source {
                        if let Err(error) = lease.release_provider().await {
                            last_error = coordination_error(error);
                            break 'session;
                        }
                    }
                }
            }
        }
        if let Some(next) = (0..candidates.len()).find(|index| !retry.tried.contains(index)) {
            current = next;
            continue;
        }
        // Like Python cold startup, never cycle the exhausted candidate list
        // before media succeeds. Healthy playback permits bounded cooldown wraps.
        if !retry.had_media
            || candidates.len() < 2
            || retry.rotations >= inner.config.max_stream_switches
        {
            break;
        }
        retry.rotations += 1;
        tokio::select! {
            _ = stop.changed() => break,
            _ = lost.changed() => break,
            _ = time::sleep(inner.config.rotation_cooldown) => {},
        }
        retry.tried.clear();
        retry.tried.insert(current);
        current = (0..candidates.len())
            .find(|index| !retry.tried.contains(index))
            .unwrap();
    }
    if *lost.borrow() {
        last_error = PlaybackError::new(503, "Channel ownership was lost");
    }
    if session.native_initializing.load(Ordering::Acquire)
        && session.upstream_admitted.load(Ordering::Acquire)
        && matches!(*session.startup.borrow(), Startup::Pending)
        && !*stop.borrow()
        && !*lost.borrow()
    {
        // Exhausted cold inputs remain an initializing channel until native
        // viewer ghost/initialization expiry and the ordinary idle teardown.
        // Waiting on controls performs no retry loop or payload allocation.
        tokio::select! {_=stop.changed()=>{},_=lost.changed()=>{}}
    }
    session.channel.stop();
    session.stop.send_replace(true);
    if let Some(clients) = &inner.clients {
        let _ = clients.stop_local_channel(&key).await;
    }
    if let Some(mut task) = metadata_task {
        if time::timeout(Duration::from_secs(4), &mut task)
            .await
            .is_err()
        {
            task.abort();
            let _ = task.await;
        }
    }
    if let Some(lease) = lease {
        if let Some(inputs) = &inner.inputs {
            let _ = inputs
                .cleanup(&session.source.channel_id, lease.token())
                .await;
        }
        let owner = lease.token().to_owned();
        let _ = lease.release().await;
        if let Some(clients) = &inner.clients {
            let _ = clients.clear_channel_stop(&key, &owner).await;
        }
    }
    finish_session(&inner, &key, &session, last_error);
}

fn finish_session(inner: &Inner, key: &str, session: &Arc<Session>, failure: PlaybackError) {
    session.stop.send_replace(true);
    session.channel.stop();
    remove_session(inner, key, session);
    session.startup.send_replace(Startup::Failed(failure));
}

fn epoch_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

impl Session {
    fn record_received_bytes(&self, length: usize, now: f64) {
        let total = self
            .received_bytes
            .fetch_add(length as u64, Ordering::Relaxed)
            .saturating_add(length as u64);
        if crate::byte_reporting::report(
            total,
            now,
            &self.last_bytes_report_bits,
            &self.reported_bytes,
        ) {
            self.metadata_wake.notify_one();
        }
    }
    fn request_ready(&self, inner: &Inner) {
        if !matches!(*self.startup.borrow(), Startup::Pending)
            || self.ready_requested.swap(true, Ordering::AcqRel)
        {
            return;
        }
        if inner.config.source_initialization && inner.clients.is_some() {
            // In application mode only the metadata producer may acknowledge
            // readiness, after its Redis state write. Followers observe that
            // same committed state before returning their first response.
            if !self.following.load(Ordering::Acquire) {
                self.metadata_wake.notify_one();
            }
        } else {
            self.startup.send_replace(Startup::Ready);
        }
    }

    async fn publish_data(
        &self,
        data: Arc<[u8]>,
        discontinuity: bool,
    ) -> Result<u64, dispatcharr_stream_runtime::Error> {
        let publisher = self.publisher.lock().unwrap().clone();
        let shared = if let Some((store, owner)) = &publisher {
            match store
                .put(
                    &self.source.channel_id,
                    owner,
                    &data,
                    epoch_seconds(),
                    discontinuity,
                )
                .await
            {
                Ok(sequence) => Some(sequence),
                Err(_) => {
                    self.stop.send_replace(true);
                    return Err(dispatcharr_stream_runtime::Error::Stopped);
                }
            }
        } else {
            None
        };
        let local = match shared {
            Some(sequence) => self.channel.publish_numbered(
                data,
                discontinuity,
                Instant::now().into_std(),
                sequence,
            )?,
            None => self.channel.publish_marked(data, discontinuity)?,
        };
        let index = shared.unwrap_or(local);
        self.buffer_index.store(index, Ordering::Relaxed);
        Ok(index)
    }
}

async fn channel_metadata_worker(
    clients: ClientStore,
    session: Arc<Session>,
    owner: String,
    shared: bool,
    health_timing: health::Timing,
) {
    let fence = if owner.is_empty() {
        uuid_fence()
    } else {
        owner.clone()
    };
    let mut stop = session.stop.subscribe();
    let mut timer = time::interval(Duration::from_secs(1));
    timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut previous_state = String::new();
    let mut state_changed = session.started_at;
    loop {
        if *stop.borrow() {
            break;
        }
        let health_delay = {
            let next = session.health.lock().unwrap().next_check;
            Duration::from_secs_f64(
                (next - epoch_seconds()).clamp(0.0, health_timing.interval.as_secs_f64()),
            )
        };
        tokio::select! {
            _=stop.changed()=>break,
            _=timer.tick()=>{},
            _=session.metadata_wake.notified()=>{},
            _=time::sleep(health_delay)=>{
                session.health.lock().unwrap().tick_if_due(epoch_seconds(),session.buffer_index.load(Ordering::Relaxed),health_timing);
                continue;
            },
        }
        session.health.lock().unwrap().tick_if_due(
            epoch_seconds(),
            session.buffer_index.load(Ordering::Relaxed),
            health_timing,
        );
        if clients
            .channel_is_stopping(&session.source.channel_id)
            .await
            .unwrap_or(true)
        {
            session.stop.send_replace(true);
            session.channel.stop();
            break;
        }
        let promotion = session.ready_requested.load(Ordering::Acquire)
            && matches!(*session.startup.borrow(), Startup::Pending);
        let activation_clients = if promotion {
            match clients
                .channel_client_count(&session.source.channel_id)
                .await
            {
                Ok(count) => Some(count),
                Err(_) => {
                    session.stop.send_replace(true);
                    break;
                }
            }
        } else {
            None
        };
        let state = match &*session.startup.borrow() {
            Startup::Pending if activation_clients.is_some_and(|count| count > 0) => "active",
            Startup::Pending if activation_clients.is_some() => "waiting_for_clients",
            Startup::Pending if session.upstream_admitted.load(Ordering::Acquire) => "connecting",
            Startup::Pending => "initializing",
            Startup::Failed(_) => "error",
            Startup::Ready if session.connecting.load(Ordering::Relaxed) => "buffering",
            Startup::Ready => "active",
        };
        if state != previous_state {
            state_changed = epoch_seconds();
            previous_state = state.into();
        }
        let mut fields = BTreeMap::new();
        if let Some(count) = activation_clients {
            fields.insert("connection_ready_time".into(), epoch_seconds().to_string());
            fields.insert(
                "buffer_chunks".into(),
                session.buffer_index.load(Ordering::Relaxed).to_string(),
            );
            if count > 0 {
                fields.insert("clients_at_activation".into(), count.to_string());
            }
        }
        {
            let current = session.current.lock().unwrap();
            for (name, value) in &session.source.metadata {
                fields.insert(name.clone(), value.clone());
            }
            for (name, value) in &current.metadata {
                fields.insert(name.clone(), value.clone());
            }
            fields.insert("url".into(), current.url.clone());
            fields.insert("stream_id".into(), current.stream_id.to_string());
            if let Some(profile) = current.provider_profile_id {
                fields.insert("m3u_profile".into(), profile.to_string());
            }
        }
        for (name, value) in [
            ("state", state.to_string()),
            ("state_changed_at", state_changed.to_string()),
            ("owner", owner.clone()),
            ("stream_profile", session.source.profile_id.to_string()),
            ("channel_name", session.source.channel_name.clone()),
            ("init_time", session.started_at.to_string()),
            (
                "last_data",
                (session.last_data_ms.load(Ordering::Relaxed) as f64 / 1000.0).to_string(),
            ),
            ("worker_id", clients.worker_id().to_owned()),
        ] {
            fields.insert(name.into(), value);
        }
        let reported = session.reported_bytes.load(Ordering::Acquire);
        if reported > 0 {
            fields.insert("total_bytes".into(), reported.to_string());
        }
        let published = if shared {
            clients
                .publish_channel_shared(&session.source.channel_id, &owner, &fence, &fields)
                .await
        } else {
            clients
                .publish_channel(
                    &session.source.channel_id,
                    &owner,
                    &fence,
                    &fields,
                    session.buffer_index.load(Ordering::Relaxed),
                )
                .await
        };
        if published.is_err() {
            session.stop.send_replace(true);
            session.channel.stop();
            break;
        }
        if promotion {
            session.startup.send_replace(Startup::Ready);
        }
    }
    if shared {
        let _ = clients
            .remove_channel_shared(&session.source.channel_id, &fence)
            .await;
    } else {
        let _ = clients
            .remove_channel(&session.source.channel_id, &fence)
            .await;
    }
}

fn uuid_fence() -> String {
    format!("local-{}", uuid::Uuid::new_v4())
}

struct InputAccumulator {
    assembler: PacketAssembler,
    marker: DiscontinuityMarker,
    chunk: Vec<u8>,
    mark_chunk: bool,
}
struct ConnectionClock<'a>(&'a Session);
impl Drop for ConnectionClock<'_> {
    fn drop(&mut self) {
        self.0.health.lock().unwrap().connected = false;
    }
}
impl InputAccumulator {
    fn new(bytes: usize) -> Self {
        Self {
            assembler: PacketAssembler::default(),
            marker: DiscontinuityMarker::default(),
            chunk: Vec::with_capacity(bytes),
            mark_chunk: false,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn drive_candidate(
    inner: &Inner,
    session: &Session,
    candidate: &PlaybackCandidate,
    current: usize,
    discontinuity: bool,
    input: &mut InputAccumulator,
    retry: &mut crate::retry::RetryState,
    stop: &mut watch::Receiver<bool>,
    lost: &mut watch::Receiver<bool>,
) {
    if !inner.config.source_initialization {
        *input = InputAccumulator::new(inner.config.chunk_bytes);
    } else if discontinuity && !input.chunk.is_empty() {
        let data = std::mem::replace(
            &mut input.chunk,
            Vec::with_capacity(inner.config.chunk_bytes),
        );
        if session
            .publish_data(data.into(), input.mark_chunk)
            .await
            .is_err()
        {
            return;
        }
    }
    if discontinuity {
        input.assembler.reset();
        input.marker.arm();
        input.mark_chunk = true;
    }
    let Ok(attempt) =
        crate::upstream::Attempt::new(inner.config.connect_timeout, inner.config.read_timeout)
    else {
        return;
    };
    let Ok(request) = attempt.request(&candidate.url, &candidate.user_agent, &candidate.headers)
    else {
        return;
    };
    {
        let mut health = session.health.lock().unwrap();
        health.arm(epoch_seconds());
        health.connected = true;
    }
    let _connection_clock = ConnectionClock(session);
    let response = tokio::select! {
        _ = stop.changed() => return, _ = lost.changed() => return,
        response = request.send() => response,
    };
    let Ok(response) = response else { return };
    if response.status() != reqwest::StatusCode::OK || attempt.cookie_budget_exceeded() {
        return;
    }
    let Ok(mut bytes) = crate::upstream::body(response) else {
        return;
    };
    let InputAccumulator {
        assembler,
        marker,
        mark_chunk,
        chunk,
    } = input;
    let mut first_media = None;
    let mut stable_noted = false;
    let mut flush = time::interval_at(
        Instant::now() + inner.config.flush_interval,
        inner.config.flush_interval,
    );
    flush.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    loop {
        let mut published = false;
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            _ = lost.changed() => break,
            frame = bytes.next() => {
                let Some(Ok(frame)) = frame else { break };
                if frame.is_empty(){continue;}
                let now=epoch_seconds();
                session.record_received_bytes(frame.len(),now);
                session.last_data_ms.store((now*1000.0) as u64,Ordering::Relaxed);
                session.health.lock().unwrap().data(now);
                let mut failed = false;
                // One packet-aligned input window can finish at most one
                // publish chunk. Await Redis before processing the next window;
                // never accumulate an unbounded queue from a large HTTP frame.
                for input in frame.chunks(inner.config.chunk_bytes){
                    let ready = assemble_input_window(assembler, marker, mark_chunk, chunk, input, inner.config.chunk_bytes);
                    if let Some((data,discontinuity))=ready{
                        if !inner.config.source_initialization { marker.disarm(); }
                        if session.publish_data(data,discontinuity).await.is_err(){failed=true;break;}
                        published=true;
                    }
                }
                // Original stamps every complete packet in one incoming batch
                // before the first Redis publication disarms the next batch.
                // Pending packets in this same frame must retain those flags.
                if inner.config.source_initialization && published { marker.disarm(); }
                if failed { break; }
            }
            _ = flush.tick(), if inner.config.flush_partial_chunks => {
                if !chunk.is_empty() {
                    let data: Arc<[u8]> = std::mem::replace(chunk,
                        Vec::with_capacity(inner.config.chunk_bytes)).into();
                    if session.publish_data(data,*mark_chunk).await.is_err() { break; }
                    *mark_chunk=false;
                    marker.disarm();
                    published = true;
                }
            }
        }
        if published {
            retry.had_media = true;
            let began = *first_media.get_or_insert_with(Instant::now);
            session.connecting.store(false, Ordering::Relaxed);
            if session.buffer_index.load(Ordering::Relaxed) >= inner.config.initial_buffer_chunks {
                session.request_ready(inner);
            }
            if !stable_noted && began.elapsed() >= inner.config.stable_connection_threshold {
                retry.stable(current);
                stable_noted = true;
            }
        }
    }
    if inner.config.flush_partial_chunks
        && !chunk.is_empty()
        && !*stop.borrow()
        && !*lost.borrow()
        && session
            .publish_data(std::mem::take(chunk).into(), *mark_chunk)
            .await
            .is_ok()
    {
        retry.had_media = true;
        session.connecting.store(false, Ordering::Relaxed);
        if session.buffer_index.load(Ordering::Relaxed) >= inner.config.initial_buffer_chunks {
            session.request_ready(inner);
        }
    }
}

fn coordination_error(error: dispatcharr_coordination::Error) -> PlaybackError {
    use dispatcharr_coordination::Error;
    match error {
        Error::OwnerHeld => PlaybackError::new(503, "Channel is owned by another worker"),
        Error::ProfileFull | Error::CredentialFull => PlaybackError::new(
            503,
            "All active M3U profiles have reached maximum connection limits",
        ),
        Error::CredentialConflict | Error::InvalidRequest => {
            PlaybackError::new(503, "Provider reservation configuration is incompatible")
        }
        _ => PlaybackError::new(503, "Channel reservation is unavailable"),
    }
}

fn remove_session(inner: &Inner, key: &str, session: &Arc<Session>) {
    let mut sessions = inner.sessions.lock().unwrap();
    if sessions
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, session))
    {
        sessions.remove(key);
    }
}

// The production bounded input window; also exercised by the captured Source peer.
fn assemble_input_window(
    assembler: &mut PacketAssembler,
    marker: &mut DiscontinuityMarker,
    mark_chunk: &mut bool,
    chunk: &mut Vec<u8>,
    input: &[u8],
    chunk_bytes: usize,
) -> Option<(Arc<[u8]>, bool)> {
    let mut ready = None;
    assembler.feed(input, |packet| {
        let mut packet = *packet;
        marker.stamp(&mut packet);
        chunk.extend_from_slice(&packet);
        if chunk.len() == chunk_bytes {
            let data: Arc<[u8]> = std::mem::replace(chunk, Vec::with_capacity(chunk_bytes)).into();
            debug_assert!(ready.is_none());
            ready = Some((data, *mark_chunk));
            *mark_chunk = false;
        }
    });
    ready
}

#[cfg(test)]
#[path = "core_buffer_peer.rs"]
mod core_buffer_peer;
