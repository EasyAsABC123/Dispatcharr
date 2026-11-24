#[path = "output_profiles.rs"]
mod output_profiles;
#[path = "profile_process.rs"]
mod profile_process;
#[path = "redirect.rs"]
mod redirect;
#[path = "source_keepalive.rs"]
mod source_keepalive;
#[path = "source_switch.rs"]
mod source_switch;
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
#[path = "input_writer.rs"]
mod input_writer;
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

/// Three bounded copied values; no settings tree or cache permit is retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HealthTimeouts {
    pub startup: Duration,
    pub connection: Duration,
    pub initialization: Duration,
}
impl Default for HealthTimeouts {
    fn default() -> Self {
        Self {
            startup: Duration::from_secs(8),
            connection: Duration::from_secs(10),
            initialization: Duration::from_secs(60),
        }
    }
}
/// Optional real consumer capability. The existing static engine is unchanged
/// unless the owner explicitly supplies a source before creating any clones.
pub trait HealthSettingsSource: Send + Sync {
    fn snapshot(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<HealthTimeouts, PlaybackError>> + Send + '_>>;
}
async fn effective_health_timing(
    base: health::Timing,
    source: Option<&Arc<dyn HealthSettingsSource>>,
) -> Result<health::Timing, PlaybackError> {
    let Some(source) = source else {
        return Ok(base);
    };
    // Unsupported shapes and physical read errors stay explicit. In particular,
    // a decode refusal must never be silently treated as valid default settings.
    let value = source.snapshot().await?;
    Ok(health::Timing {
        startup: value.startup,
        connection: value.connection,
        initialization: value.initialization,
        interval: base.interval,
    })
}

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
    source_http_retry_backoff: bool,
    health_settings: Option<Arc<dyn HealthSettingsSource>>,
    live_client_connect: Option<Arc<dyn dispatcharr_contracts::LiveClientConnectObserver>>,
    profiles: Option<Arc<output_profiles::Profiles>>,
    local_source_switches: bool,
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
    buffer_index: Arc<AtomicU64>,
    last_data_ms: AtomicU64,
    started_at: f64,
    current: Mutex<PlaybackCandidate>,
    connecting: AtomicBool,
    following: AtomicBool,
    manager_visible: AtomicBool,
    health: Mutex<health::Health>,
    idle_watching: AtomicBool,
    /// Internal profile consumers hold channel demand without a raw data
    /// subscription, client metadata, user slot or provider reservation.
    profile_demands: AtomicUsize,
    native_initializing: AtomicBool,
    upstream_admitted: AtomicBool,
    provider_epoch: AtomicU64,
    source_slot_published: AtomicBool,
    ready_requested: AtomicBool,
    metadata_wake: Notify,
    publisher: Mutex<Option<input_writer::InputWriter>>,
    input_owner: Mutex<Option<String>>,
    source_switch: Option<Box<source_switch::Control>>,
    _permit: Arc<OwnedSemaphorePermit>,
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
                source_http_retry_backoff: false,
                health_settings: None,
                live_client_connect: None,
                profiles: None,
                local_source_switches: false,
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

    /// Use Original backoff for registered direct-HTTP TS playback in Main.
    /// Existing constructors continue to use the exact configured delay.
    pub fn with_source_http_retry_backoff(mut self) -> Result<Self, PlaybackError> {
        let inner = Arc::get_mut(&mut self.inner).ok_or_else(|| {
            PlaybackError::new(
                500,
                "HTTP retry backoff must be bound before engine sharing",
            )
        })?;
        inner.source_http_retry_backoff = true;
        Ok(self)
    }

    /// Bind before cloning/starting the engine. Main must construct the Core
    /// reader from its pre-engine AuthService, avoiding an EngineControl cycle.
    pub fn with_health_settings(
        mut self,
        source: Arc<dyn HealthSettingsSource>,
    ) -> Result<Self, PlaybackError> {
        let inner = Arc::get_mut(&mut self.inner).ok_or_else(|| {
            PlaybackError::new(500, "Health settings must be bound before engine sharing")
        })?;
        inner.health_settings = Some(source);
        Ok(self)
    }

    /// Enable owner-local source switches before starting the engine.
    pub fn with_local_source_switches(mut self) -> Result<Self, PlaybackError> {
        let inner = Arc::get_mut(&mut self.inner).ok_or_else(|| {
            PlaybackError::new(500, "Source switches must be bound before engine sharing")
        })?;
        inner.local_source_switches = true;
        Ok(self)
    }

    /// Select an unambiguous, already-resolved raw HTTP target on the current
    /// unlimited profile. The caller must have checked control authorization.
    pub async fn switch_local_source(
        &self,
        channel: &str,
        stream_id: i64,
    ) -> Result<String, PlaybackError> {
        let session = self
            .inner
            .sessions
            .lock()
            .unwrap()
            .get(channel)
            .cloned()
            .ok_or_else(|| PlaybackError::new(404, "Channel not found"))?;
        let control = session
            .source_switch
            .as_ref()
            .ok_or_else(|| PlaybackError::new(409, "Local source switch is unavailable"))?;
        let (reply, url) = {
            let active = session.current.lock().unwrap();
            if !source_switch::supported(&active) {
                return Err(PlaybackError::new(
                    409,
                    "Active source is outside the local switch scope",
                ));
            }
            let current = source_switch::source(&active);
            let target = (stream_id, current.1);
            if target == current {
                return Err(PlaybackError::new(
                    409,
                    "Requested stream is already active",
                ));
            }
            let primary = session.source.primary();
            let mut matches = std::iter::once(&primary)
                .chain(&session.source.alternates)
                .filter(|candidate| {
                    source_switch::source(candidate) == target
                        && source_switch::supported(candidate)
                });
            let Some(candidate) = matches.next().filter(|_| matches.next().is_none()) else {
                return Err(PlaybackError::new(
                    404,
                    "Requested stream is outside the local switch scope",
                ));
            };
            if candidate.url == active.url {
                return Err(PlaybackError::new(409, "Requested URL is already active"));
            }
            (control.submit(current, target)?, candidate.url.clone())
        };
        // Once a producer begins a handoff it owns the bounded transition even
        // if this waiter times out. A timeout never queues another handoff.
        match time::timeout(self.inner.config.startup_timeout, reply).await {
            Ok(Ok(result)) => result.map(|()| url),
            Ok(Err(_)) => Err(PlaybackError::new(
                502,
                "Source attempt ended before switching",
            )),
            Err(_) => Err(PlaybackError::new(
                504,
                "Local source switch was not confirmed",
            )),
        }
    }

    pub(crate) fn local_source_switch_body_deadline(&self) -> Duration {
        self.inner.config.startup_timeout
    }

    pub(crate) fn local_source_switch_worker(&self) -> Option<&str> {
        self.inner
            .clients
            .as_ref()
            .map(|clients| clients.worker_id())
    }

    /// Sample the same TS settings slot used by channel health. A standalone
    /// gateway without a dynamic source keeps its configured startup grace.
    pub async fn hls_initialization_grace(
        &self,
        fallback: Duration,
    ) -> Result<Duration, PlaybackError> {
        match &self.inner.health_settings {
            Some(source) => Ok(source.snapshot().await?.initialization),
            None => Ok(fallback),
        }
    }

    pub fn with_live_client_connect(
        mut self,
        observer: Arc<dyn dispatcharr_contracts::LiveClientConnectObserver>,
    ) -> Result<Self, PlaybackError> {
        let inner = Arc::get_mut(&mut self.inner).ok_or_else(|| {
            PlaybackError::new(500, "Connect observer must be bound before engine sharing")
        })?;
        inner.live_client_connect = Some(observer);
        Ok(self)
    }

    /// Authorization/resolution must run before calling this method for each viewer.
    pub async fn play(&self, mut source: PlaybackSource) -> Result<Response<Body>, PlaybackError> {
        if let Some(profile) = source.output_profile.take() {
            return self
                .play_output_profile(source, profile.id, profile.command)
                .await;
        }
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
        if let Some(profile) = source.output_profile.take() {
            return self
                .open_output_profile(source, profile.id, profile.command)
                .await;
        }
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
        let connect = self.inner.live_client_connect.as_ref().and_then(|_| {
            let viewer = source.viewer.as_ref()?.clone();
            let client = client.as_ref()?;
            viewer.user_id?;
            Some(dispatcharr_contracts::LiveClientConnectEvent {
                channel_id: source.channel_id.clone(),
                channel_name: source.channel_name.clone(),
                client_id: client.id().to_owned(),
                viewer,
            })
        });
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
                let primary = source.primary();
                let source_switch = (register_client
                    && self.inner.local_source_switches
                    && source.output_format == dispatcharr_contracts::OutputFormat::MpegTs
                    && source_switch::supported(&primary)
                    && source.alternates.iter().any(|candidate| {
                        source_switch::supported(candidate)
                            && candidate.provider_profile_id == primary.provider_profile_id
                            && candidate.stream_id != primary.stream_id
                            && candidate.url != primary.url
                    }))
                .then(|| Box::new(source_switch::Control::new()));
                let current = Mutex::new(primary);
                let session = Arc::new(Session {
                    source,
                    channel,
                    stop,
                    startup,
                    received_bytes: AtomicU64::new(0),
                    reported_bytes: AtomicU64::new(0),
                    last_bytes_report_bits: AtomicU64::new(epoch_seconds().to_bits()),
                    reconnects: AtomicU64::new(0),
                    buffer_index: Arc::new(AtomicU64::new(0)),
                    last_data_ms: AtomicU64::new(0),
                    started_at: epoch_seconds(),
                    current,
                    connecting: AtomicBool::new(true),
                    following: AtomicBool::new(false),
                    manager_visible: AtomicBool::new(false),
                    health: Mutex::new(health::Health::new(epoch_seconds())),
                    idle_watching: AtomicBool::new(false),
                    profile_demands: AtomicUsize::new(0),
                    native_initializing: AtomicBool::new(
                        register_client && self.inner.config.source_initialization,
                    ),
                    upstream_admitted: AtomicBool::new(false),
                    provider_epoch: AtomicU64::new(0),
                    source_slot_published: AtomicBool::new(false),
                    ready_requested: AtomicBool::new(false),
                    metadata_wake: Notify::new(),
                    publisher: Mutex::new(None),
                    input_owner: Mutex::new(None),
                    source_switch,
                    _permit: Arc::new(permit),
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
            let worker = Arc::new(WorkerCount(self.inner.clone()));
            tokio::spawn(async move { run_upstream(weak, key, running, worker).await });
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
                retained: Some(Arc::new((state._guard.clone(), state.client.clone()))),
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
        // Both local and remote Source generators use the same empty-read
        // backoff. Local managers retain their in-memory health decision;
        // followers consult only the canonical raw ingress timestamp.
        let source_keepalive_enabled = register_client && self.inner.config.source_initialization;
        let source_health_timing = self.inner.config.health_timing();
        let source_health_settings = self.inner.health_settings.clone();
        let stream = async_stream::stream! {
            let mut state = state;
            let mut connect = connect;
            let stream_started = std::time::Instant::now();
            let mut stopped=state.client.as_ref().map(ClientLease::stopped);
            let mut last_media=Instant::now();
            let mut source_keepalive=source_keepalive::SourceKeepalive::new();
            let mut remote_health=history::RemoteHealth::default();
            'transport: loop {
                if stopped.as_ref().is_some_and(|control|*control.borrow()) {break;}
                // Keep a historical read alive across keepalive ticks. Dropping
                // and recreating it each tick would queue cancelled Redis reads.
                let cursor=AtomicU64::new(state.viewer.next_sequence());
                let waiting=history::next_observed(&mut state.viewer,history.as_ref(),Some(&cursor));
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
                        _=time::sleep(if source_keepalive_enabled {source_keepalive.delay()}else{keepalive_interval})=>{
                            if !register_client {continue;}
                            if source_keepalive_enabled {
                                let session=&state._guard.session;
                                let at_head=cursor.load(Ordering::Acquire)>session.buffer_index.load(Ordering::Acquire);
                                let healthy=if session.manager_visible.load(Ordering::Acquire) {
                                    session.health.lock().unwrap().healthy
                                }else if !source_keepalive.health_check_due(at_head) {
                                    true
                                }else if let Some(history)=history.as_ref() {
                                    let timeout=async {
                                        effective_health_timing(source_health_timing,source_health_settings.as_ref()).await.map(|v|v.connection)
                                    };
                                    !history.source_unhealthy(&mut remote_health,epoch_seconds(),timeout).await
                                }else {
                                    // Source with no Redis client returns false from
                                    // _should_send_keepalive (do not invent unhealthy).
                                    true
                                };
                                match source_keepalive.empty(at_head,healthy,Instant::now().into_std(),keepalive_interval,max_keepalive_duration) {
                                    source_keepalive::Decision::Wait=>continue,
                                    source_keepalive::Decision::Stop=>break 'transport,
                                    source_keepalive::Decision::Send=>{},
                                }
                            }else if last_media.elapsed()>=max_keepalive_duration {break 'transport;}
                            if let Some(client)=&state.client {client.record_keepalive(188);}
                            let frame=KeepaliveFrame {_client:state.client.clone(),_guard:state._guard.clone()};
                            yield Ok::<MediaFrame,io::Error>(MediaFrame {data:Bytes::from_owner(frame),discontinuity:false});
                        }
                    }
                };
                match next {
                    Ok(Some(chunk)) => {
                        if let Some(event) = connect.take() {
                            observe_client_connect(
                                &state._guard,
                                state.client.as_ref(),
                                event,
                                stream_started,
                            ).await;
                        }
                        last_media=Instant::now();
                        source_keepalive.received();
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
        if let Some(profiles) = &self.inner.profiles {
            profiles.request_stop();
        }
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
                        .profiles
                        .as_ref()
                        .is_none_or(|profiles| profiles.reader_settled())
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

    /// Source Recording stop/delete targets exact DVR UA identity. A sibling
    /// recording or ordinary viewer keeps the shared channel/upstream alive.
    pub async fn stop_recording_clients(
        &self,
        channel: &str,
        recording: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<usize, PlaybackError> {
        if recording <= 0 || recording > i64::from(i32::MAX) {
            return Err(PlaybackError::new(400, "Invalid recording identity"));
        }
        let clients = self
            .inner
            .clients
            .as_ref()
            .ok_or_else(|| PlaybackError::new(503, "Redis client metadata unavailable"))?
            .clone();
        let channel = channel.to_owned();
        tokio::spawn(async move {
            let _owner = owner;
            let rows = clients
                .channel_clients(&channel)
                .await
                .map_err(|e| client_error(e, 0))?;
            let expected = format!("Dispatcharr-DVR/recording-{recording}");
            let mut stopped = 0usize;
            for row in rows {
                if row.get("user_agent").and_then(serde_json::Value::as_str)
                    != Some(expected.as_str())
                {
                    continue;
                }
                let Some(id) = row.get("client_id").and_then(serde_json::Value::as_str) else {
                    continue;
                };
                match clients.stop_client(&channel, id).await {
                    Ok(_) => stopped += 1,
                    Err(_) => {
                        eprintln!("Recording client stop failed; preserving other clients")
                    }
                }
            }
            Ok(stopped)
        })
        .await
        .map_err(|_| PlaybackError::new(503, "Recording client control supervisor failed"))?
    }

    /// One bounded snapshot from this listener's client store. A deferred
    /// handoff uses these identities once, independent of later reconnects.
    pub async fn snapshot_local_recording_clients(
        &self,
        channel: &str,
        recording: i64,
    ) -> Result<Vec<String>, PlaybackError> {
        if recording <= 0 || recording > i64::from(i32::MAX) {
            return Err(PlaybackError::new(400, "Invalid recording identity"));
        }
        let clients = self
            .inner
            .clients
            .as_ref()
            .ok_or_else(|| PlaybackError::new(503, "Redis client metadata unavailable"))?;
        let rows = clients
            .channel_clients(channel)
            .await
            .map_err(|error| client_error(error, 0))?;
        Ok(local_recording_client_ids(
            &rows,
            recording,
            clients.worker_id(),
        ))
    }

    pub async fn stop_local_recording_clients(
        &self,
        channel: &str,
        ids: Vec<String>,
    ) -> Result<(), PlaybackError> {
        let clients = self
            .inner
            .clients
            .as_ref()
            .ok_or_else(|| PlaybackError::new(503, "Redis client metadata unavailable"))?;
        for id in ids {
            clients
                .stop_local_client(channel, &id)
                .await
                .map_err(|error| client_error(error, 0))?;
        }
        Ok(())
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

fn local_recording_client_ids(
    rows: &[serde_json::Value],
    recording: i64,
    worker: &str,
) -> Vec<String> {
    let expected = format!("Dispatcharr-DVR/recording-{recording}");
    rows.iter()
        .filter(|row| {
            row["user_agent"].as_str() == Some(expected.as_str())
                && row["worker_id"].as_str() == Some(worker)
                && row["output_format"].as_str() == Some("mpegts")
                && row["ip_address"]
                    .as_str()
                    .and_then(|ip| ip.parse::<std::net::IpAddr>().ok())
                    .is_some_and(|ip| ip.to_canonical().is_loopback())
        })
        .filter_map(|row| row["client_id"].as_str().map(str::to_owned))
        .collect()
}

#[cfg(test)]
#[test]
fn local_recording_snapshot_excludes_live_sibling_and_foreign_clients() {
    let row = |id, user_agent, worker, ip, format| {
        serde_json::json!({
            "client_id": id,
            "user_agent": user_agent,
            "worker_id": worker,
            "ip_address": ip,
            "output_format": format,
        })
    };
    let dvr = "Dispatcharr-DVR/recording-17";
    let rows = [
        row("first-dvr", dvr, "local", "127.0.0.1", "mpegts"),
        row("second-dvr", dvr, "local", "::1", "mpegts"),
        row("foreign", dvr, "foreign", "127.0.0.1", "mpegts"),
        row("remote", dvr, "local", "192.0.2.1", "mpegts"),
        row("live", "live-client", "local", "127.0.0.1", "mpegts"),
        row("hls", dvr, "local", "127.0.0.1", "hls"),
        row(
            "sibling-dvr",
            "Dispatcharr-DVR/recording-18",
            "local",
            "::1",
            "mpegts",
        ),
    ];
    let ids = local_recording_client_ids(&rows, 17, "local");
    assert_eq!(ids, ["first-dvr", "second-dvr"]);
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
            || self.session.has_local_demand()
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
                    if session.has_local_demand() {
                        session.idle_watching.store(false, Ordering::Release);
                        // Close the last-drop/reset race without creating duplicate watchers.
                        if !session.has_local_demand()
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
                            if !session.has_local_demand()
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

async fn observe_client_connect(
    guard: &ViewerGuard,
    client: Option<&ClientLease>,
    event: dispatcharr_contracts::LiveClientConnectEvent,
    stream_started: std::time::Instant,
) {
    let Some(inner) = guard.inner.upgrade() else {
        return;
    };
    let Some(observer) = inner.live_client_connect.clone() else {
        return;
    };
    let owner = {
        // Shutdown takes the same registry lock after closing slots. It cannot
        // certify retirement between successful admission and marking the work.
        let sessions = inner.sessions.lock().unwrap();
        if inner.slots.is_closed()
            || *guard.session.stop.borrow()
            || !sessions
                .get(&guard.key)
                .is_some_and(|active| Arc::ptr_eq(active, &guard.session))
        {
            return;
        }
        inner.workers.fetch_add(1, Ordering::AcqRel);
        Arc::new(WorkerCount(inner.clone()))
    };
    // The event owns only the worker count and immutable viewer fields. A
    // webhook cannot keep a disconnected viewer or a provider slot alive.
    observer.before_media(event.clone(), owner.clone()).await;
    if let Some(client) = client {
        let loss = Arc::downgrade(&inner);
        client.observe_natural_retirement(stream_started, move |elapsed, bytes_sent, admission| {
            if loss.upgrade().is_none_or(|engine| engine.slots.is_closed()) {
                return None;
            }
            Some(Box::pin(async move {
                observer
                    .after_natural_retirement(
                        dispatcharr_contracts::LiveClientDisconnectEvent {
                            connected: event,
                            elapsed,
                            bytes_sent,
                        },
                        owner,
                        admission,
                    )
                    .await;
            }))
        });
    }
}

struct WorkerCount(Arc<Inner>);
impl Drop for WorkerCount {
    fn drop(&mut self) {
        self.0.workers.fetch_sub(1, Ordering::AcqRel);
        self.0.workers_done.notify_waiters();
    }
}

#[cfg(test)]
#[tokio::test]
async fn shared_live_observer_owner_retires_once_and_holds_engine_drain() {
    let engine = StreamEngine::new(EngineConfig::default()).unwrap();
    engine.inner.workers.fetch_add(1, Ordering::AcqRel);
    let connect = Arc::new(WorkerCount(engine.inner.clone()));
    let disconnect = connect.clone();

    drop(connect);
    assert_eq!(engine.inner.workers.load(Ordering::Acquire), 1);
    assert!(!engine.shutdown_wait(Duration::ZERO).await);
    drop(disconnect);
    assert_eq!(engine.inner.workers.load(Ordering::Acquire), 0);
    assert!(engine.shutdown_wait(Duration::from_millis(100)).await);
}

fn supported_input_url(candidate: &PlaybackCandidate) -> bool {
    // Source promotes RTSP/RTP/UDP to a configured input process.
    reqwest::Url::parse(&candidate.url).is_ok_and(|url| {
        matches!(url.scheme(), "http" | "https")
            || (candidate.input_command.is_some()
                && matches!(url.scheme(), "rtsp" | "rtp" | "udp")
                && url.host_str().is_some_and(|host| !host.is_empty()))
    })
}

fn same_live_reservation(current: &PlaybackCandidate, next: &PlaybackCandidate) -> bool {
    current.input_command.is_none()
        && next.input_command.is_none()
        && current.stream_id != next.stream_id
        && current.url != next.url
        && current.provider_max_streams > 0
        && current.provider_max_streams == next.provider_max_streams
        && current.provider_profile_id.is_some()
        && current.provider_profile_id == next.provider_profile_id
        && current
            .credential_pool
            .as_ref()
            .map(|pool| (&pool.counter_key, pool.max_streams))
            == next
                .credential_pool
                .as_ref()
                .map(|pool| (&pool.counter_key, pool.max_streams))
        && supported_input_url(next)
}

#[cfg(test)]
mod retained_source_tests {
    use super::*;
    use dispatcharr_contracts::PlaybackCredentialPool;

    fn candidate(id: i64) -> PlaybackCandidate {
        PlaybackCandidate {
            input_command: None,
            stream_id: id,
            provider_profile_id: Some(3),
            provider_max_streams: 1,
            credential_pool: Some(PlaybackCredentialPool {
                counter_key: "server_group_connections:7:owned".to_owned(),
                max_streams: 1,
            }),
            url: format!("http://owned-origin/{id}.ts"),
            user_agent: String::new(),
            headers: Vec::new(),
            metadata: Vec::new(),
        }
    }

    #[test]
    fn only_distinct_raw_inputs_with_equal_finite_reservation_identity_reuse_capacity() {
        let initial = candidate(11);
        let alternate = candidate(12);
        assert!(same_live_reservation(&initial, &alternate));
        let duplicate_url = PlaybackCandidate {
            url: initial.url.clone(),
            ..candidate(13)
        };
        let pool = [initial.clone(), duplicate_url, alternate.clone()];
        let mut retry = crate::retry::RetryState::new();
        retry.new_source(0);
        let next = retry.next_candidate(&pool, Some(&initial.url)).unwrap();
        assert_eq!(next, 2);
        assert!(same_live_reservation(&initial, &pool[next]));
        assert_eq!(retry.next_candidate(&pool, Some(&initial.url)), Some(next));
        for different in [
            PlaybackCandidate {
                stream_id: initial.stream_id,
                ..alternate.clone()
            },
            PlaybackCandidate {
                url: initial.url.clone(),
                ..alternate.clone()
            },
            PlaybackCandidate {
                url: "rtsp://owned-origin/fallback".to_owned(),
                ..alternate.clone()
            },
            PlaybackCandidate {
                provider_profile_id: None,
                ..alternate.clone()
            },
            PlaybackCandidate {
                provider_profile_id: Some(4),
                ..alternate.clone()
            },
            PlaybackCandidate {
                provider_max_streams: 0,
                ..alternate.clone()
            },
            PlaybackCandidate {
                provider_max_streams: 2,
                ..alternate.clone()
            },
            PlaybackCandidate {
                credential_pool: None,
                ..alternate.clone()
            },
            PlaybackCandidate {
                credential_pool: Some(PlaybackCredentialPool {
                    counter_key: "server_group_connections:7:different".to_owned(),
                    max_streams: 1,
                }),
                ..alternate.clone()
            },
            PlaybackCandidate {
                credential_pool: Some(PlaybackCredentialPool {
                    counter_key: "server_group_connections:7:owned".to_owned(),
                    max_streams: 2,
                }),
                ..alternate
            },
        ] {
            assert!(
                !same_live_reservation(&initial, &different),
                "{different:?}"
            );
        }
    }
}

async fn run_upstream(
    weak: Weak<Inner>,
    key: String,
    session: Arc<Session>,
    worker: Arc<WorkerCount>,
) {
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
            Ok(lease) => Some(Arc::new(lease)),
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
        match input_writer::InputWriter::new(
            inputs.clone(),
            session.source.channel_id.clone(),
            lease.token().to_owned(),
            session.channel.clone(),
            session.buffer_index.clone(),
            inner.config.source_initialization,
            Some(session._permit.clone()),
        )
        .await
        {
            Ok(writer) => *session.publisher.lock().unwrap() = Some(writer),
            Err(_) => {
                session.stop.send_replace(true);
            }
        }
        *session.input_owner.lock().unwrap() = Some(lease.token().to_owned());
    }
    // Source selects/reuses public assignments before initialize_channel writes
    // new metadata. Do not overwrite stale metadata into initializing first.
    let mut assignment_error = None;
    let mut initial_assignment = None;
    let mut preview_borrowed = false;
    let preview_scope = session
        .source
        .metadata
        .iter()
        .find(|(key, _)| key == "source_preview")
        .map(|(_, v)| serde_json::from_str::<serde_json::Value>(v));
    let mut preview_assignment = None;
    if let Some(scope) = preview_scope {
        let parsed = scope
            .ok()
            .and_then(|v| {
                Some((
                    v.get("stream")?.as_i64()?,
                    v.get("hash")?.as_str()?.to_owned(),
                    v.get("scoped")?.as_bool()?,
                ))
            })
            .and_then(|(stream, hash, scoped)| {
                dispatcharr_coordination::LivePreviewAssignment::new(
                    stream,
                    &hash,
                    session.source.provider_profile_id?,
                    scoped,
                )
                .ok()
            });
        match (parsed, lease.as_ref()) {
            (Some(assignment), Some(owner)) => {
                match owner.prepare_preview_assignment(assignment.clone()).await {
                    Ok(borrowed) => {
                        preview_borrowed = borrowed;
                        preview_assignment = Some(assignment);
                    }
                    Err(error) => assignment_error = Some(coordination_error(error)),
                }
            }
            _ => {
                assignment_error = Some(PlaybackError::new(
                    503,
                    "Preview owner or assignment unavailable",
                ))
            }
        }
    }

    let numeric_channel = if !session
        .source
        .metadata
        .iter()
        .any(|(key, _)| key == "source_preview")
        && uuid::Uuid::parse_str(&session.source.channel_id)
            .is_ok_and(|id| id.to_string() == session.source.channel_id)
    {
        session
            .source
            .metadata
            .iter()
            .find(|(key, _)| key == "channel_id")
            .and_then(|(_, value)| value.parse::<i64>().ok())
    } else {
        None
    };
    if let (Some(coordinator), Some(numeric)) = (&inner.coordinator, numeric_channel) {
        match coordinator
            .observe_redirect_assignment(
                numeric,
                &session.source.channel_id,
                Arc::new((session.clone(), worker.clone())),
            )
            .await
        {
            Ok(value) => initial_assignment = value,
            Err(error) => assignment_error = Some(coordination_error(error)),
        }
    }
    if session
        .source
        .metadata
        .iter()
        .any(|(key, value)| key == "source_assignment_required" && value == "true")
        && initial_assignment.is_none()
        && assignment_error.is_none()
    {
        assignment_error = Some(PlaybackError::new(
            503,
            "Observed Source assignment changed before raw adoption",
        ));
    }
    let metadata_task = inner.clients.as_ref().map(|clients| {
        let clients = clients.clone();
        let session = session.clone();
        let owner = lease
            .as_ref()
            .map_or(String::new(), |lease| lease.token().to_owned());
        let shared = inner.inputs.is_some();
        let health_timing = inner.config.health_timing();
        let health_settings = inner.health_settings.clone();
        tokio::spawn(async move {
            channel_metadata_worker(
                clients,
                session,
                owner,
                shared,
                health_timing,
                health_settings,
            )
            .await
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
    let mut current = if let Some(assignment) = initial_assignment.as_ref() {
        match candidates.iter().position(|candidate| {
            candidate.stream_id == assignment.stream_id()
                && candidate.provider_profile_id == Some(assignment.profile_id())
        }) {
            Some(index) => index,
            None => {
                assignment_error=Some(PlaybackError::new(501,"Raw reuse of an assignment outside current SQL candidates requires the retained-row resolver"));
                0
            }
        }
    } else {
        0
    };
    let mut attempts = 0u64;
    let mut retained_source_slot = false;
    let mut last_candidate = None;
    let mut capacity_wait = (numeric_channel.is_some()
        && session.native_initializing.load(Ordering::Acquire)
        && lease.is_some()
        && initial_assignment.is_none())
    .then(|| crate::retry::ProviderCapacityWait::new(Instant::now()));
    let mut input = InputAccumulator::new(inner.config.chunk_bytes);
    let mut last_error = PlaybackError::new(502, "Upstream did not provide stream data");
    let mut pending_switch = None;
    'session: loop {
        if let Some(error) = assignment_error.take() {
            last_error = error;
            break;
        }
        if *stop.borrow() || *lost.borrow() {
            break;
        }
        retry.new_source(current);
        let candidate = &candidates[current];
        session.connecting.store(true, Ordering::Relaxed);
        let keep_source_slot = std::mem::take(&mut retained_source_slot);
        let source_borrowed = initial_assignment.is_some();
        let bytes_at_admission = session.received_bytes.load(Ordering::Relaxed);
        let mut provider_full = false;
        let reservation = if !supported_input_url(candidate) {
            Err(PlaybackError::new(
                502,
                "Invalid or unsupported upstream URL",
            ))
        } else if keep_source_slot || std::mem::take(&mut preview_borrowed) {
            Ok(())
        } else if let Some(assignment) = initial_assignment.take() {
            match lease.as_ref().ok_or_else(||PlaybackError::new(503,"Raw assignment owner unavailable")) {
                Ok(lease)=>match lease.adopt_source_assignment(&assignment).await {
                    Ok(())=>Ok(()),
                    Err(dispatcharr_coordination::Error::AlreadyReserved)=>Err(PlaybackError::new(501,"Native live-owner reassignment requires a separate owner-loss adoption boundary")),
                    Err(error)=>{last_error=coordination_error(error);break 'session;},
                },
                Err(error)=>Err(error),
            }
        } else if let Some(lease) = &lease {
            match lease
                .reserve_provider(ProviderReservation {
                    profile_id: candidate.provider_profile_id,
                    max_streams: candidate.provider_max_streams,
                    credential_pool: candidate.credential_pool.as_ref().map(|pool| {
                        dispatcharr_coordination::CredentialPool {
                            counter_key: pool.counter_key.clone(),
                            max_streams: pool.max_streams,
                        }
                    }),
                })
                .await
            {
                Ok(()) => Ok(()),
                Err(
                    dispatcharr_coordination::Error::ProfileFull
                    | dispatcharr_coordination::Error::CredentialFull
                    | dispatcharr_coordination::Error::CredentialConflict,
                ) => {
                    provider_full = true;
                    Err(PlaybackError::new(
                        503,
                        crate::retry::PROVIDER_CAPACITY_ERROR,
                    ))
                }
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
        if !provider_full {
            capacity_wait = None;
        }
        let mut requested_switch = None;
        match reservation {
            Err(error) => last_error = error,
            Ok(()) => {
                // Only an actual Channel UUID resolved by Store carries its
                // numeric Source Channel PK. Stream/profile-scoped previews
                // never publish a channel's bare assignment aliases.
                if let (Some(preview), Some(owner), Some(clients)) = (
                    preview_assignment.as_ref(),
                    lease.as_ref(),
                    inner.clients.as_ref(),
                ) {
                    let assignment =
                        match preview.with_profile(candidate.provider_profile_id.unwrap_or(0)) {
                            Ok(value) => value,
                            Err(error) => {
                                last_error = coordination_error(error);
                                break 'session;
                            }
                        };
                    if let Err(error) = owner
                        .publish_preview_assignment(assignment, clients.channel_metadata_ttl())
                        .await
                    {
                        last_error = coordination_error(error);
                        break 'session;
                    }
                    session.source_slot_published.store(true, Ordering::Release);
                } else if preview_assignment.is_some() {
                    last_error =
                        PlaybackError::new(503, "Preview metadata publication unavailable");
                    break 'session;
                } else if uuid::Uuid::parse_str(&session.source.channel_id)
                    .is_ok_and(|id| id.to_string() == session.source.channel_id)
                {
                    if let (Some(lease), Some(numeric)) = (
                        &lease,
                        session
                            .source
                            .metadata
                            .iter()
                            .find(|(key, _)| key == "channel_id")
                            .and_then(|(_, value)| value.parse::<i64>().ok()),
                    ) {
                        let assignment = match dispatcharr_coordination::LiveChannelAssignment::new(
                            numeric,
                            candidate.stream_id,
                        ) {
                            Ok(value) => value,
                            Err(error) => {
                                last_error = coordination_error(error);
                                break 'session;
                            }
                        };
                        let published = if keep_source_slot {
                            lease
                                .switch_active_source_with_reservation(
                                    candidate.stream_id,
                                    candidate
                                        .provider_profile_id
                                        .expect("retained provider profile"),
                                )
                                .await
                        } else if let Some(clients) = inner.clients.as_ref() {
                            lease
                                .publish_source_assignment_with_metadata(
                                    assignment,
                                    clients.channel_metadata_ttl(),
                                )
                                .await
                        } else {
                            lease.publish_source_assignment(assignment).await
                        };
                        if let Err(error) = published {
                            last_error = coordination_error(error);
                            break 'session;
                        }
                        session
                            .source_slot_published
                            .store(inner.clients.is_some(), Ordering::Release);
                    }
                }
                *session.current.lock().unwrap() = candidate.clone();
                session.provider_epoch.fetch_add(1, Ordering::AcqRel);
                session.upstream_admitted.store(true, Ordering::Release);
                session.metadata_wake.notify_one();
                loop {
                    if *stop.borrow() || *lost.borrow() {
                        break 'session;
                    }
                    if retry.failures > 0 {
                        let delay = if inner.source_http_retry_backoff
                            && session.native_initializing.load(Ordering::Acquire)
                            && candidate.input_command.is_none()
                        {
                            retry.source_http_reconnect_delay()
                        } else {
                            inner.config.retry_delay
                        };
                        tokio::select! {
                            _ = stop.changed() => break 'session,
                            _ = lost.changed() => break 'session,
                            _ = time::sleep(delay) => {},
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
                    requested_switch = drive_candidate(
                        &inner,
                        &session,
                        &worker,
                        lease.as_ref(),
                        candidate,
                        current,
                        discontinuity,
                        &mut input,
                        &mut retry,
                        &mut stop,
                        &mut lost,
                        &mut pending_switch,
                    )
                    .await;
                    session.connecting.store(true, Ordering::Relaxed);
                    if *stop.borrow() || *lost.borrow() {
                        break 'session;
                    }
                    if requested_switch.is_some() {
                        break;
                    }
                    if let (Some(control), Some(request)) =
                        (&session.source_switch, pending_switch.take())
                    {
                        control.fail(
                            request,
                            PlaybackError::new(502, "Requested source did not publish media"),
                        );
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
                retained_source_slot = requested_switch.is_none()
                    && inner.config.source_initialization
                    && !source_borrowed
                    && preview_assignment.is_none()
                    && lease.is_some()
                    && session.source_slot_published.load(Ordering::Acquire)
                    && matches!(*session.startup.borrow(), Startup::Ready)
                    && session.received_bytes.load(Ordering::Relaxed) > bytes_at_admission
                    && retry
                        .next_candidate(&candidates, Some(&candidate.url))
                        .is_some_and(|next| same_live_reservation(candidate, &candidates[next]));
                if let Some(lease) = &lease {
                    if !final_initializing_source && !retained_source_slot {
                        if let Err(error) = lease.release_provider().await {
                            last_error = coordination_error(error);
                            break 'session;
                        }
                    }
                }
            }
        }
        if let (Some(control), Some(request)) = (&session.source_switch, requested_switch.take()) {
            let target = candidates.iter().position(|candidate| {
                source_switch::source(candidate) == request.target
                    && source_switch::supported(candidate)
            });
            if let Some(target) = target {
                current = target;
                retry.tried.clear();
                retry.rotations = 0;
                pending_switch = Some(request);
                continue;
            }
            control.fail(
                request,
                PlaybackError::new(502, "Requested source is unavailable"),
            );
        }
        if let (Some(control), Some(request)) = (&session.source_switch, pending_switch.take()) {
            control.fail(
                request,
                PlaybackError::new(502, "Requested source could not start"),
            );
        }
        // After actual input attempts, Original compares the next source URL
        // with the outgoing one. An unavailable slot alone supplies no URL to
        // skip, and same-input retries above retain their existing budget.
        let opened_url = if inner.config.source_initialization {
            last_candidate.map(|index| candidates[index].url.as_str())
        } else {
            None
        };
        if let Some(next) = retry.next_candidate(&candidates, opened_url) {
            current = next;
            continue;
        }
        if let Some(wait) = &mut capacity_wait {
            let now = Instant::now();
            if let Some(delay) = wait.next_delay(now) {
                tokio::select! {
                    _ = stop.changed() => break,
                    _ = lost.changed() => break,
                    _ = time::sleep(delay) => {},
                }
                retry.tried.clear();
                current = 0;
                continue;
            }
            last_error.body["waited"] = format!("{}s", wait.elapsed(now).as_secs()).into();
        }
        // Like Python cold startup, never cycle the exhausted candidate list
        // before media succeeds. Healthy playback permits bounded cooldown wraps.
        if !retry.had_media
            || candidates.len() < 2
            || retry.rotations >= inner.config.max_stream_switches
            || opened_url.is_some_and(|url| candidates.iter().all(|candidate| candidate.url == url))
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
        let Some(next) = retry.next_candidate(&candidates, opened_url) else {
            break;
        };
        current = next;
    }
    if let (Some(control), Some(request)) = (&session.source_switch, pending_switch.take()) {
        control.fail(
            request,
            PlaybackError::new(503, "Source switch owner ended"),
        );
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
    if inner.config.source_initialization {
        input.finish_position();
    }
    let publisher = session.publisher.lock().unwrap().clone();
    if let Some(writer) = publisher {
        let _ = writer.finish().await;
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
    if let Some(profiles) = &inner.profiles {
        // Drain actual profile children and fenced profile Redis cleanup before
        // releasing the raw TS/provider owner they consume. Unknown cleanup
        // stays visibly retained instead of advertising successful teardown.
        profiles.stop_channel(&key).await;
    }
    if let Some(lease) = lease {
        if let Some(inputs) = &inner.inputs {
            if inner.config.source_initialization {
                // Actual process/body and InputWriter finish are already drained.
                // Clear raw health while current owner still matches, before the
                // coordinator release can admit a successor.
                let _ = inputs
                    .clear_source_last_data_retained(
                        &session.source.channel_id,
                        lease.token(),
                        (session.clone(), lease.clone()),
                    )
                    .await;
            }
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

fn input_process_diagnostics_enabled() -> bool {
    std::env::var_os("RUST_INPUT_PROCESS_DIAGNOSTICS").is_some_and(|value| value == "1")
}
fn safe_input_spawn_error(error: &io::Error) -> serde_json::Value {
    // Never format the error itself: its payload may contain command/URL data.
    serde_json::json!({"stage":"spawn","error_kind":format!("{:?}",error.kind()),"errno":error.raw_os_error()})
}
fn safe_input_process_receipt(receipt: &profile_process::Receipt) -> serde_json::Value {
    // This is one bounded summary per PHYSICAL child completion, never per
    // packet/viewer. Counts are explicitly of the existing <=64KiB tail, not
    // claims about dropped earlier stderr. No raw argv, URL or text escapes.
    let lower = String::from_utf8_lossy(&receipt.stderr_tail).to_ascii_lowercase();
    let signatures = [
        "permission denied",
        "unable to open the mrl",
        "no access modules matched",
        "no suitable access module",
        "name or service not known",
        "cannot resolve",
        "connection refused",
        "proxy",
        "no plugins found",
        "cannot open",
    ];
    let counts = signatures
        .into_iter()
        .map(|signature| (signature, lower.matches(signature).count()))
        .filter(|(_, count)| *count > 0)
        .map(|(signature, count)| (signature.to_owned(), serde_json::json!(count)))
        .collect::<serde_json::Map<String, serde_json::Value>>();
    serde_json::json!({"stage":"settled-child","pid":receipt.pid,"actual_exit":receipt.code,"signal":receipt.signal,
        "stdout_bytes":receipt.stdout_bytes,"stderr_bytes":receipt.stderr_bytes,"retained_stderr_tail_bytes":receipt.stderr_tail.len(),
        "stderr_tail_signature_counts":counts,"stdout_EOF":receipt.stdout_eof,"stderr_EOF":receipt.stderr_eof,
        "read_failed":receipt.read_failed,"input_failed":receipt.input_failed,"forced_kill":receipt.forced_kill,"group_settled":receipt.group_settled})
}
fn epoch_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

impl Session {
    fn has_local_demand(&self) -> bool {
        self.channel.viewer_count() > 0 || self.profile_demands.load(Ordering::Acquire) > 0
    }
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
        let index = if let Some(writer) = publisher {
            match writer.publish(data, discontinuity, epoch_seconds()).await {
                Ok(sequence) => sequence,
                Err(error) => {
                    self.stop.send_replace(true);
                    return Err(error);
                }
            }
        } else {
            self.channel.publish_marked(data, discontinuity)?
        };
        self.buffer_index.store(index, Ordering::Relaxed);
        Ok(index)
    }
}

async fn channel_metadata_worker(
    clients: ClientStore,
    session: Arc<Session>,
    owner: String,
    shared: bool,
    mut health_timing: health::Timing,
    health_settings: Option<Arc<dyn HealthSettingsSource>>,
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
    let mut published_provider_epoch = 0;
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
                health_timing = tokio::select! {
                    _=stop.changed()=>break,
                    value=effective_health_timing(health_timing, health_settings.as_ref())=>match value {
                        Ok(timing)=>timing,
                        Err(_)=>{session.stop.send_replace(true);session.channel.stop();break;},
                    },
                };
                session.health.lock().unwrap().tick_if_due(epoch_seconds(),session.buffer_index.load(Ordering::Relaxed),health_timing);
                continue;
            },
        }
        health_timing = tokio::select! {
            _=stop.changed()=>break,
            value=effective_health_timing(health_timing, health_settings.as_ref())=>match value {
                        Ok(timing)=>timing,
                        Err(_)=>{session.stop.send_replace(true);session.channel.stop();break;},
                    },
        };
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
        let provider_epoch = session.provider_epoch.load(Ordering::Acquire);
        {
            let current = session.current.lock().unwrap();
            for (name, value) in &session.source.metadata {
                if !matches!(
                    name.as_str(),
                    "source_assignment_required" | "source_stream_hash" | "source_preview"
                ) {
                    fields.insert(name.clone(), value.clone());
                }
            }
            for (name, value) in &current.metadata {
                if !matches!(
                    name.as_str(),
                    "source_assignment_required" | "source_stream_hash" | "source_preview"
                ) {
                    fields.insert(name.clone(), value.clone());
                }
            }
            fields.insert("url".into(), current.url.clone());
            fields.insert("stream_id".into(), current.stream_id.to_string());
            if !session.source_slot_published.load(Ordering::Acquire)
                && provider_epoch > published_provider_epoch
            {
                if let Some(profile) = current.provider_profile_id {
                    fields.insert("m3u_profile".into(), profile.to_string());
                }
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
        published_provider_epoch = provider_epoch;
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
    fn flush_for_switch(&mut self) -> Option<(Arc<[u8]>, bool)> {
        if self.chunk.is_empty() {
            return None;
        }
        Some((std::mem::take(&mut self.chunk).into(), self.mark_chunk))
    }
    fn reset_position(&mut self) {
        self.chunk.clear();
        self.assembler.reset();
    }
    fn finish_position(&mut self) {
        // Source.stop clears a partial tail only when complete local packets
        // were pending; an isolated partial packet remains until object drop.
        if !self.chunk.is_empty() {
            self.reset_position();
        }
    }
    fn arm_switch(&mut self) {
        self.reset_position();
        self.marker.arm();
        self.mark_chunk = true;
    }
}

#[allow(clippy::too_many_arguments)]
async fn drive_candidate(
    inner: &Inner,
    session: &Arc<Session>,
    worker: &Arc<WorkerCount>,
    provider: Option<&Arc<dispatcharr_coordination::Lease>>,
    candidate: &PlaybackCandidate,
    current: usize,
    discontinuity: bool,
    input: &mut InputAccumulator,
    retry: &mut crate::retry::RetryState,
    stop: &mut watch::Receiver<bool>,
    lost: &mut watch::Receiver<bool>,
    pending_switch: &mut Option<source_switch::Request>,
) -> Option<source_switch::Request> {
    if !inner.config.source_initialization {
        *input = InputAccumulator::new(inner.config.chunk_bytes);
    } else if discontinuity {
        if let Some((data, mark)) = input.flush_for_switch() {
            if session.publish_data(data, mark).await.is_err() {
                return None;
            }
        }
    }
    if discontinuity {
        let writer = session.publisher.lock().unwrap().clone();
        if let Some(writer) = writer {
            if writer.mark_discontinuity().await.is_err() {
                return None;
            }
        }
        input.arm_switch();
    }
    let _connection_clock = ConnectionClock(session);
    enum CandidateBytes {
        Http {
            bytes: dispatcharr_upstream_http::ByteStream,
            _attempt: crate::upstream::Attempt,
        },
        Process(profile_process::InputProcess),
    }
    impl CandidateBytes {
        async fn next(&mut self) -> Option<io::Result<Bytes>> {
            match self {
                Self::Http { bytes, .. } => bytes.next().await,
                Self::Process(process) => process.next().await,
            }
        }
        async fn finish(self) {
            if let Self::Process(process) = self {
                let receipt = process.finish().await;
                if input_process_diagnostics_enabled() {
                    match receipt {
                        Ok(receipt) => eprintln!(
                            "dispatcharr-input-process {}",
                            safe_input_process_receipt(&receipt)
                        ),
                        Err(_) => eprintln!(
                            "dispatcharr-input-process {}",
                            serde_json::json!({"stage":"supervisor","join_failed":true})
                        ),
                    }
                }
            }
        }
    }
    let mut bytes = if let Some(command) = &candidate.input_command {
        let scope: Arc<dyn Send + Sync> =
            Arc::new((session.clone(), worker.clone(), provider.cloned()));
        match profile_process::InputProcess::start(command, inner.config.read_timeout, scope) {
            Ok(process) => {
                {
                    let mut health = session.health.lock().unwrap();
                    health.arm(epoch_seconds());
                    health.connected = true;
                }
                CandidateBytes::Process(process)
            }
            Err(error) => {
                if input_process_diagnostics_enabled() {
                    eprintln!(
                        "dispatcharr-input-process {}",
                        safe_input_spawn_error(&error)
                    );
                }
                return None;
            }
        }
    } else {
        let Ok(attempt) = crate::upstream::Attempt::new_playback_requests(
            inner.config.connect_timeout,
            inner.config.read_timeout,
        ) else {
            return None;
        };
        let Ok(request) =
            attempt.request(&candidate.url, &candidate.user_agent, &candidate.headers)
        else {
            return None;
        };
        {
            let mut health = session.health.lock().unwrap();
            health.arm(epoch_seconds());
            health.connected = true;
        }
        let response = tokio::select! {
            _ = stop.changed() => return None, _ = lost.changed() => return None,
            response = request.send() => response,
        };
        let Ok(response) = response else {
            return None;
        };
        if response.status() != reqwest::StatusCode::OK || attempt.cookie_budget_exceeded() {
            return None;
        }
        let Ok(body) = crate::upstream::body(response) else {
            return None;
        };
        CandidateBytes::Http {
            bytes: body,
            _attempt: attempt,
        }
    };
    let InputAccumulator {
        assembler,
        marker,
        mark_chunk,
        chunk,
    } = input;
    let control = session
        .source_switch
        .as_ref()
        .filter(|_| source_switch::supported(candidate));
    let mut requested_switch = None;
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
            request = async { control.unwrap().receive().await }, if control.is_some() => {
                requested_switch = Some(request);
                break;
            }
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
                if inner.config.source_initialization {
                    if let Some(inputs)=&inner.inputs {
                        let owner=session.input_owner.lock().unwrap().clone();
                        if let Some(owner)=owner {
                            // Source add_chunk accepts partial/subtarget ingress;
                            // this is deliberately independent of `published`.
                            let written=inputs.put_source_last_data_retained(
                                &session.source.channel_id,&owner,epoch_seconds(),
                                (session.clone(),worker.clone(),provider.cloned()),
                            ).await;
                            if matches!(written,Err(dispatcharr_input_store::Error::LostOwnership)) {break;}
                            // Redis/type/timeout failure cannot manufacture fresh
                            // remote health. Source fetch_chunk catches such an
                            // error and continues its ordinary ingress attempts.
                        }
                    }
                }
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
            if let Some(control) =
                control.filter(|_| first_media.is_none() && !*stop.borrow() && !*lost.borrow())
            {
                let current_source = source_switch::source(candidate);
                if let Some(request) = pending_switch.take() {
                    control.confirm(request, current_source);
                } else {
                    control.arm(current_source);
                }
            }
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
    if let Some(control) = control {
        control.disarm();
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
    bytes.finish().await;
    requested_switch
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

#[cfg(test)]
#[path = "core_trace_peer.rs"]
mod core_trace_peer;

#[cfg(test)]
mod dynamic_health_settings_tests {
    use super::*;
    struct Source(AtomicU64);
    impl HealthSettingsSource for Source {
        fn snapshot(
            &self,
        ) -> Pin<
            Box<
                dyn std::future::Future<Output = Result<HealthTimeouts, PlaybackError>> + Send + '_,
            >,
        > {
            Box::pin(async {
                Ok(HealthTimeouts {
                    startup: Duration::from_secs(self.0.load(Ordering::Acquire)),
                    ..HealthTimeouts::default()
                })
            })
        }
    }
    fn base() -> health::Timing {
        health::Timing {
            startup: Duration::from_secs(8),
            connection: Duration::from_secs(10),
            initialization: Duration::from_secs(60),
            interval: Duration::from_secs(5),
        }
    }
    #[tokio::test]
    async fn next_real_health_decision_uses_new_copied_timeout() {
        let concrete = Arc::new(Source(AtomicU64::new(8)));
        let source: Arc<dyn HealthSettingsSource> = concrete.clone();
        let mut state = health::Health::new(0.0);
        state.connected = true;
        state.tick(
            4.0,
            0,
            effective_health_timing(base(), Some(&source))
                .await
                .unwrap(),
        );
        assert!(state.healthy);
        assert!(!state.needs_switch);
        concrete.0.store(3, Ordering::Release);
        state.tick(
            4.0,
            0,
            effective_health_timing(base(), Some(&source))
                .await
                .unwrap(),
        );
        assert!(!state.healthy);
        assert!(state.needs_switch);
    }
    #[tokio::test]
    async fn settings_error_is_not_silently_replaced_with_default_health() {
        struct Refused;
        impl HealthSettingsSource for Refused {
            fn snapshot(
                &self,
            ) -> Pin<
                Box<
                    dyn std::future::Future<Output = Result<HealthTimeouts, PlaybackError>>
                        + Send
                        + '_,
                >,
            > {
                Box::pin(async { Err(PlaybackError::new(503, "Unsupported health settings")) })
            }
        }
        let source: Arc<dyn HealthSettingsSource> = Arc::new(Refused);
        assert!(effective_health_timing(base(), Some(&source))
            .await
            .is_err());
    }
    #[test]
    fn existing_constructors_keep_explicit_fixed_retry_delays() {
        for milliseconds in [0, 10, 500, 501] {
            let delay = Duration::from_millis(milliseconds);
            let engine = StreamEngine::new(EngineConfig {
                source_initialization: true,
                retry_delay: delay,
                ..EngineConfig::default()
            })
            .unwrap();
            assert!(!engine.inner.source_http_retry_backoff);
            assert_eq!(engine.inner.config.retry_delay, delay);
        }
    }

    #[test]
    fn source_http_backoff_is_bound_only_before_engine_sharing() {
        let engine = StreamEngine::new(EngineConfig::default()).unwrap();
        let shared = engine.clone();
        assert!(engine.with_source_http_retry_backoff().is_err());
        assert!(!shared.inner.source_http_retry_backoff);
        let default = StreamEngine::new(EngineConfig::default()).unwrap();
        let bound = default.with_source_http_retry_backoff().unwrap();
        assert!(bound.inner.source_http_retry_backoff);
        assert_eq!(bound.inner.config.retry_delay, Duration::from_millis(500));
    }

    #[tokio::test]
    async fn no_hook_retains_static_config_and_late_binding_is_refused() {
        let timing = effective_health_timing(base(), None).await.unwrap();
        assert_eq!(timing.startup, Duration::from_secs(8));
        let engine = StreamEngine::new(EngineConfig::default()).unwrap();
        let _clone = engine.clone();
        assert!(engine
            .with_health_settings(Arc::new(Source(AtomicU64::new(3))))
            .is_err());
    }
}

#[cfg(test)]
mod input_diagnostic_privacy {
    use super::*;
    #[test]
    fn bounded_child_receipt_and_spawn_errors_never_export_payload_text() {
        let receipt=profile_process::Receipt {
            pid:123,code:Some(1),signal:None,stdout_bytes:0,stderr_bytes:99999,
            stderr_tail:b"PRIVATE_FIXTURE_TOKEN http://fixture.invalid/private?key=SECRET Permission denied PERMISSION DENIED proxy".to_vec(),
            stdout_eof:true,stderr_eof:true,read_failed:false,input_failed:true,
            stdin_bytes:0,stdin_eof:false,stdin_failed:false,forced_kill:false,group_settled:true,
        };
        let value = safe_input_process_receipt(&receipt);
        let encoded = value.to_string();
        assert_eq!(
            value["stderr_tail_signature_counts"]["permission denied"],
            2
        );
        assert_eq!(value["stderr_tail_signature_counts"]["proxy"], 1);
        assert_eq!(value["stderr_bytes"], 99999);
        assert!(
            value["stdout_EOF"] == true
                && value["stderr_EOF"] == true
                && value["group_settled"] == true
        );
        for sentinel in [
            "PRIVATE_FIXTURE_TOKEN",
            "fixture.invalid",
            "SECRET",
            "http://",
        ] {
            assert!(!encoded.contains(sentinel));
        }
        let error = io::Error::new(
            io::ErrorKind::PermissionDenied,
            "PRIVATE_FIXTURE_TOKEN command /secret?key=SECRET",
        );
        let spawn = safe_input_spawn_error(&error).to_string();
        assert!(spawn.contains("PermissionDenied"));
        assert!(!spawn.contains("PRIVATE_FIXTURE_TOKEN"));
        assert!(!spawn.contains("SECRET"));
    }
}
