use axum::{body::Body, http::Response};
use bytes::Bytes;
use dispatcharr_clients::{ClientLease, ClientStore};
use dispatcharr_contracts::{
    PlaybackCandidate, PlaybackError, PlaybackSource, PlaybackViewer, UserLimitPolicy,
};
use dispatcharr_coordination::{AcquireRequest, Coordinator, ProviderReservation};
use dispatcharr_stream_core::ts::{DiscontinuityMarker, PacketAssembler, PACKET_SIZE};
use dispatcharr_stream_runtime::{Channel, OwnedChunk, OwnedDelivery, Viewer};
use futures_util::StreamExt;
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashMap},
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{watch, OwnedSemaphorePermit, Semaphore},
    time::{self, Instant},
};

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub max_channels: usize,
    pub max_viewers_per_channel: usize,
    pub retained_bytes_per_channel: usize,
    pub retention: Duration,
    pub chunk_bytes: usize,
    pub flush_interval: Duration,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub startup_timeout: Duration,
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
            retention: Duration::from_secs(30),
            chunk_bytes: PACKET_SIZE * 1361,
            flush_interval: Duration::from_millis(50),
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(10),
            startup_timeout: Duration::from_secs(30),
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
    client: reqwest::Client,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    slots: Arc<Semaphore>,
    coordinator: Option<Coordinator>,
    pub(crate) clients: Option<ClientStore>,
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
    reconnects: AtomicU64,
    buffer_index: AtomicU64,
    last_data_ms: AtomicU64,
    started_at: f64,
    current: Mutex<PlaybackCandidate>,
    connecting: AtomicBool,
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

impl StreamEngine {
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
        if config.max_channels == 0
            || config.max_channels > Semaphore::MAX_PERMITS
            || config.max_viewers_per_channel == 0
            || config.max_viewers_per_channel > Semaphore::MAX_PERMITS
            || config.chunk_bytes < PACKET_SIZE
            || !config.chunk_bytes.is_multiple_of(PACKET_SIZE)
            || config.chunk_bytes > config.retained_bytes_per_channel
            || config.flush_interval.is_zero()
            || config.retention.is_zero()
            || config.connect_timeout.is_zero()
            || config.read_timeout.is_zero()
            || config.startup_timeout.is_zero()
            || config.retry_window.is_zero()
            || config.stable_connection_threshold.is_zero()
            || config.rotation_cooldown.is_zero()
            || config.max_candidates == 0
            || config.keepalive_interval.is_zero()
            || config.max_keepalive_duration.is_zero()
        {
            return Err(PlaybackError::new(500, "Invalid streaming resource limits"));
        }
        let client = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .read_timeout(config.read_timeout)
            .redirect(reqwest::redirect::Policy::limited(10))
            .build()
            .map_err(|_| PlaybackError::new(500, "HTTP client initialization failed"))?;
        let slots = Arc::new(Semaphore::new(config.max_channels));
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                client,
                sessions: Mutex::new(HashMap::new()),
                slots,
                coordinator,
                clients,
            }),
        })
    }

    /// Authorization/resolution must run before calling this method for each viewer.
    pub async fn play(&self, source: PlaybackSource) -> Result<Response<Body>, PlaybackError> {
        let stream = self.open_stream(source, true).await?;
        Response::builder()
            .header("Content-Type", "video/mp2t")
            .header("Cache-Control", "no-cache")
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
                let channel = Channel::new(
                    self.inner.config.retained_bytes_per_channel,
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
                    reconnects: AtomicU64::new(0),
                    buffer_index: AtomicU64::new(0),
                    last_data_ms: AtomicU64::new(0),
                    started_at: epoch_seconds(),
                    current,
                    connecting: AtomicBool::new(true),
                    _permit: permit,
                });
                sessions.insert(key.clone(), session.clone());
                (session, viewer, true)
            }
        };
        let state = TransportState {
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
            tokio::spawn(async move { run_upstream(weak, key, running).await });
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
        time::timeout(self.inner.config.startup_timeout, ready)
            .await
            .map_err(|_| PlaybackError::new(504, "Stream initialization timed out"))??;
        let keepalive_interval = self.inner.config.keepalive_interval;
        let max_keepalive_duration = self.inner.config.max_keepalive_duration;
        let stream = async_stream::stream! {
            let mut state = state;
            let mut stopped=state.client.as_ref().map(ClientLease::stopped);
            let mut last_media=Instant::now();
            loop {
                if stopped.as_ref().is_some_and(|control|*control.borrow()) {break;}
                let next=match stopped.as_mut() {
                    Some(control)=>tokio::select! { _=control.changed()=>break, next=time::timeout(keepalive_interval,state.viewer.next_owned())=>next },
                    None=>time::timeout(keepalive_interval,state.viewer.next_owned()).await,
                };
                let next=match next {
                    Ok(next)=>next,
                    Err(_)=>{
                        if !register_client {continue;}
                        if last_media.elapsed()>=max_keepalive_duration {break;}
                        if let Some(client)=&state.client {client.record_keepalive(188);}
                        let frame=KeepaliveFrame {_client:state.client.clone(),_guard:state._guard.clone()};
                        yield Ok::<MediaFrame,io::Error>(MediaFrame {data:Bytes::from_owner(frame),discontinuity:false});
                        continue;
                    }
                };
                match next {
                    OwnedDelivery::Chunk(chunk) => {
                        last_media=Instant::now();
                        if let Some(client)=&state.client {client.record_bytes(chunk.as_ref().len());}
                        // The frame retains both viewer and channel admission
                        // until Hyper releases its final Bytes clone.
                        let discontinuity=chunk.discontinuity;
                        let frame = FrameOwner { chunk, _client:state.client.clone(), _guard: state._guard.clone() };
                        yield Ok::<MediaFrame, io::Error>(MediaFrame {data:Bytes::from_owner(frame),discontinuity});
                    }
                    OwnedDelivery::Lagged { .. } => continue,
                    OwnedDelivery::Stopped | OwnedDelivery::Cancelled => break,
                }
            }
        };
        Ok(Box::pin(stream))
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
    chunk: OwnedChunk,
    _client: Option<ClientLease>,
    _guard: ViewerGuard,
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
        if self.session.channel.viewer_count() != 0 {
            return;
        }
        let weak = self.inner.clone();
        let session = self.session.clone();
        let key = self.key.clone();
        // Runtime shutdown can drop response bodies without an executor.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                time::sleep(session.source.shutdown_delay).await;
                let Some(inner) = weak.upgrade() else {
                    session.channel.stop();
                    return;
                };
                let mut sessions = inner.sessions.lock().unwrap();
                if session.channel.viewer_count() == 0
                    && sessions
                        .get(&key)
                        .is_some_and(|current| Arc::ptr_eq(current, &session))
                {
                    sessions.remove(&key);
                    session.stop.send_replace(true);
                    session.channel.stop();
                }
            });
        }
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
            Err(error) => {
                finish_session(&inner, &key, &session, coordination_error(error));
                return;
            }
        }
    } else {
        None
    };
    let metadata_task = inner.clients.as_ref().map(|clients| {
        let clients = clients.clone();
        let session = session.clone();
        let owner = lease
            .as_ref()
            .map_or(String::new(), |lease| lease.token().to_owned());
        tokio::spawn(async move { channel_metadata_worker(clients, session, owner).await })
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
                    let discontinuity = attempts > 0;
                    attempts = attempts.saturating_add(1);
                    drive_candidate(
                        &inner,
                        &session,
                        candidate,
                        current,
                        discontinuity,
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
                if let Some(lease) = &lease {
                    if let Err(error) = lease.release_provider().await {
                        last_error = coordination_error(error);
                        break 'session;
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
    session.channel.stop();
    session.stop.send_replace(true);
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
        let _ = lease.release().await;
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
    fn publish_data(
        &self,
        data: Arc<[u8]>,
        discontinuity: bool,
    ) -> Result<u64, dispatcharr_stream_runtime::Error> {
        let index = self.channel.publish_marked(data, discontinuity)?;
        self.buffer_index.store(index, Ordering::Relaxed);
        Ok(index)
    }
}

async fn channel_metadata_worker(clients: ClientStore, session: Arc<Session>, owner: String) {
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
        tokio::select! {_=stop.changed()=>break,_=timer.tick()=>{}}
        let state = match &*session.startup.borrow() {
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
            ("stream_profile", "Proxy".to_string()),
            ("channel_name", session.source.channel_name.clone()),
            ("init_time", session.started_at.to_string()),
            (
                "total_bytes",
                session.received_bytes.load(Ordering::Relaxed).to_string(),
            ),
            (
                "last_data",
                (session.last_data_ms.load(Ordering::Relaxed) as f64 / 1000.0).to_string(),
            ),
            ("worker_id", clients.worker_id().to_owned()),
        ] {
            fields.insert(name.into(), value);
        }
        if clients
            .publish_channel(
                &session.source.channel_id,
                &owner,
                &fence,
                &fields,
                session.buffer_index.load(Ordering::Relaxed),
            )
            .await
            .is_err()
        {
            session.stop.send_replace(true);
            session.channel.stop();
            break;
        }
    }
    let _ = clients
        .remove_channel(&session.source.channel_id, &fence)
        .await;
}

fn uuid_fence() -> String {
    format!("local-{}", uuid::Uuid::new_v4())
}

#[allow(clippy::too_many_arguments)]
async fn drive_candidate(
    inner: &Inner,
    session: &Session,
    candidate: &PlaybackCandidate,
    current: usize,
    discontinuity: bool,
    retry: &mut crate::retry::RetryState,
    stop: &mut watch::Receiver<bool>,
    lost: &mut watch::Receiver<bool>,
) {
    let mut request = inner
        .client
        .get(&candidate.url)
        .header("User-Agent", &candidate.user_agent);
    for (name, value) in &candidate.headers {
        request = request.header(name, value);
    }
    let response = tokio::select! {
        _ = stop.changed() => return, _ = lost.changed() => return,
        response = request.send() => response,
    };
    let Ok(response) = response else { return };
    if !response.status().is_success() {
        return;
    }
    let mut bytes = response.bytes_stream();
    let mut assembler = PacketAssembler::default();
    let mut marker = DiscontinuityMarker::default();
    let mut mark_chunk = discontinuity;
    if discontinuity {
        marker.arm();
    }
    let mut chunk = Vec::with_capacity(inner.config.chunk_bytes);
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
                session.received_bytes.fetch_add(frame.len() as u64, Ordering::Relaxed);
                session.last_data_ms.store((epoch_seconds()*1000.0) as u64,Ordering::Relaxed);
                let mut failed = false;
                assembler.feed(&frame, |packet| {
                    let mut packet = *packet;
                    marker.stamp(&mut packet);
                    chunk.extend_from_slice(&packet);
                    if chunk.len() == inner.config.chunk_bytes {
                        let data: Arc<[u8]> = std::mem::replace(&mut chunk,
                            Vec::with_capacity(inner.config.chunk_bytes)).into();
                        if session.publish_data(data,mark_chunk).is_err() { failed = true; }
                        mark_chunk=false;
                        marker.disarm();
                        published = true;
                    }
                });
                if failed { break; }
            }
            _ = flush.tick() => {
                if !chunk.is_empty() {
                    let data: Arc<[u8]> = std::mem::replace(&mut chunk,
                        Vec::with_capacity(inner.config.chunk_bytes)).into();
                    if session.publish_data(data,mark_chunk).is_err() { break; }
                    mark_chunk=false;
                    marker.disarm();
                    published = true;
                }
            }
        }
        if published {
            retry.had_media = true;
            let began = *first_media.get_or_insert_with(Instant::now);
            session.connecting.store(false, Ordering::Relaxed);
            session.startup.send_replace(Startup::Ready);
            if !stable_noted && began.elapsed() >= inner.config.stable_connection_threshold {
                retry.stable(current);
                stable_noted = true;
            }
        }
    }
    if !chunk.is_empty()
        && !*stop.borrow()
        && !*lost.borrow()
        && session.publish_data(chunk.into(), mark_chunk).is_ok()
    {
        retry.had_media = true;
        session.connecting.store(false, Ordering::Relaxed);
        session.startup.send_replace(Startup::Ready);
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
