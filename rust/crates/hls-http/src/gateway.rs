//! Capability session transport derived from Dispatcharr's HLS manager/views.
//! Original authors retain full credit. See ../../../ATTRIBUTION.md.
use axum::{
    body::Body,
    http::{header, Response, StatusCode},
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use bytes::Bytes;
use dispatcharr_clients::{ClientLease, ClientStore};
use dispatcharr_contracts::{PlaybackError, PlaybackResolver, PlaybackSource};
use dispatcharr_hls_core::{
    render_media_playlist, Segment, SegmentDescriptor, SegmenterConfig, TSSegmenter,
};
use dispatcharr_output_store::{OutputLease, OutputStore};
use dispatcharr_stream_http::StreamEngine;
use futures_util::StreamExt;
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{watch, Notify, OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Debug)]
pub struct GatewayConfig {
    pub max_managers: usize,
    pub max_sessions: usize,
    pub max_retained_bytes: usize,
    pub max_segment_bytes: usize,
    pub max_stored_segments: usize,
    pub max_requests_per_session: usize,
    pub segment_ttl: Duration,
    pub session_idle_timeout: Duration,
    pub initialization_timeout: Duration,
    pub ready_timeout: Duration,
    pub reconnect_grace: Duration,
}
impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            max_managers: 32,
            max_sessions: 12_800,
            max_retained_bytes: 512 * 1024 * 1024,
            max_segment_bytes: 32 * 1024 * 1024,
            max_stored_segments: 256,
            max_requests_per_session: 8,
            segment_ttl: Duration::from_secs(100),
            session_idle_timeout: Duration::from_secs(12),
            initialization_timeout: Duration::from_secs(60),
            ready_timeout: Duration::from_secs(10),
            reconnect_grace: Duration::from_millis(500),
        }
    }
}
#[derive(Clone)]
pub struct HlsGateway {
    pub(crate) inner: Arc<Inner>,
}
pub(crate) struct Inner {
    pub engine: StreamEngine,
    pub clients: ClientStore,
    pub resolver: Arc<dyn PlaybackResolver>,
    pub config: GatewayConfig,
    pub outputs: Option<OutputStore>,
    pub remote: Mutex<HashMap<String, std::sync::Weak<crate::shared::RemoteSession>>>,
    managers: Mutex<HashMap<String, Arc<Manager>>>,
    pub sessions: Mutex<HashMap<String, Arc<Session>>>,
    manager_slots: Arc<Semaphore>,
    pub session_slots: Arc<Semaphore>,
    pub bytes: Arc<Semaphore>,
    pub stopped: AtomicBool,
}
pub(crate) struct Session {
    pub token: String,
    pub client: ClientLease,
    pub manager: Arc<Manager>,
    pub requests: Arc<Semaphore>,
    pub expired: AtomicBool,
    pub touched: Mutex<Instant>,
    _admission: OwnedSemaphorePermit,
}
/// Own the pre-registered client until a profiled mint sends its capability.
struct PendingProfileMint {
    inner: Arc<Inner>,
    session: Option<Arc<Session>>,
}
impl Drop for PendingProfileMint {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            let inner = self.inner.clone();
            tokio::spawn(async move { inner.expire(&session).await });
        }
    }
}
pub(crate) struct Manager {
    pub channel: String,
    key: String,
    format: String,
    profile: Option<i64>,
    fence: String,
    pub state: Mutex<ManagerState>,
    pub notify: Notify,
    pub stop: watch::Sender<bool>,
    done: watch::Sender<bool>,
    profile_start: watch::Sender<Option<bool>>,
    demand: AtomicUsize,
    admission: Mutex<Option<Arc<OwnedSemaphorePermit>>>,
    startup_recovery: Mutex<Option<(Instant, u64)>>,
    behind: f64,
    output_lease: Mutex<Option<OutputLease>>,
}
pub(crate) struct ManagerState {
    pub playlist: Option<Arc<Payload>>,
    pub dead: bool,
    pub ready: Option<Instant>,
    pub last_produced: Option<Instant>,
    stored: BTreeMap<u64, Stored>,
    window: VecDeque<SegmentDescriptor>,
    disc_seq: u64,
    seq: u64,
    ts: f64,
}
struct Stored {
    payload: Arc<Payload>,
    created: Instant,
}
pub(crate) struct Payload {
    data: Vec<u8>,
    _budget: OwnedSemaphorePermit,
    _manager: Arc<OwnedSemaphorePermit>,
}
impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
struct FrameOwner {
    payload: Arc<Payload>,
    _session: Arc<Session>,
    _request: Arc<OwnedSemaphorePermit>,
}
impl AsRef<[u8]> for FrameOwner {
    fn as_ref(&self) -> &[u8] {
        self.payload.as_ref().as_ref()
    }
}
pub(crate) fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value.lock().unwrap_or_else(|error| error.into_inner())
}
pub(crate) fn wall_time() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
fn token() -> Result<String, PlaybackError> {
    let mut raw = [0; 24];
    getrandom::getrandom(&mut raw).map_err(|_| PlaybackError::new(503, "Session unavailable"))?;
    Ok(URL_SAFE_NO_PAD.encode(raw))
}
pub(crate) fn failure(status: u16, message: &str) -> PlaybackError {
    PlaybackError::new(status, message)
}
impl HlsGateway {
    pub fn new(
        engine: StreamEngine,
        clients: ClientStore,
        resolver: Arc<dyn PlaybackResolver>,
        config: GatewayConfig,
    ) -> Result<Self, PlaybackError> {
        if [
            config.max_managers,
            config.max_sessions,
            config.max_requests_per_session,
            config.max_retained_bytes,
        ]
        .iter()
        .any(|value| *value > Semaphore::MAX_PERMITS)
            || config.max_managers == 0
            || config.max_sessions == 0
            || config.max_retained_bytes == 0
            || config.max_retained_bytes > u32::MAX as usize
            || config.max_segment_bytes < 188
            || config.max_segment_bytes > config.max_retained_bytes
            || config.max_stored_segments == 0
            || config.max_requests_per_session == 0
            || config.segment_ttl <= Duration::from_secs(4)
            || config.session_idle_timeout.is_zero()
            || config.initialization_timeout.is_zero()
            || config.ready_timeout.is_zero()
        {
            return Err(failure(500, "Invalid HLS configuration"));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                engine,
                clients,
                resolver,
                manager_slots: Arc::new(Semaphore::new(config.max_managers)),
                session_slots: Arc::new(Semaphore::new(config.max_sessions)),
                bytes: Arc::new(Semaphore::new(config.max_retained_bytes)),
                config,
                outputs: None,
                remote: Mutex::new(HashMap::new()),
                managers: Mutex::new(HashMap::new()),
                sessions: Mutex::new(HashMap::new()),
                stopped: AtomicBool::new(false),
            }),
        })
    }
    /// Shared legacy Redis media storage enables cross-replica capability reads.
    /// Configure before cloning or starting the gateway.
    pub fn with_output_store(mut self, outputs: OutputStore) -> Result<Self, PlaybackError> {
        let inner =
            Arc::get_mut(&mut self.inner).ok_or_else(|| failure(500, "Gateway already shared"))?;
        inner.outputs = Some(outputs);
        Ok(self)
    }
    /// The caller must resolve and authorize the source before minting a token.
    pub async fn mint(&self, source: PlaybackSource) -> Result<Response<Body>, PlaybackError> {
        if self.inner.stopped.load(Ordering::Acquire) {
            return Err(failure(503, "HLS is shutting down"));
        }
        if source
            .metadata
            .iter()
            .any(|(key, value)| key == "output_profile_id" && !value.is_empty())
        {
            return Err(failure(501, "HLS output profiles are not migrated"));
        }
        let profiled = source.output_profile.is_some();
        let viewer = source
            .viewer
            .as_ref()
            .ok_or_else(|| failure(500, "Missing viewer context"))?;
        let admission = self
            .inner
            .session_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| failure(503, "HLS session capacity exhausted"))?;
        let token = token()?;
        let client = self
            .inner
            .clients
            .register_profile(
                &source.channel_id,
                viewer,
                "hls",
                source.output_profile.as_ref().map(|profile| profile.id),
            )
            .await
            .map_err(|error| match error {
                dispatcharr_clients::Error::UserLimit => failure(429, "User stream limit reached"),
                dispatcharr_clients::Error::Stopped => failure(410, "Stream stopped"),
                _ => failure(503, "Client registration failed"),
            })?;
        let user_id = viewer.user_id;
        if !profiled && client.bind_hls_token(&token, user_id).await.is_err() {
            let _ = client.release().await;
            return Err(failure(503, "Session unavailable"));
        }
        let manager_result = self.inner.manager(&source);
        let (manager, created) = match manager_result {
            Ok(result) => result,
            Err(error) => {
                let _ = client.release().await;
                return Err(error);
            }
        };
        manager.demand.fetch_add(1, Ordering::AcqRel);
        let session = Arc::new(Session {
            token: token.clone(),
            client,
            manager: manager.clone(),
            requests: Arc::new(Semaphore::new(self.inner.config.max_requests_per_session)),
            expired: AtomicBool::new(false),
            touched: Mutex::new(Instant::now()),
            _admission: admission,
        });
        lock(&self.inner.sessions).insert(token.clone(), session.clone());
        if created {
            let inner = self.inner.clone();
            tokio::spawn(async move {
                inner.run_manager(manager, source).await;
            });
        }
        if profiled {
            let mut pending = PendingProfileMint {
                inner: self.inner.clone(),
                session: Some(session.clone()),
            };
            let mut startup = session.manager.profile_start.subscribe();
            let started = tokio::time::timeout(
                self.inner.config.initialization_timeout,
                startup.wait_for(Option::is_some),
            )
            .await
            .is_ok_and(|state| state.is_ok_and(|state| *state == Some(true)));
            if !started {
                return Err(failure(500, "Failed to start output profile transcode"));
            }
            if session
                .client
                .bind_hls_token(&token, user_id)
                .await
                .is_err()
            {
                return Err(failure(500, "Failed to create HLS session"));
            }
            *lock(&session.touched) = Instant::now();
            pending.session = None;
        }
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let mut stopped = session.client.stopped();
            loop {
                tokio::select! { _=tokio::time::sleep(Duration::from_millis(250))=>{}, _=stopped.changed()=>{} }
                if *stopped.borrow()
                    || session.expired.load(Ordering::Acquire)
                    || lock(&session.manager.startup_recovery)
                        .is_some_and(|(deadline, _)| Instant::now() >= deadline)
                {
                    break;
                }
                if lock(&session.touched).elapsed() >= inner.config.session_idle_timeout {
                    if inner.outputs.is_some() {
                        match inner
                            .clients
                            .hls_session_within_idle_window(
                                &session.token,
                                inner.config.session_idle_timeout,
                            )
                            .await
                        {
                            Ok(true) => {
                                *lock(&session.touched) = Instant::now();
                                continue;
                            }
                            Err(_) => continue,
                            _ => {}
                        }
                    }
                    break;
                }
            }
            inner.expire(&session).await;
        });
        Response::builder()
            .status(StatusCode::FOUND)
            .header(header::LOCATION, format!("/proxy/hls/{token}/index.m3u8"))
            .header("X-Dispatcharr-Session-Token", token)
            .header(
                "Access-Control-Expose-Headers",
                "X-Dispatcharr-Session-Token",
            )
            .body(Body::empty())
            .map_err(|_| failure(500, "Session unavailable"))
    }
    pub async fn shutdown(&self) {
        self.inner.stopped.store(true, Ordering::Release);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let managers: Vec<_> = lock(&self.inner.managers).values().cloned().collect();
        for manager in &managers {
            manager.stop.send_replace(true);
            manager.notify.notify_waiters();
        }
        let sessions: Vec<_> = lock(&self.inner.sessions).values().cloned().collect();
        let _ = tokio::time::timeout_at(deadline, async {
            futures_util::stream::iter(sessions)
                .for_each_concurrent(32, |session| async move {
                    self.inner.expire(&session).await;
                })
                .await;
            futures_util::stream::iter(managers)
                .for_each_concurrent(32, |manager| async move {
                    let mut done = manager.done.subscribe();
                    if !*done.borrow() {
                        let _ = done.changed().await;
                    }
                })
                .await;
        })
        .await;
    }
}
impl Inner {
    fn manager(&self, source: &PlaybackSource) -> Result<(Arc<Manager>, bool), PlaybackError> {
        let profile = source.output_profile.as_ref().map(|profile| profile.id);
        if profile.is_some() && self.outputs.is_none() {
            return Err(failure(501, "Profile HLS requires shared output storage"));
        }
        let format = profile.map_or_else(|| "hls".to_owned(), |id| format!("hls:p{id}"));
        let key = format!("{}:{format}", source.channel_id);
        let mut managers = lock(&self.managers);
        if let Some(manager) = managers.get(&key) {
            if !lock(&manager.state).dead && !*manager.stop.borrow() {
                return Ok((manager.clone(), false));
            }
        }
        let admission = Arc::new(
            self.manager_slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| failure(503, "HLS channel capacity exhausted"))?,
        );
        let (stop, _) = watch::channel(false);
        let (done, _) = watch::channel(false);
        let (profile_start, _) = watch::channel(None);
        let manager = Arc::new(Manager {
            channel: source.channel_id.clone(),
            key: key.clone(),
            format,
            profile,
            fence: token()?,
            notify: Notify::new(),
            stop,
            done,
            profile_start,
            demand: AtomicUsize::new(0),
            admission: Mutex::new(Some(admission)),
            startup_recovery: Mutex::new(None),
            behind: source.startup_behind.as_secs_f64(),
            output_lease: Mutex::new(None),
            state: Mutex::new(ManagerState {
                playlist: None,
                dead: false,
                ready: None,
                last_produced: None,
                stored: BTreeMap::new(),
                window: VecDeque::new(),
                disc_seq: 0,
                seq: 1,
                ts: 0.0,
            }),
        });
        managers.insert(key, manager.clone());
        Ok((manager, true))
    }
    pub(crate) async fn expire(&self, session: &Arc<Session>) {
        if session.expired.swap(true, Ordering::AcqRel) {
            return;
        }
        {
            let mut sessions = lock(&self.sessions);
            if sessions
                .get(&session.token)
                .is_some_and(|current| Arc::ptr_eq(current, session))
            {
                sessions.remove(&session.token);
            }
        }
        let last = session.manager.demand.fetch_sub(1, Ordering::AcqRel) == 1;
        if last && self.outputs.is_none() {
            session.manager.stop.send_replace(true);
            session.manager.notify.notify_waiters();
        }
        let _ = session.client.release().await;
        if last
            && self.outputs.is_some()
            && matches!(
                self.has_demand(&session.manager.channel, session.manager.profile)
                    .await,
                Ok(false)
            )
        {
            session.manager.stop.send_replace(true);
            session.manager.notify.notify_waiters();
        }
    }
    pub(crate) async fn lookup(&self, token: &str) -> Result<Arc<Session>, PlaybackError> {
        let session = lock(&self.sessions)
            .get(token)
            .cloned()
            .ok_or_else(|| failure(410, "Session expired"))?;
        if session.expired.load(Ordering::Acquire) {
            return Err(failure(410, "Session expired"));
        }
        match self.clients.touch_hls_session(token).await {
            Ok(Some(info))
                if info.channel_id == session.manager.channel
                    && info.client_id == session.client.id() => {}
            Err(dispatcharr_clients::Error::Stopped) => {
                self.expire(&session).await;
                return Err(failure(410, "Stream stopped"));
            }
            Ok(_) => {
                self.expire(&session).await;
                return Err(failure(410, "Session expired"));
            }
            Err(_) => return Err(failure(503, "Session unavailable")),
        }
        *lock(&session.touched) = Instant::now();
        session.client.touch();
        Ok(session)
    }
    fn payload(&self, manager: &Manager, data: Vec<u8>) -> Result<Arc<Payload>, PlaybackError> {
        let size = u32::try_from(data.len().max(1))
            .map_err(|_| failure(503, "HLS byte capacity exhausted"))?;
        let budget = self
            .bytes
            .clone()
            .try_acquire_many_owned(size)
            .map_err(|_| failure(503, "HLS byte capacity exhausted"))?;
        Ok(Arc::new(Payload {
            data,
            _budget: budget,
            _manager: lock(&manager.admission)
                .clone()
                .ok_or_else(|| failure(503, "HLS producer retired"))?,
        }))
    }
    pub(crate) fn hls_startup_recovery(
        &self,
        token: &str,
        info: &dispatcharr_clients::HlsSessionInfo,
        active_index: Option<u64>,
    ) -> Result<Option<String>, PlaybackError> {
        let Some(session) = lock(&self.sessions).get(token).cloned() else {
            return Ok(None);
        };
        let mut recovered = lock(&session.manager.startup_recovery);
        let Some((deadline, index)) = *recovered else {
            return Ok(None);
        };
        if self.stopped.load(Ordering::Acquire) {
            return Err(failure(503, "HLS is shutting down"));
        }
        if Instant::now() >= deadline
            || session.expired.load(Ordering::Acquire)
            || session.client.id() != info.client_id
            || session.manager.channel != info.channel_id
            || session.manager.profile.is_some()
            || info
                .metadata
                .get("output_profile_id")
                .is_some_and(|value| !value.is_empty())
        {
            return Err(failure(410, "Session expired"));
        }
        if active_index.is_some_and(|latest| latest > index) {
            *recovered = None;
            return Ok(None);
        }
        Ok(Some(session.manager.fence.clone()))
    }

    async fn has_demand(&self, channel: &str, profile: Option<i64>) -> Result<bool, PlaybackError> {
        let clients = self
            .clients
            .channel_clients(channel)
            .await
            .map_err(|_| failure(503, "HLS demand unavailable"))?;
        Ok(clients.iter().any(|client| {
            client.get("output_format").and_then(|value| value.as_str()) == Some("hls")
                && client
                    .get("output_profile_id")
                    .and_then(|value| value.as_i64())
                    == profile
        }))
    }
    async fn follow_manager(
        &self,
        manager: &Arc<Manager>,
        cancelled: &mut watch::Receiver<bool>,
    ) -> Result<(), PlaybackError> {
        // A profiled mint also needs the actual remote output to start.
        // Poll once per follower manager, independent of its viewer count.
        if manager.profile.is_some() {
            let outputs = self
                .outputs
                .as_ref()
                .ok_or_else(|| failure(503, "HLS state unavailable"))?;
            let ready = async {
                loop {
                    if outputs
                        .is_active(&manager.channel, &manager.format)
                        .await
                        .map_err(|_| failure(503, "HLS state unavailable"))?
                    {
                        return Ok::<_, PlaybackError>(());
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            };
            tokio::select! {
                outcome = tokio::time::timeout(self.config.initialization_timeout, ready) =>
                    outcome.map_err(|_| failure(503, "HLS profile startup timed out"))??,
                _ = cancelled.changed() => return Ok(()),
            }
            manager.profile_start.send_replace(Some(true));
        }
        // Capability reads fetch the shared descriptor directly. A follower
        // retains only its real client leases.
        let mut idle = 0;
        loop {
            tokio::select! {_=cancelled.changed()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(10))=>{}}
            if self.has_demand(&manager.channel, manager.profile).await? {
                idle = 0;
            } else {
                idle += 1;
                if idle >= 2 {
                    return Ok(());
                }
            }
        }
    }
    async fn run_manager(self: Arc<Self>, manager: Arc<Manager>, source: PlaybackSource) {
        let mut cancelled = manager.stop.subscribe();
        let mut retained_on_startup = None;
        let result=async {
            if *cancelled.borrow() {return Ok(());}
            if let Some(outputs)=&self.outputs {
                match outputs.claim(&manager.channel,&manager.format,&manager.fence).await.map_err(|_|failure(503,"HLS ownership unavailable"))? {
                    Some(lease)=>{
                        let snapshot=outputs.snapshot(&manager.channel,&manager.format).await.map_err(|_|failure(503,"HLS state unavailable"))?;
                        {
                            let mut state = lock(&manager.state);
                            state.seq = snapshot.index + 1;
                            if let Some(descriptor) = snapshot.descriptor {
                                let (window, disc_seq, _, ts) = crate::shared::descriptor(
                                    &descriptor,
                                    self.config.max_stored_segments,
                                )?;
                                if snapshot.owner.as_deref() == Some(manager.fence.as_str())
                                    && snapshot.index > 0
                                    && !window.is_empty()
                                {
                                    let whole_published_window = window.last().is_some_and(|leaf| leaf.seq == snapshot.index);
                                    retained_on_startup = Some((snapshot.index, whole_published_window.then_some(descriptor)));
                                }
                                state.window = window.into();
                                state.disc_seq = disc_seq;
                                state.ts = ts;
                            }
                        }
                        *lock(&manager.output_lease)=Some(lease.clone());lease.set_state("initializing").await.map_err(|_|failure(503,"HLS state unavailable"))?;
                    },
                    None=>return self.follow_manager(&manager,&mut cancelled).await,
                }
            }
            let mut input=tokio::select! {result=self.engine.open_input(source)=>result?,_=cancelled.changed()=>return Ok(())};
            retained_on_startup = None;
            if self.outputs.is_none()&&!self.clients.claim_output_owner(&manager.channel,&manager.format,&manager.fence,Duration::from_secs(60)).await.map_err(|_|failure(503,"HLS ownership unavailable"))? {return Err(failure(503,"HLS output is owned by another worker"));}
            lock(&manager.state).ready=Some(Instant::now());manager.notify.notify_waiters();
            let mut segmenter=TSSegmenter::new(SegmenterConfig {target_duration:3.9,max_segment_duration:Some(6.45),max_segment_bytes:self.config.max_segment_bytes,..Default::default()}).map_err(|_|failure(500,"Segmenter unavailable"))?;
            if lock(&manager.state).seq>1 {segmenter.flag_discontinuity();}
            let lease=lock(&manager.output_lease).clone();if let Some(lease)=lease {lease.set_state("active").await.map_err(|_|failure(503,"HLS state unavailable"))?;}
            if manager.profile.is_some() {manager.profile_start.send_replace(Some(true));}
            let mut tick=tokio::time::interval(Duration::from_secs(1));let mut renew=Instant::now();let mut idle_checks=0;
            loop {tokio::select! {
                _=cancelled.changed()=>break,
                _=tick.tick()=>{
                    if renew.elapsed()>=Duration::from_secs(10) {
                        if self.outputs.is_some() {if self.has_demand(&manager.channel, manager.profile).await? {idle_checks=0;}else {idle_checks+=1;if idle_checks>=2 {break;}}}
                        self.publish(&manager,None).await?;
                        let lease=lock(&manager.output_lease).clone();
                        if let Some(lease)=lease {lease.renew().await.map_err(|_|failure(503,"HLS ownership lost"))?;}
                        else if !self.clients.renew_output_owner(&manager.channel,&manager.format,&manager.fence,Duration::from_secs(60)).await.map_err(|_|failure(503,"HLS ownership unavailable"))? {return Err(failure(503,"HLS ownership lost"));}
                        renew=Instant::now();
                    }
                },
                frame=input.next()=>{
                    let Some(frame)=frame else {break;};let frame=frame.map_err(|_|failure(502,"Upstream failed"))?;
                    if frame.discontinuity {if let Some(segment)=segmenter.flag_discontinuity() {self.publish(&manager,Some(segment)).await?;}}
                    for chunk in frame.data.chunks(64*1024) {
                        for segment in segmenter.feed(chunk).map_err(|_|failure(503,"HLS segment capacity exhausted"))? {self.publish(&manager,Some(segment)).await?;}
                    }
                }
            }}
            Ok::<(),PlaybackError>(())
        }.await;
        let allow_recovery = result.is_err()
            && !self.stopped.load(Ordering::Acquire)
            && !*cancelled.borrow()
            && manager.profile.is_none();
        {
            let mut state = lock(&manager.state);
            state.dead = true;
            state.playlist = None;
            state.stored.clear();
            state.window.clear();
        }
        manager.notify.notify_waiters();
        if manager.profile.is_some() {
            manager.profile_start.send_replace(Some(false));
        }
        let lease = lock(&manager.output_lease).take();
        let mut recovering = false;
        if let Some(lease) = lease {
            match retained_on_startup {
                Some((index, Some(descriptor))) if allow_recovery => {
                    let started = Instant::now();
                    if let Ok(ttl) = lease
                        .retain_abandoned_hls_startup(
                            index,
                            &descriptor,
                            self.config.initialization_timeout,
                        )
                        .await
                    {
                        if ttl > started.elapsed() {
                            *lock(&manager.startup_recovery) = Some((started + ttl, index));
                            lock(&manager.admission).take();
                            recovering = true;
                        }
                    }
                }
                Some((index, _)) => {
                    let _ = lease.abandon_hls_startup(index).await;
                }
                None => {
                    let _ = lease.release().await;
                }
            }
        } else if self.outputs.is_none() {
            let _ = self
                .clients
                .release_output_owner(&manager.channel, &manager.format, &manager.fence)
                .await;
        }
        {
            let mut managers = lock(&self.managers);
            if managers
                .get(&manager.key)
                .is_some_and(|current| Arc::ptr_eq(current, &manager))
            {
                managers.remove(&manager.key);
            }
        }
        if recovering {
            manager.done.send_replace(true);
            return;
        }
        let sessions: Vec<_> = lock(&self.sessions)
            .values()
            .filter(|session| Arc::ptr_eq(&session.manager, &manager))
            .cloned()
            .collect();
        let _ = tokio::time::timeout(
            Duration::from_secs(3),
            futures_util::stream::iter(sessions).for_each_concurrent(32, |session| {
                let inner = self.clone();
                async move {
                    inner.expire(&session).await;
                }
            }),
        )
        .await;
        manager.done.send_replace(true);
    }
    async fn publish(
        &self,
        manager: &Arc<Manager>,
        segment: Option<Segment>,
    ) -> Result<(), PlaybackError> {
        let shared_seq = if let Some(segment) = segment.as_ref() {
            let lease = lock(&manager.output_lease).clone();
            if let Some(lease) = lease {
                Some(
                    lease
                        .put(&segment.data, wall_time())
                        .await
                        .map_err(|_| failure(503, "HLS segment storage unavailable"))?,
                )
            } else {
                None
            }
        } else {
            None
        };
        let shared_scores = if let Some(outputs) = &self.outputs {
            Some(
                outputs
                    .snapshot(&manager.channel, &manager.format)
                    .await
                    .map_err(|_| failure(503, "HLS state unavailable"))?
                    .segments
                    .into_iter()
                    .collect::<BTreeMap<_, _>>(),
            )
        } else {
            None
        };
        let (playlist, descriptor) = {
            let mut state = lock(&manager.state);
            let now = Instant::now();
            let mut changed = false;
            if let Some(segment) = segment {
                if state.stored.len() >= self.config.max_stored_segments {
                    return Err(failure(503, "HLS segment capacity exhausted"));
                }
                let seq = shared_seq.unwrap_or(state.seq);
                state.seq = seq + 1;
                if shared_seq.is_none() {
                    let payload = self.payload(manager, segment.data)?;
                    state.stored.insert(
                        seq,
                        Stored {
                            payload,
                            created: now,
                        },
                    );
                }
                // Shared Redis media is not duplicated in an HTTP-local segment cache.

                state.window.push_back(SegmentDescriptor {
                    seq,
                    duration: segment.duration,
                    discontinuity: segment.discontinuity,
                });
                state.ts = wall_time();
                state.last_produced = Some(now);
                changed = true;
            }
            state
                .stored
                .retain(|_, stored| stored.created.elapsed() < self.config.segment_ttl);
            while state.window.front().is_some_and(|head| {
                shared_scores.as_ref().map_or_else(
                    || !state.stored.contains_key(&head.seq),
                    |scores| !scores.contains_key(&head.seq),
                )
            }) {
                if state.window.pop_front().unwrap().discontinuity {
                    state.disc_seq += 1;
                }
                changed = true;
            }
            while state.window.len() > 1
                && state.window.front().is_some_and(|head| {
                    if let Some(scores) = &shared_scores {
                        return scores.get(&head.seq).is_none_or(|stamp| {
                            wall_time() - stamp >= self.config.segment_ttl.as_secs_f64() - 4.0
                        });
                    }
                    state.stored.get(&head.seq).is_none_or(|stored| {
                        stored.created.elapsed() >= self.config.segment_ttl - Duration::from_secs(4)
                    })
                })
            {
                if state.window.pop_front().unwrap().discontinuity {
                    state.disc_seq += 1;
                }
                changed = true;
            }
            if !changed
                || (state.playlist.is_none()
                    && state.window.iter().map(|item| item.duration).sum::<f64>() + 0.1 < 4.0)
            {
                return Ok(());
            }
            let window: Vec<_> = state.window.iter().cloned().collect();
            let text = render_media_playlist(
                &window,
                4.0,
                "{seq}.ts",
                Some(6),
                state.disc_seq,
                Some(manager.behind),
            );
            let playlist = self.payload(manager, text.into_bytes())?;
            let descriptor = json!({"window":window.iter().map(|item|json!({"seq":item.seq,"dur":(item.duration*1000.0).round()/1000.0,"disc":item.discontinuity})).collect::<Vec<_>>(),
                "target":4.0,"adv_target":6,"start_behind":manager.behind,"disc_seq":state.disc_seq,"ts":state.ts});
            (playlist, descriptor)
        };
        let lease = lock(&manager.output_lease).clone();
        if let Some(lease) = lease {
            lease
                .publish(&descriptor)
                .await
                .map_err(|_| failure(503, "HLS ownership lost"))?;
        } else if !self
            .clients
            .publish_output_playlist(
                &manager.channel,
                &manager.format,
                &manager.fence,
                &descriptor,
                Duration::from_secs(3600),
            )
            .await
            .map_err(|_| failure(503, "HLS publication unavailable"))?
        {
            return Err(failure(503, "HLS ownership lost"));
        }
        lock(&manager.state).playlist = Some(playlist);
        manager.notify.notify_waiters();
        Ok(())
    }
}
impl Session {
    pub(crate) fn playlist(&self) -> Result<Option<Arc<Payload>>, PlaybackError> {
        let state = lock(&self.manager.state);
        if state.dead {
            return Err(failure(404, "Stream unavailable"));
        }
        if state
            .last_produced
            .is_some_and(|time| time.elapsed() > Duration::from_secs(45))
        {
            return Err(failure(404, "Playlist stale"));
        }
        Ok(state.playlist.clone())
    }
}
/// Source gives the ready channel a fresh playlist wait budget. Going back
/// to a starting state restores the original startup deadline.
pub(crate) fn cold_wait_exceeded(
    now: Instant,
    started: Instant,
    ready: bool,
    ready_since: &mut Option<Instant>,
    init_grace: Duration,
    ready_grace: Duration,
) -> bool {
    if ready {
        now.duration_since(*ready_since.get_or_insert(now)) >= ready_grace
    } else {
        *ready_since = None;
        now.duration_since(started) >= init_grace
    }
}

pub(crate) fn response_bytes(
    payload: Arc<Payload>,
    session: Arc<Session>,
    request: Arc<OwnedSemaphorePermit>,
) -> Bytes {
    Bytes::from_owner(FrameOwner {
        payload,
        _session: session,
        _request: request,
    })
}
pub(crate) fn session_segment(
    session: &Arc<Session>,
    seq: u64,
    request: Arc<OwnedSemaphorePermit>,
    ttl: Duration,
) -> Result<Bytes, PlaybackError> {
    let payload = lock(&session.manager.state)
        .stored
        .get(&seq)
        .filter(|stored| stored.created.elapsed() < ttl)
        .map(|stored| stored.payload.clone())
        .ok_or_else(|| failure(404, "Segment expired"))?;
    Ok(response_bytes(payload, session.clone(), request))
}

#[cfg(test)]
mod cold_wait_tests {
    use super::cold_wait_exceeded;
    use std::time::{Duration, Instant};

    #[test]
    fn ready_channel_can_wait_past_startup_deadline_and_starting_resets_ready_budget() {
        let start = Instant::now();
        let mut ready_since = None;
        let mut expired = |second, ready| {
            cold_wait_exceeded(
                start + Duration::from_secs(second),
                start,
                ready,
                &mut ready_since,
                Duration::from_secs(20),
                Duration::from_secs(10),
            )
        };
        assert!(!expired(18, true));
        assert!(!expired(19, false));
        assert!(!expired(19, true));
        assert!(!expired(28, true));
        assert!(expired(29, true));
        assert!(expired(29, false));
    }
}
