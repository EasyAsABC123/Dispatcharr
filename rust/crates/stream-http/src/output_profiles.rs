//! Shared pre-delivery MPEG-TS output profiles. Original Dispatcharr credit retained.
//! Only the actual raw TS owner creates a process; other workers read its history.
use super::*;
use dispatcharr_contracts::StreamCommand;
use dispatcharr_output_store::{
    Error as StoreError, OutputProfileOwner, OutputProfileRequest, OutputStore, ProfileEvents,
    RemoteSwitchWake,
};
use tokio::sync::watch;

type Key = (String, i64);
pub(super) struct Profiles {
    outputs: OutputStore,
    events: ProfileEvents,
    sessions: Mutex<HashMap<Key, Arc<Profile>>>,
    readers: Arc<Semaphore>,
    admitted: AtomicU64,
}
struct Profile {
    stop: watch::Sender<bool>,
    done: watch::Sender<bool>,
    fault: AtomicBool,
    _slot: OwnedSemaphorePermit,
    _worker: Arc<WorkerCount>,
}
struct Dispatcher {
    inner: Weak<Inner>,
    #[cfg(test)]
    observation_pause: Option<Arc<ObservationPause>>,
}
#[cfg(test)]
#[derive(Default)]
struct ObservationPause {
    observed: Notify,
    resume: Notify,
}
impl Dispatcher {
    fn new(inner: Weak<Inner>) -> Self {
        Self {
            inner,
            #[cfg(test)]
            observation_pause: None,
        }
    }
}
impl OutputProfileOwner for Dispatcher {
    fn ensure_owned(
        &self,
        request: OutputProfileRequest,
        owner: Arc<dyn Send + Sync>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), StoreError>> + Send + '_>> {
        Box::pin(async move {
            let Some(inner) = self.inner.upgrade() else {
                return Ok(());
            };
            let Some(profiles) = inner.profiles.as_ref().cloned() else {
                return Ok(());
            };
            let (session, observation_owner) = {
                let sessions = inner.sessions.lock().unwrap();
                if inner.slots.is_closed() {
                    return Ok(());
                }
                let Some(session) = sessions.get(&request.channel).cloned() else {
                    return Ok(());
                };
                // Linearize the actual pending observation with shutdown's raw
                // registry barrier, before the first Redis await. A timed-out
                // observer must not release physical-operation accounting.
                inner.workers.fetch_add(1, Ordering::AcqRel);
                (session, Arc::new(WorkerCount(inner.clone())))
            };
            if session.following.load(Ordering::Acquire)
                || !session.manager_visible.load(Ordering::Acquire)
                || *session.stop.borrow()
            {
                return Ok(());
            }
            let raw_owner = session.input_owner.lock().unwrap().clone();
            let Some(raw_owner) = raw_owner else {
                return Ok(());
            };
            let Some(inputs) = inner.inputs.as_ref() else {
                return Err(StoreError::Configuration);
            };
            let (snapshot, _scope) = inputs
                .snapshot_retained(
                    &request.channel,
                    (owner.clone(), session.clone(), observation_owner),
                )
                .await
                .map_err(|_| StoreError::Unavailable)?;
            if snapshot.owner.as_deref() != Some(raw_owner.as_str()) {
                return Ok(());
            }
            #[cfg(test)]
            if let Some(pause) = &self.observation_pause {
                // Hold a real completed Redis observation at the exact await
                // boundary to deterministically exercise concurrent real stop.
                pause.observed.notify_one();
                pause.resume.notified().await;
            }
            let key = (request.channel.clone(), request.profile_id);
            let running = {
                let mut sessions = profiles.sessions.lock().unwrap();
                // Raw teardown sets stop BEFORE locking this same registry.
                // Either it sees this accepted producer, or this recheck sees
                // stop. No producer may escape the teardown snapshot.
                if *session.stop.borrow()
                    || session.following.load(Ordering::Acquire)
                    || !session.manager_visible.load(Ordering::Acquire)
                    || inner.slots.is_closed()
                    || session.input_owner.lock().unwrap().as_deref() != Some(raw_owner.as_str())
                {
                    return Ok(());
                }
                if let Some(previous) = sessions.get(&key) {
                    if !*previous.done.borrow() {
                        return Ok(());
                    }
                }
                let permit = inner
                    .slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| StoreError::Capacity)?;
                let (stop, _) = watch::channel(false);
                let (done, _) = watch::channel(false);
                inner.workers.fetch_add(1, Ordering::AcqRel);
                let running = Arc::new(Profile {
                    stop,
                    done,
                    fault: AtomicBool::new(false),
                    _slot: permit,
                    _worker: Arc::new(WorkerCount(inner.clone())),
                });
                profiles.admitted.fetch_add(1, Ordering::AcqRel);
                sessions.insert(key.clone(), running.clone());
                running
            };
            tokio::spawn(run(
                inner,
                profiles,
                key,
                running,
                Producer {
                    session,
                    raw_owner,
                    argv: request.command,
                    parent: owner,
                },
            ));
            Ok(())
        })
    }

    fn switch_source_owned(
        &self,
        wake: RemoteSwitchWake,
        owner: Arc<dyn Send + Sync>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), StoreError>> + Send + '_>> {
        Box::pin(async move {
            let Some(inner) = self.inner.upgrade() else {
                return Ok(());
            };
            let Some(store) = inner
                .profiles
                .as_ref()
                .map(|profiles| profiles.outputs.clone())
            else {
                return Ok(());
            };
            remote_source_switch::dispatch(inner, store, wake, owner)
        })
    }
}
impl StreamEngine {
    /// Bind once before engine sharing, using Main's existing OutputStore and
    /// retained application lifetime. This does not activate resolver HTTP501.
    pub fn with_output_profiles(
        mut self,
        outputs: OutputStore,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Self, PlaybackError> {
        if self.inner.inputs.is_none()
            || self.inner.coordinator.is_none()
            || self.inner.clients.is_none()
        {
            return Err(PlaybackError::new(
                503,
                "Shared profile dependencies unavailable",
            ));
        }
        let events = outputs.profile_events().map_err(store_error)?;
        let readers = self
            .inner
            .config
            .max_channels
            .checked_mul(self.inner.config.max_viewers_per_channel)
            .filter(|count| *count <= Semaphore::MAX_PERMITS)
            .ok_or_else(|| PlaybackError::new(500, "Profile reader capacity overflow"))?;
        let profiles = Arc::new(Profiles {
            outputs,
            events: events.clone(),
            sessions: Mutex::new(HashMap::new()),
            readers: Arc::new(Semaphore::new(readers)),
            admitted: AtomicU64::new(0),
        });
        let inner = Arc::get_mut(&mut self.inner).ok_or_else(|| {
            PlaybackError::new(500, "Output profiles must be bound before engine sharing")
        })?;
        if inner.profiles.is_some() {
            return Err(PlaybackError::new(
                500,
                "Output profiles already configured",
            ));
        }
        inner.profiles = Some(profiles);
        events
            .start(
                Arc::new(Dispatcher::new(Arc::downgrade(&self.inner))),
                owner,
            )
            .map_err(store_error)?;
        Ok(self)
    }
    /// Bounded internal diagnostics; command argv/stderr can contain provider
    /// credentials and are deliberately absent from this public value.
    pub fn output_profile_diagnostics(&self) -> serde_json::Value {
        let Some(profiles) = &self.inner.profiles else {
            return serde_json::json!({"configured":false});
        };
        let event = profiles.events.observation();
        let sessions = profiles.sessions.lock().unwrap().iter().map(|((channel,profile),session)| serde_json::json!({
            "channel_id":channel,"profile_id":profile,"stopping":*session.stop.borrow(),
            "physical_done":*session.done.borrow(),"uncertain":session.fault.load(Ordering::Acquire),
        })).collect::<Vec<_>>();
        let demands=self.inner.sessions.lock().unwrap().iter().map(|(channel,session)|serde_json::json!({
            "channel_id":channel,"internal_profile_demands":session.profile_demands.load(Ordering::Acquire),
            "raw_transport_subscriptions":session.channel.viewer_count(),
        })).collect::<Vec<_>>();
        serde_json::json!({"configured":true,"profiles":sessions,"local_channel_demand":demands,"actual_profile_producer_admissions":profiles.admitted.load(Ordering::Acquire),"event_reader_closed":event.reader_closed,
            "event_callbacks_completed":event.applied,"event_requests_observed":event.observed,
            "event_requests_rejected":event.rejected})
    }
    pub async fn output_profiles_ready(&self, budget: Duration) -> bool {
        let Some(profiles) = self.inner.profiles.as_ref() else {
            return false;
        };
        profiles.events.wait_ready(budget).await
    }
    /// Caller supplies the actual authorized channel resolution and actual
    /// active OutputProfile command. Existing auth/provider rules are unchanged.
    pub async fn play_output_profile(
        &self,
        source: PlaybackSource,
        profile: i64,
        command: StreamCommand,
    ) -> Result<Response<Body>, PlaybackError> {
        let stream = self.open_profile(source, profile, command, true).await?;
        Response::builder()
            .header("Content-Type", "video/mp2t")
            .header("Cache-Control", "no-cache")
            .header("Connection", "close")
            .body(Body::from_stream(
                stream.map(|frame| frame.map(|frame| frame.data)),
            ))
            .map_err(|_| PlaybackError::new(500, "Profile response initialization failed"))
    }
    /// A real format manager may subscribe after separately registering its
    /// actual HLS/fMP4 clients with the SAME profile ID. It must also key its own
    /// segmenter/output namespace by profile, not reuse the untransformed lane.
    pub async fn open_output_profile(
        &self,
        mut source: PlaybackSource,
        profile: i64,
        command: StreamCommand,
    ) -> Result<MediaStream, PlaybackError> {
        source.viewer = None;
        self.open_profile(source, profile, command, false).await
    }
    async fn open_profile(
        &self,
        mut source: PlaybackSource,
        profile: i64,
        command: StreamCommand,
        register: bool,
    ) -> Result<MediaStream, PlaybackError> {
        if profile <= 0
            || !command.spawn_compatible()
            || command.argv.len() > 4096
            || command
                .argv
                .iter()
                .try_fold(0usize, |n, v| n.checked_add(v.len()))
                .is_none_or(|n| n > 256 * 1024)
        {
            return Err(PlaybackError::new(501, "Unsupported output profile argv"));
        }
        source.output_profile = None;
        let profiles = self
            .inner
            .profiles
            .as_ref()
            .cloned()
            .ok_or_else(|| PlaybackError::new(501, "Output profiles are not configured"))?;
        let reader = Arc::new(
            profiles
                .readers
                .clone()
                .try_acquire_owned()
                .map_err(|_| PlaybackError::new(503, "Profile reader capacity reached"))?,
        );
        let clients = self
            .inner
            .clients
            .as_ref()
            .ok_or_else(|| PlaybackError::new(503, "Profile client metadata unavailable"))?;
        let client = if register {
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
                    .register_profile(&source.channel_id, &viewer, "mpegts", Some(profile))
                    .await
                    .map_err(|error| client_error(error, viewer.stream_limit))?,
            )
        } else {
            None
        };
        // Establish/join the SAME raw session before asking its actual TS owner.
        // A temporary subscription has no phantom ClientStore registration.
        let mut raw = source.clone();
        raw.viewer = None;
        let raw_subscription = self.open_stream(raw, false).await?;
        let request = OutputProfileRequest {
            channel: source.channel_id.clone(),
            profile_id: profile,
            command: command.argv,
        };
        let local = self
            .inner
            .sessions
            .lock()
            .unwrap()
            .get(&source.channel_id)
            .is_some_and(|s| !s.following.load(Ordering::Acquire));
        if local {
            Dispatcher::new(Arc::downgrade(&self.inner))
                .ensure_owned(request, Arc::new(()))
                .await
                .map_err(store_error)?;
        } else {
            profiles
                .events
                .request_retained(&request, (client.clone(), profiles.clone()))
                .await
                .map_err(store_error)?;
        }
        let store = profiles
            .outputs
            .profile_with_chunk_ttl(profile, self.inner.config.retention)
            .map_err(store_error)?;
        let deadline = Instant::now() + Duration::from_secs(5);
        let first = loop {
            if let Some(snapshot) = store
                .snapshot_ready_for_reader(&source.channel_id)
                .await
                .map_err(store_error)?
            {
                break snapshot;
            }
            if Instant::now() >= deadline {
                return Err(PlaybackError::new(
                    503,
                    "Output profile initialization failed",
                ));
            }
            time::sleep(Duration::from_millis(100)).await;
        };
        // Transfer the temporary internal raw viewer to a lightweight demand
        // guard BEFORE dropping it. A follower with no raw data viewers must
        // still retain its real transformed clients; retiring that follower
        // calls stop_local_channel and would cancel these live clients.
        let demand = {
            let sessions = self.inner.sessions.lock().unwrap();
            let session = sessions
                .get(&source.channel_id)
                .cloned()
                .ok_or_else(|| PlaybackError::new(503, "Profile raw follower disappeared"))?;
            if *session.stop.borrow() || self.inner.slots.is_closed() {
                return Err(PlaybackError::new(503, "Profile raw follower is stopping"));
            }
            // Every demand owns the already acquired profile-reader admission;
            // counts cannot exceed that existing bounded semaphore.
            session.profile_demands.fetch_add(1, Ordering::AcqRel);
            Arc::new(ProfileDemand {
                inner: Arc::downgrade(&self.inner),
                session,
                key: source.channel_id.clone(),
                _reader: reader.clone(),
            })
        };
        drop(raw_subscription);
        let expected = first.owner.unwrap();
        let behind = source.startup_behind.as_secs_f64();
        let wanted = epoch_seconds() - behind;
        let compare = |a: &&(u64, f64), b: &&(u64, f64)| {
            a.1.total_cmp(&b.1)
                .then_with(|| a.0.to_string().cmp(&b.0.to_string()))
        };
        let mut sequence = if behind == 0.0 {
            first.index.saturating_add(1)
        } else {
            first
                .segments
                .iter()
                .filter(|(_, stamp)| *stamp <= wanted)
                .max_by(compare)
                .or_else(|| first.segments.iter().min_by(compare))
                .map_or(first.index.saturating_add(1), |(seq, _)| *seq)
        };
        let health_engine = self.clone();
        let health_retained: Arc<dyn Send + Sync> = Arc::new((
            client.clone(),
            reader.clone(),
            profiles.clone(),
            demand.clone(),
        ));
        let raw_owner = self
            .inner
            .sessions
            .lock()
            .unwrap()
            .get(&source.channel_id)
            .and_then(|session| session.input_owner.lock().unwrap().clone());
        let health_history = self
            .inner
            .inputs
            .as_ref()
            .zip(raw_owner)
            .map(|(store, raw_owner)| history::History {
                store: store.clone(),
                channel: source.channel_id.clone(),
                owner: raw_owner,
                initial_behind_chunks: self.inner.config.initial_buffer_chunks,
                read_timeout: self.inner.config.read_timeout,
                retained: Some(health_retained),
            });
        let source_controls = register && self.inner.config.source_initialization;
        let interval = self.inner.config.keepalive_interval;
        let maximum = self.inner.config.max_keepalive_duration;
        let channel = source.channel_id;
        let stream = async_stream::stream! {
            let _profiles=profiles;
            let reader=reader;
            let demand=demand;
            let client=client;
            let mut stopped=client.as_ref().map(ClientLease::stopped);
            let mut source_stopped=demand.session.stop.subscribe();
            let delivery=Arc::new(Semaphore::new(1));
            let mut keepalive=source_keepalive::SourceKeepalive::new();
            let mut remote=history::RemoteHealth::default();
            loop{
                if *source_stopped.borrow() || stopped.as_ref().is_some_and(|stop| *stop.borrow()) {break;}
                let snapshot=match store.snapshot(&channel).await{Ok(v)=>v,Err(_)=>{yield Err(io::Error::other("Profile history unavailable"));break;}};
                if snapshot.owner.as_deref()!=Some(expected.as_str()){break;}
                if let Some((oldest,_))=snapshot.segments.first(){sequence=sequence.max(*oldest);}
                if sequence<=snapshot.index{
                    // A held response cannot reserve another Redis payload.
                    // Snapshot above still observes an already retired owner.
                    let Some(permit)=next_profile_delivery(&delivery,&mut stopped,&mut source_stopped).await else {break;};
                    let retained=(permit,client.clone(),reader.clone(),demand.clone(),_profiles.clone());
                    let payload=if register {
                        tokio::select!{
                            biased;
                            _=wait_profile_client_stop(&mut stopped)=>break,
                            _=source_stopped.changed()=>break,
                            result=store.get_owned_wait_retained(&channel,&expected,sequence,retained)=>result,
                        }
                    }else{
                        store.get_owned_retained(&channel,&expected,sequence,retained).await
                    };
                    match payload{
                        Ok((Some(data),(permit,client_owner,reader_owner,demand_owner,profile_owner)))=>{
                            sequence=sequence.saturating_add(1);
                            keepalive.received();
                            if let Some(client)=client.as_ref(){client.record_bytes(data.as_ref().len());}
                            // Actual media-frame ownership retains the profile
                            // client beyond stream cancellation/backpressure.
                            let frame=HeldFrame{data,_client:client_owner,_profiles:profile_owner,_reader:reader_owner,_demand:demand_owner};
                            let delivered=ProfileDelivery{frame,_permit:permit};
                            yield Ok(MediaFrame{data:Bytes::from_owner(delivered),discontinuity:false});
                        },
                        Ok((None,_retained))=>sequence=sequence.saturating_add(1),
                        Err(StoreError::LostOwnership)=>break,
                        Err(_)=>{yield Err(io::Error::other("Profile payload unavailable"));break;},
                    }
                }else{
                    if source_controls {
                        let healthy=if let Some(manager)=health_engine.local_manager_status(&channel){manager.healthy}
                            else if !keepalive.health_check_due(true){true}
                            else if let Some(history)=health_history.as_ref(){
                                let timeout=async{effective_health_timing(health_engine.inner.config.health_timing(),health_engine.inner.health_settings.as_ref()).await.map(|timing|timing.connection)};
                                !history.source_unhealthy(&mut remote,epoch_seconds(),timeout).await
                            }else{true};
                        match keepalive.empty(true,healthy,Instant::now().into_std(),interval,maximum){
                            source_keepalive::Decision::Stop=>break,
                            source_keepalive::Decision::Wait=>{},
                            source_keepalive::Decision::Send=>{
                                let Some(permit)=next_profile_delivery(&delivery,&mut stopped,&mut source_stopped).await else {break;};
                                if let Some(client)=client.as_ref(){client.record_keepalive(188);}
                                let frame=ProfileKeepalive{_client:client.clone(),_reader:reader.clone(),_profiles:_profiles.clone(),_demand:demand.clone()};
                                let delivered=ProfileDelivery{frame,_permit:permit};
                                yield Ok(MediaFrame{data:Bytes::from_owner(delivered),discontinuity:false});
                            },
                        }
                    }
                    tokio::select!{
                        _=wait_profile_client_stop(&mut stopped)=>break,
                        _=source_stopped.changed()=>break,
                        _=time::sleep(if source_controls {keepalive.delay()}else{Duration::from_millis(50)})=>{},
                    }
                }
            }
        };
        Ok(Box::pin(stream))
    }
}
/// A profile viewer is channel demand, not an additional ordinary raw client.
/// Keep no media stream/payload queue here. Its existing reader permit bounds
/// demand count, and every handed-off frame retains this same guard.
struct ProfileDemand {
    inner: Weak<Inner>,
    session: Arc<Session>,
    key: String,
    _reader: Arc<OwnedSemaphorePermit>,
}
impl Drop for ProfileDemand {
    fn drop(&mut self) {
        self.session.profile_demands.fetch_sub(1, Ordering::AcqRel);
        // Reuse the existing last-local-demand idle/remote-client cleanup path.
        // Its pointer fence never retires a successor session at the same key.
        drop(ViewerGuard {
            inner: self.inner.clone(),
            session: self.session.clone(),
            key: self.key.clone(),
        });
    }
}
async fn wait_profile_client_stop(stopped: &mut Option<watch::Receiver<bool>>) {
    match stopped {
        Some(stop) => {
            if !*stop.borrow() {
                let _ = stop.changed().await;
            }
        }
        None => std::future::pending::<()>().await,
    }
}

async fn next_profile_delivery(
    delivery: &Arc<Semaphore>,
    client_stopped: &mut Option<watch::Receiver<bool>>,
    source_stopped: &mut watch::Receiver<bool>,
) -> Option<OwnedSemaphorePermit> {
    if *source_stopped.borrow() {
        return None;
    }
    tokio::select! {
        biased;
        _ = wait_profile_client_stop(client_stopped) => None,
        _ = source_stopped.changed() => None,
        permit = delivery.clone().acquire_owned() => permit.ok(),
    }
}

/// The lease stays attached to the last Bytes owner, including cloned/sliced
/// transport buffers. The shared store's byte reservation is held by the frame.
struct ProfileDelivery<T> {
    frame: T,
    _permit: OwnedSemaphorePermit,
}
impl<T: AsRef<[u8]>> AsRef<[u8]> for ProfileDelivery<T> {
    fn as_ref(&self) -> &[u8] {
        self.frame.as_ref()
    }
}
struct HeldFrame {
    data: dispatcharr_output_store::SegmentBytes,
    _client: Option<ClientLease>,
    _profiles: Arc<Profiles>,
    _reader: Arc<OwnedSemaphorePermit>,
    _demand: Arc<ProfileDemand>,
}
impl AsRef<[u8]> for HeldFrame {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}
struct ProfileKeepalive {
    _client: Option<ClientLease>,
    _reader: Arc<OwnedSemaphorePermit>,
    _profiles: Arc<Profiles>,
    _demand: Arc<ProfileDemand>,
}
impl AsRef<[u8]> for ProfileKeepalive {
    fn as_ref(&self) -> &[u8] {
        &KEEPALIVE_PACKET
    }
}
fn store_error(_: StoreError) -> PlaybackError {
    PlaybackError::new(503, "Shared output profile unavailable")
}
impl Profiles {
    pub(super) fn switch_store(&self) -> &OutputStore {
        &self.outputs
    }

    pub(super) fn request_stop(&self) {
        self.readers.close();
        self.events.request_shutdown();
        for session in self.sessions.lock().unwrap().values() {
            session.stop.send_replace(true);
        }
    }
    pub(super) async fn stop_channel(&self, channel: &str) {
        let selected = self
            .sessions
            .lock()
            .unwrap()
            .iter()
            .filter(|((id, _), _)| id == channel)
            .map(|(_, s)| s.clone())
            .collect::<Vec<_>>();
        for session in &selected {
            session.stop.send_replace(true);
        }
        for session in selected {
            let mut done = session.done.subscribe();
            while !*done.borrow_and_update() {
                if done.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
            }
        }
    }
    pub(super) fn reader_settled(&self) -> bool {
        self.events.observation().reader_closed
    }
}
async fn uncertain(running: &Profile) {
    running.fault.store(true, Ordering::Release);
    // An ambiguous Redis effect is not a normal retirement. Keep the original
    // bounded process/channel/worker admission visible; Main drain must fail.
    std::future::pending::<()>().await;
}
async fn captured_raw_input(
    inner: &Arc<Inner>,
    session: Arc<Session>,
    owner: &str,
    retained: Arc<dyn Send + Sync>,
) -> Result<MediaStream, PlaybackError> {
    if *session.stop.borrow()
        || session.following.load(Ordering::Acquire)
        || !session.manager_visible.load(Ordering::Acquire)
    {
        return Err(PlaybackError::new(503, "Raw profile source is stopping"));
    }
    let viewer = session
        .channel
        .subscribe(session.source.startup_behind)
        .map_err(runtime_error)?;
    let mut state = TransportState {
        viewer,
        client: None,
        _guard: ViewerGuard {
            inner: Arc::downgrade(inner),
            session: session.clone(),
            key: session.source.channel_id.clone(),
        },
    };
    let store = inner
        .inputs
        .as_ref()
        .ok_or_else(|| PlaybackError::new(503, "Shared raw profile history unavailable"))?;
    let history = history::History {
        store: store.clone(),
        channel: session.source.channel_id.clone(),
        owner: owner.to_owned(),
        initial_behind_chunks: inner.config.initial_buffer_chunks,
        read_timeout: inner.config.read_timeout,
        retained: Some(Arc::new((retained, state._guard.clone()))),
    };
    history
        .position(&mut state.viewer, session.source.startup_behind)
        .await
        .map_err(|_| PlaybackError::new(503, "Raw profile history position unavailable"))?;
    if *session.stop.borrow() {
        return Err(PlaybackError::new(
            503,
            "Raw profile source stopped during setup",
        ));
    }
    Ok(Box::pin(async_stream::stream! {
        let mut state = state;
        let history = history;
        loop {
            match history::next_observed(&mut state.viewer,Some(&history),None).await {
                Ok(Some(chunk)) => {
                    let discontinuity=chunk.discontinuity();
                    let frame=FrameOwner { chunk, _client: None, _guard: state._guard.clone() };
                    yield Ok(MediaFrame { data: Bytes::from_owner(frame), discontinuity });
                },
                Ok(None) => break,
                Err(error) => { yield Err(error); break; },
            }
        }
    }))
}
struct Producer {
    session: Arc<Session>,
    raw_owner: String,
    argv: Vec<String>,
    parent: Arc<dyn Send + Sync>,
}
async fn run(
    inner: Arc<Inner>,
    profiles: Arc<Profiles>,
    key: Key,
    running: Arc<Profile>,
    producer: Producer,
) {
    let Producer {
        session,
        raw_owner,
        argv,
        parent,
    } = producer;
    let store = match profiles
        .outputs
        .profile_with_chunk_ttl(key.1, inner.config.retention)
    {
        Ok(v) => v,
        Err(_) => {
            finish(&profiles, &key, &running);
            return;
        }
    };
    if *running.stop.borrow() || *session.stop.borrow() {
        finish(&profiles, &key, &running);
        return;
    }
    let retained: Arc<dyn Send + Sync> = Arc::new((running.clone(), parent));
    let token = uuid::Uuid::new_v4().to_string();
    let lease = match store
        .claim_retained(&key.0, &raw_owner, &token, retained.clone())
        .await
    {
        Ok((Some(lease), _)) => lease,
        Ok((None, _)) => {
            finish(&profiles, &key, &running);
            return;
        }
        Err(StoreError::Unavailable | StoreError::Corrupt) => {
            uncertain(&running).await;
            return;
        }
        Err(_) => {
            finish(&profiles, &key, &running);
            return;
        }
    };
    // This is the already authenticated, actually owned raw session captured
    // before admission. Never resolve/open a new channel from old source data.
    let raw = captured_raw_input(&inner, session.clone(), &raw_owner, retained.clone()).await;
    let mut process = match raw {
        Ok(raw) if !*running.stop.borrow() && !*session.stop.borrow() => {
            profile_process::InputProcess::start_transform(
                &StreamCommand { argv },
                raw,
                inner.config.read_timeout,
                retained.clone(),
            )
            .ok()
        }
        _ => None,
    };
    if process.is_some() {
        if let Err(error) = lease.set_state_retained("active", retained.clone()).await {
            let receipt = process.take().unwrap().finish().await;
            if !receipt.as_ref().is_ok_and(|receipt| {
                receipt.group_settled && receipt.stdout_eof && receipt.stderr_eof
            }) {
                uncertain(&running).await;
            }
            // An actual Redis comparison proved the token no longer owns this
            // lane. Its process is now physically settled; fenced release below
            // may only remove our own generation. This is not an unknown effect.
            if error != StoreError::LostOwnership {
                uncertain(&running).await;
            }
        }
    }
    if let Some(mut process) = process {
        let mut stop = running.stop.subscribe();
        let mut tick = time::interval(Duration::from_secs(1));
        let mut renewed = Instant::now();
        let mut buffered = Vec::with_capacity(inner.config.chunk_bytes);
        let mut ambiguous = false;
        let mut ownership_lost = false;
        loop {
            if *stop.borrow() {
                break;
            }
            tokio::select! {
                _=stop.changed()=>break,
                _=tick.tick()=>{
                    if let Some(clients)=inner.clients.as_ref(){
                        match clients.output_profile_client_count(&key.0,key.1,retained.clone()).await{
                            Ok(0)=>break,Ok(_)=>{},Err(_)=>{ambiguous=true;break;},
                        }
                    }
                    if renewed.elapsed()>=Duration::from_secs(60){
                        if let Err(error)=lease.renew_retained(retained.clone()).await{
                            ambiguous=error!=StoreError::LostOwnership;break;
                        }
                        renewed=Instant::now();
                    }
                },
                frame=process.next()=>{
                    let Some(Ok(data))=frame else{break;};
                    let mut input=data.as_ref();
                    while !input.is_empty(){
                        let take=input.len().min(inner.config.chunk_bytes-buffered.len());
                        buffered.extend_from_slice(&input[..take]);input=&input[take..];
                        if buffered.len()==inner.config.chunk_bytes{
                            if let Err(error)=lease.put_retained(&buffered,epoch_seconds(),retained.clone()).await{
                                ambiguous=error!=StoreError::LostOwnership;
                                ownership_lost=true;break;
                            }
                            buffered.clear();
                        }
                    }
                    if ambiguous || ownership_lost {break;}
                },
            }
        }
        let receipt = process.finish().await;
        if !receipt
            .as_ref()
            .is_ok_and(|receipt| receipt.group_settled && receipt.stdout_eof && receipt.stderr_eof)
        {
            uncertain(&running).await;
        }
        if ambiguous {
            uncertain(&running).await;
        }
    }
    // Source stops a producer's partial final buffer; do not invent tail flush.
    match lease.release_retained(retained).await {
        Ok((_released, _)) => finish(&profiles, &key, &running),
        Err(_) => uncertain(&running).await,
    }
}
fn finish(profiles: &Profiles, key: &Key, running: &Arc<Profile>) {
    let mut sessions = profiles.sessions.lock().unwrap();
    if sessions
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, running))
    {
        sessions.remove(key);
    }
    running.done.send_replace(true);
}

#[cfg(test)]
mod profile_delivery_tests {
    use super::*;
    use std::task::Poll;

    #[tokio::test]
    async fn slow_profile_body_retains_one_delivery_without_blocking_peer() {
        let slow = Arc::new(Semaphore::new(1));
        let fast = Arc::new(Semaphore::new(1));
        let (_source_stop, source_stopped) = watch::channel(false);
        let mut slow_source = source_stopped.clone();
        let mut fast_source = source_stopped.clone();
        let mut slow_client = None;
        let mut fast_client = None;
        let first = next_profile_delivery(&slow, &mut slow_client, &mut slow_source)
            .await
            .unwrap();
        let delivered = Bytes::from_owner(ProfileDelivery {
            frame: &b"packet"[..],
            _permit: first,
        });
        let transport = delivered.slice(1..);
        drop(delivered);

        let waiting = next_profile_delivery(&slow, &mut slow_client, &mut slow_source);
        tokio::pin!(waiting);
        assert!(matches!(futures_util::poll!(&mut waiting), Poll::Pending));
        assert!(
            next_profile_delivery(&fast, &mut fast_client, &mut fast_source)
                .await
                .is_some()
        );
        assert_eq!(transport.as_ref(), b"acket");
        drop(transport);
        assert!(waiting.await.is_some());
    }

    #[tokio::test]
    async fn stopping_viewer_or_source_does_not_wait_for_held_profile_delivery() {
        for stop_source in [false, true] {
            let delivery = Arc::new(Semaphore::new(1));
            let held = delivery.clone().try_acquire_owned().unwrap();
            let (source_stop, mut source_stopped) = watch::channel(false);
            let (client_stop, client_stopped) = watch::channel(false);
            let mut client_stopped = Some(client_stopped);
            let waiting =
                next_profile_delivery(&delivery, &mut client_stopped, &mut source_stopped);
            tokio::pin!(waiting);
            assert!(matches!(futures_util::poll!(&mut waiting), Poll::Pending));

            if stop_source {
                source_stop.send_replace(true);
            } else {
                client_stop.send_replace(true);
            }
            assert!(waiting.await.is_none());
            assert_eq!(delivery.available_permits(), 0);
            drop(held);
            assert_eq!(delivery.available_permits(), 1);
        }
    }
}

#[cfg(test)]
mod stop_races {
    use super::*;
    use axum::{routing::get, Router};
    use dispatcharr_clients::ClientConfig;
    use dispatcharr_coordination::LeaseConfig;
    use dispatcharr_input_store::InputConfig;
    use dispatcharr_output_store::OutputConfig;
    use http_body_util::BodyExt;
    use std::convert::Infallible;
    use tokio::net::TcpListener;

    #[tokio::test]
    #[ignore = "requires real isolated Redis; actual HTTP/raw session and held dispatcher observation"]
    async fn real_stop_during_observed_await_refuses_profile_admission_and_captured_source_reopen()
    {
        let redis = std::env::var("REDIS_TEST_URL").expect("owned isolated Redis required");
        let channel = format!("profile-stop-{}", uuid::Uuid::new_v4());
        let connections = Arc::new(AtomicUsize::new(0));
        let counted = connections.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let origin = Router::new().route(
            "/live",
            get(move || {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::AcqRel);
                    Body::from_stream(async_stream::stream! {
                        loop {
                            let mut packet = [0u8;188];
                            packet[..4].copy_from_slice(&[0x47,0,100,0x10]);
                            yield Ok::<_,Infallible>(Bytes::copy_from_slice(&packet));
                            time::sleep(Duration::from_millis(2)).await;
                        }
                    })
                }
            }),
        );
        let origin = tokio::spawn(async move {
            axum::serve(listener, origin).await.unwrap();
        });
        let coordinator = Coordinator::connect(&redis, LeaseConfig::default())
            .await
            .unwrap();
        let clients = ClientStore::connect(&redis, ClientConfig::default())
            .await
            .unwrap();
        let inputs = InputStore::connect(&redis, InputConfig::default())
            .await
            .unwrap();
        let outputs = OutputStore::connect(&redis, OutputConfig::default())
            .await
            .unwrap();
        let engine = StreamEngine::new_with_shared_inputs(
            EngineConfig {
                max_channels: 4,
                chunk_bytes: 188 * 2,
                initial_buffer_chunks: 1,
                ..EngineConfig::default()
            },
            Some(coordinator),
            Some(clients.clone()),
            Some(inputs.clone()),
        )
        .unwrap()
        .with_output_profiles(outputs, Arc::new(()))
        .unwrap();
        assert!(engine.output_profiles_ready(Duration::from_secs(3)).await);
        let source = PlaybackSource {
            output_profile: None,
            input_command: None,
            channel_id: channel.clone(),
            channel_name: "Actual stop peer".into(),
            stream_id: 1,
            profile_id: 1,
            provider_profile_id: None,
            provider_max_streams: 0,
            credential_pool: None,
            url: format!("http://{address}/live"),
            user_agent: "Actual profile stop peer".into(),
            headers: vec![],
            metadata: vec![],
            output_format: Default::default(),
            shutdown_delay: Duration::ZERO,
            startup_behind: Duration::ZERO,
            alternates: vec![],
            viewer: None,
        };
        let mut raw = engine.play(source).await.unwrap().into_body();
        drop(raw.frame().await.unwrap().unwrap());
        let session = engine
            .inner
            .sessions
            .lock()
            .unwrap()
            .get(&channel)
            .unwrap()
            .clone();
        let raw_owner = session.input_owner.lock().unwrap().clone().unwrap();
        let pause = Arc::new(ObservationPause::default());
        let dispatcher = Dispatcher {
            inner: Arc::downgrade(&engine.inner),
            observation_pause: Some(pause.clone()),
        };
        let request = OutputProfileRequest {
            channel: channel.clone(),
            profile_id: 7,
            command: vec!["/bin/cat".into()],
        };
        let pending =
            tokio::spawn(async move { dispatcher.ensure_owned(request, Arc::new(())).await });
        // The hook fires only after the production Dispatcher completed its
        // actual successful InputStore Redis read with the real owner token.
        time::timeout(Duration::from_secs(3), pause.observed.notified())
            .await
            .unwrap();
        assert!(!pending.is_finished());
        assert!(engine.stop(&channel));
        // Require actual owner cleanup, not merely a local stop flag, before
        // asking the captured session to supply a raw subscription again.
        time::timeout(Duration::from_secs(8), async {
            while inputs.snapshot(&channel).await.unwrap().owner.is_some() {
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            captured_raw_input(&engine.inner, session, &raw_owner, Arc::new(()))
                .await
                .is_err()
        );
        pause.resume.notify_one();
        pending.await.unwrap().unwrap();
        let diagnostics = engine.output_profile_diagnostics();
        let admitted = diagnostics["actual_profile_producer_admissions"]
            .as_u64()
            .unwrap();
        let active = diagnostics["profiles"].as_array().unwrap().len();
        let requests = connections.load(Ordering::Acquire);
        drop(raw);
        let drained = engine.shutdown_wait(Duration::from_secs(8)).await;
        clients.shutdown().await;
        origin.abort();
        let _ = origin.await;
        assert_eq!(
            admitted, 0,
            "a real successful pre-stop snapshot cannot grant post-stop admission"
        );
        assert_eq!(active, 0);
        assert_eq!(
            requests, 1,
            "captured stopped source must not reopen an upstream/provider"
        );
        assert!(drained);
    }
}
