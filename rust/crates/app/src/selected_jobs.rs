//! Experimental selected-family + startup recovery assembly. Original Dispatcharr,
//! Celery/Kombu/django-celery-beat and dependency authors retain all credit.
//! Does not claim Source47 parity or default-on/full scheduler acceptance.
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_job_transport as jobs;
use dispatcharr_store::{AuthService, Store};
use dispatcharr_stream_http::StreamEngine;
use jobs::milestone_dispatch::{Family, Handler, Lifecycle};
use std::{
    env,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{watch, Semaphore};
#[derive(Clone, Debug)]
enum Startup {
    Prepared,
    WaitingForHttp,
    Subscribing,
    WorkersRunning,
    PublishingRecovery,
    Started,
    Stopping,
    Stopped,
    Fault(&'static str),
}
struct Inner {
    plugin_refresh: jobs::normal_plugin_repos::Prepared,
    queue: jobs::normal_queue::Application,
    scanner: jobs::normal_scanner::Prepared,
    shared_jobs: jobs::transport::Jobs,
    publisher: jobs::normal_recording_publication::NormalRecordingPublisher,
    dvr: Arc<jobs::recording_dvr_normal::NormalDvr>,
    recovery: jobs::normal_recording_recovery::Worker,
    scheduler: jobs::normal_beat::Scheduler,
    control: Arc<jobs::normal_control::Control>,
    startup: watch::Sender<Startup>,
    startup_slots: Arc<Semaphore>,
    started: AtomicBool,
    closed: AtomicBool,
    local: SocketAddr,
    recording_http: dispatcharr_store::recording_http::RecordingHttp,
    xc_http: dispatcharr_store::xc_account_http::Http,
    recording_playback: dispatcharr_store::RecordingPlayback,
    beat_enabled: bool,
    backups: dispatcharr_store::BackupFiles,
    backup_jobs: jobs::normal_backup::Application,
    channel_http: dispatcharr_store::channel_maintenance_http::ChannelMaintenanceHttp,
}
#[derive(Clone)]
pub struct SelectedJobs {
    inner: Arc<Inner>,
}
/// These exact existing applications retain all HTTP routers/file lookup clones.
/// Normal handlers reserve their lifecycle and start no legacy family poller.
pub struct Existing {
    pub index: jobs::index_application::IndexApplication,
    pub refresh: jobs::refresh_application::RefreshApplication,
    pub series: jobs::configured_series_application::Application,
    pub artwork: jobs::recording_existing_artwork_application::Application,
    pub tvg: jobs::tvg_programme_application::Application,
    pub version: jobs::version_application::Application,
    pub completions: jobs::ArtworkCompletions,
    pub backups: dispatcharr_store::BackupFiles,
}
pub fn enabled() -> Result<bool, String> {
    let worker = flag("RUST_EXPERIMENTAL_SELECTED_JOBS")?;
    let beat = flag("RUST_EXPERIMENTAL_SELECTED_BEAT")?;
    if beat && !worker {
        return Err(
            "Selected Beat requires selected workers; configure exactly one Beat instance".into(),
        );
    }
    Ok(worker)
}
fn flag(name: &str) -> Result<bool, String> {
    match env::var(name).as_deref() {
        Ok("1") => Ok(true),
        Ok("0") | Err(_) => Ok(false),
        _ => Err(format!("{name} must be 0 or 1")),
    }
}
fn loopback(actual: SocketAddr) -> Result<SocketAddr, String> {
    if actual.port() == 0 {
        return Err("Selected jobs require the actual bound HTTP port".into());
    }
    let ip = match actual.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip if ip.is_loopback() => ip,
        _ => {
            return Err(
                "Selected DVR currently requires a loopback or wildcard Main HTTP bind".into(),
            )
        }
    };
    Ok(SocketAddr::new(ip, actual.port()))
}
impl SelectedJobs {
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare(
        store: Store,
        auth: AuthService,
        events: Arc<dyn UpdateEventSink>,
        engine: &StreamEngine,
        actual: SocketAddr,
        existing: Existing,
    ) -> Result<Self, String> {
        let local = loopback(actual)?;
        let beat_enabled = flag("RUST_EXPERIMENTAL_SELECTED_BEAT")?;
        let beat_identity = auth.producer_identity_source();
        let cache = env::var("REDIS_URL").map_err(|_| "Selected jobs require REDIS_URL")?;
        let broker = env::var("CELERY_BROKER_URL").unwrap_or_else(|_| cache.clone());
        let backend = env::var("CELERY_RESULT_BACKEND").unwrap_or_else(|_| broker.clone());
        let plugin_directory =
            env::var("DISPATCHARR_PLUGINS_DIR").unwrap_or_else(|_| "/data/plugins".into());
        let media_root =
            PathBuf::from(env::var("RUST_EPG_MEDIA_ROOT").unwrap_or_else(|_| "/app/media".into()));
        use std::os::unix::fs::MetadataExt;
        let media_uid = std::fs::metadata(&media_root)
            .map_err(|_| "Selected jobs media root metadata unavailable")?
            .uid();
        let revoked = jobs::normal_revocation::Revoked::new();
        let control = Arc::new(
            jobs::normal_control::Control::connect(&broker, revoked.clone())
                .await
                .map_err(|_| "Selected pidbox transport is unsupported or unavailable")?,
        );
        let (publisher, publications) =
            jobs::normal_recording_publication::NormalRecordingPublisher::prepare(
                store.pool().clone(),
                &broker,
                &backend,
            )
            .await
            .map_err(|_| "Selected recording publisher initialization failed")?;
        publisher
            .set_booking_artwork(
                existing
                    .artwork
                    .booking_publisher()
                    .map_err(|_| "Selected root artwork publisher unavailable")?,
            )
            .map_err(|_| "Selected root artwork publisher composition failed")?;
        let dvr = Arc::new(
            jobs::recording_dvr_normal::NormalDvr::connect(
                jobs::recording_dvr_normal::Assembly {
                    store: store.clone(),
                    broker: broker.clone(),
                    result: backend.clone(),
                    cache: cache.clone(),
                    events: events.clone(),
                    explicit_internal_ts_base: format!("http://{local}"),
                    configured_plugin_directory: plugin_directory,
                    media_uid,
                    publisher: publisher.clone(),
                    revoked: revoked.clone(),
                    control: control.clone(),
                    publications,
                    named_publications: publisher.named_publications(),
                },
                Arc::new(()),
            )
            .await
            .map_err(|_| "Selected DVR configuration is unsupported or initialization failed")?,
        );
        let recovery_jobs = jobs::transport::RecoveryJobs::connect(&broker, &backend)
            .await
            .map_err(|_| "Selected recovery transport initialization failed")?;
        let media: Arc<dyn jobs::normal_recording_recovery::RecoveryMedia> = dvr.clone();
        let recovery_service = jobs::normal_recording_recovery::Service::prepare(
            store.pool().clone(),
            publisher.clone(),
            media,
            Some(&cache),
            Arc::new(()),
        )
        .await
        .map_err(|_| "Selected recovery initialization failed")?;
        let recovery =
            jobs::normal_recording_recovery::Worker::prepare(recovery_jobs, recovery_service);
        let shared_jobs = jobs::transport::Jobs::connect_defaults(
            &broker,
            &backend,
            jobs::transport::Config::default(),
        )
        .await
        .map_err(|_| "Selected scanner transport initialization failed")?;
        control
            .bind_backend(shared_jobs.clone())
            .map_err(|_| "Selected pidbox backend binding failed")?;
        existing.artwork.bind_normal_provider(auth.clone())?;
        let (backup_database, backup_runtime, backup_root, backup_scratch) =
            crate::backup_config::configured()?;
        let backup_jobs = jobs::normal_backup::Application::prepare(
            shared_jobs.clone(),
            revoked.clone(),
            store.clone(),
            cache.clone(),
            backup_database,
            backup_runtime,
            backup_root,
            backup_scratch,
            auth.producer_identity_source(),
        )
        .map_err(|_| "Normal backup preparation unavailable")?;
        if backup_jobs.initialize_schedule(Arc::new(())).await.is_err() {
            eprintln!("Normal backup scheduler startup sync failed");
        }
        let backups = existing
            .backups
            .clone()
            .with_jobs(Arc::new(backup_jobs.clone()));
        auth.bind_core_task_broker(Arc::new(jobs::core_settings_broker::Publisher::new(
            shared_jobs.clone(),
        )))
        .map_err(|_| "Selected Core broker port already bound or unavailable")?;
        let locks = dispatcharr_coordination::TaskLocks::connect(
            &cache,
            dispatcharr_coordination::TaskLockConfig::default(),
        )
        .await
        .map_err(|_| "Selected scanner locks initialization failed")?;
        let xc_delay = env::var("XC_PROFILE_REFRESH_DELAY")
            .unwrap_or_else(|_| "2.5".into())
            .parse::<f64>()
            .map_err(|_| "XC profile delay must be numeric")?;
        let xc_delay = Duration::try_from_secs_f64(xc_delay)
            .map_err(|_| "XC profile delay must be nonnegative and finite")?;
        let xc = jobs::normal_xc_accounts::Prepared::new(
            shared_jobs.clone(),
            auth.clone(),
            events.clone(),
            locks.clone(),
            revoked.clone(),
            xc_delay,
        )
        .map_err(|_| "Selected XC account task configuration unsupported")?;
        let xc_http =
            dispatcharr_store::xc_account_http::Http::prepare(auth.clone(), Arc::new(xc.clone()));
        let recording_playback = dispatcharr_store::RecordingPlayback::prepare(
            auth.clone(),
            dispatcharr_output_http::source_output_origin,
        );
        let recording_jobs = crate::recording_http_bridge::port(
            store.clone(),
            cache.clone(),
            publisher.clone(),
            control.clone(),
            engine.clone(),
        );
        let core_tasks = jobs::normal_core_tasks::Prepared::new(
            shared_jobs.clone(),
            store.clone(),
            auth.clone(),
            recording_jobs.clone(),
            events.clone(),
            revoked.clone(),
        )
        .map_err(|_| "Selected Core downstream task initialization failed")?;
        let plugin_refresh = jobs::normal_plugin_repos::Prepared::new(
            shared_jobs.clone(),
            auth.clone(),
            revoked.clone(),
        )
        .map_err(|_| "Selected plugin repository refresh initialization failed")?;
        let channel_tasks = jobs::normal_channel_tasks::Prepared::new(
            shared_jobs.clone(),
            store.clone(),
            events.clone(),
            revoked.clone(),
        )
        .map_err(|_| "Selected channel maintenance authority is unsupported")?;
        let channel_http =
            dispatcharr_store::channel_maintenance_http::ChannelMaintenanceHttp::prepare(
                auth.clone(),
                Arc::new(jobs::normal_channel_tasks::Publisher::new(
                    shared_jobs.clone(),
                    auth.producer_identity_source(),
                )),
            );
        let recording_http = dispatcharr_store::recording_http::RecordingHttp::prepare(
            auth.clone(),
            recording_jobs,
            events.clone(),
        );
        let scanner = jobs::normal_scanner::Prepared::connect(
            shared_jobs.clone(),
            store.clone(),
            auth,
            events.clone(),
            locks,
            &broker,
            &cache,
            jobs::transport::Config::default(),
            jobs::logo_scanner::Roots {
                m3u: PathBuf::from("/data/m3us"),
                epg: PathBuf::from("/data/epgs"),
                logos: PathBuf::from("/data/logos"),
            },
            jobs::scanner_m3u::Roots {
                input: PathBuf::from("/data/m3us"),
                cache: media_root.join("cached_m3u"),
            },
            Arc::new(|| std::time::SystemTime::now().into()),
            Arc::new(jobs::logo_task::SystemClock),
            revoked.clone(),
        )
        .await
        .map_err(|_| {
            "Selected scanner/M3U configuration is unsupported or initialization failed"
        })?;
        let defaults = jobs::normal_queue::prepare_defaults(
            store.clone(),
            events.clone(),
            &broker,
            &backend,
            &cache,
            existing.completions,
        )
        .await
        .map_err(|_| "Selected default family initialization failed")?;
        let stats = jobs::normal_queue::prepare_stats(
            &broker,
            &backend,
            crate::statistics::task_source(engine),
            events,
            crate::positive("RUST_SELECTED_STATS_BYTES", 3 * 1024 * 1024)?,
        )
        .await
        .map_err(|_| "Selected live statistics initialization failed")?;
        let mut entries: Vec<(Family, Arc<dyn Handler>)> = vec![
            (
                Family::ProgrammeIndex,
                existing
                    .index
                    .into_normal_handler()
                    .map_err(|_| "Selected Index lifecycle unavailable")?,
            ),
            (
                Family::Refresh,
                existing
                    .refresh
                    .into_normal_handler()
                    .map_err(|_| "Selected Refresh lifecycle unavailable")?,
            ),
            (
                Family::Series,
                existing
                    .series
                    .into_normal_handler()
                    .map_err(|_| "Selected Series lifecycle unavailable")?,
            ),
            (
                Family::Artwork,
                existing
                    .artwork
                    .into_normal_handler()
                    .map_err(|_| "Selected artwork lifecycle unavailable")?,
            ),
            (
                Family::TvgProgrammes,
                existing
                    .tvg
                    .into_normal_handler()
                    .map_err(|_| "Selected TVG lifecycle unavailable")?,
            ),
            (
                Family::VersionCheck,
                existing
                    .version
                    .into_normal_handler()
                    .map_err(|_| "Selected version lifecycle unavailable")?,
            ),
            (Family::BackupCreate, Arc::new(backup_jobs.clone())),
            (Family::BackupScheduled, Arc::new(backup_jobs.clone())),
            (Family::ProxyStats, stats),
            (Family::Recording, dvr.clone()),
            (Family::StartupRecovery, Arc::new(recovery.clone())),
        ];
        entries.extend(core_tasks.entries());
        entries.extend(channel_tasks.entries());
        entries.push(plugin_refresh.entry());
        entries.extend(xc.entries());
        entries.extend(defaults.entries());
        entries.extend(scanner.entries());
        let queue = jobs::normal_queue::Application::prepare(
            &broker,
            &backend,
            entries,
            Duration::from_millis(100),
        )
        .await
        .map_err(|_| "Selected complete registry initialization failed")?;
        let scheduler = jobs::normal_beat::Scheduler::prepare(
            store.pool().clone(),
            shared_jobs.clone(),
            beat_identity,
            Duration::from_millis(100),
        )
        .map_err(|_| "Selected normal Beat initialization failed")?;
        if !std::env::var("DISPATCHARR_SKIP_PLUGIN_AUTODISCOVERY")
            .ok()
            .is_some_and(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            && scheduler
                .initialize_plugin_schedule(Arc::new(()))
                .await
                .is_err()
        {
            eprintln!("Could not set up plugin repository refresh schedule");
        }
        if beat_enabled {
            let setup = scheduler
                .initialize(Arc::new(()))
                .await
                .map_err(|_| "Selected normal Beat setup failed")?;
            if !setup.failed.is_empty() {
                eprintln!("Normal Beat default entries rejected: {:?}", setup.failed);
            }
        }
        Ok(Self {
            inner: Arc::new(Inner {
                plugin_refresh,
                queue,
                scanner,
                shared_jobs,
                publisher,
                dvr,
                recovery,
                scheduler,
                control,
                startup: watch::channel(Startup::Prepared).0,
                startup_slots: Arc::new(Semaphore::new(1)),
                started: AtomicBool::new(false),
                closed: AtomicBool::new(false),
                local,
                recording_http,
                xc_http,
                recording_playback,
                beat_enabled,
                backups,
                backup_jobs,
                channel_http,
            }),
        })
    }
    pub fn backup_files(&self) -> dispatcharr_store::BackupFiles {
        self.inner.backups.clone()
    }
    pub fn recordings_router(&self) -> axum::Router {
        self.inner
            .recording_http
            .clone()
            .router_with_playback(self.inner.recording_playback.clone())
            .merge(self.inner.channel_http.clone().router())
            .merge(self.inner.xc_http.clone().router())
    }
    pub fn readiness(&self) -> (bool, serde_json::Value) {
        let queue = self.inner.queue.readiness();
        let startup = self.inner.startup.borrow().clone();
        let control = self.inner.control.state();
        let scheduler = self.inner.scheduler.state();
        let startup_error = match &startup {
            Startup::Fault(reason) => Some(*reason),
            _ => None,
        };
        let ready = !self.inner.closed.load(Ordering::Acquire)
            && matches!(startup, Startup::Started)
            && control == jobs::normal_control::State::Subscribed
            && queue.ready
            && (!self.inner.beat_enabled
                || matches!(
                    scheduler,
                    jobs::normal_beat::Status::Running | jobs::normal_beat::Status::Observed(_)
                ));
        (
            ready,
            serde_json::json!({"scope":"experimental selected normal families plus startup recovery; not Source47 parity","ready":ready,"startup":format!("{startup:?}"),"startup_error":startup_error,"control":format!("{control:?}"),"celery":format!("{:?}",queue.celery),"dvr":format!("{:?}",queue.dvr),"clocked_scheduler":format!("{scheduler:?}"),"clocked_scheduler_enabled":self.inner.beat_enabled,"parity_complete":false}),
        )
    }
    pub fn start_after_serving(&self) -> Result<(), String> {
        if self.inner.closed.load(Ordering::Acquire)
            || self.inner.started.swap(true, Ordering::AcqRel)
        {
            return Err("Selected workers already started or closed".into());
        }
        let slot = self
            .inner
            .startup_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| "Selected startup admission unavailable")?;
        let controller = self.clone();
        tokio::spawn(async move {
            let owner: Arc<dyn Send + Sync> = Arc::new(slot);
            if let Err(reason) = controller.start_owned(owner).await {
                controller
                    .inner
                    .startup
                    .send_replace(Startup::Fault(reason));
                eprintln!("Selected worker startup failed: {reason}");
            }
        });
        Ok(())
    }
    async fn start_owned(&self, owner: Arc<dyn Send + Sync>) -> Result<(), &'static str> {
        self.inner.startup.send_replace(Startup::WaitingForHttp);
        // Proves that Axum actually processed an HTTP request after its server
        // future began polling; bind/listen or a fabricated Ready flag is insufficient.
        let local = self.inner.local;
        let closed = self.inner.clone();
        let probe_owner = owner.clone();
        let serving = tokio::task::spawn_blocking(move || {
            let _owner = probe_owner;
            probe(local, &closed.closed)
        })
        .await
        .map_err(|_| "Main serving probe supervisor failed")?;
        if !serving {
            return Err("Main did not serve the bounded local HTTP readiness probe");
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Err("Selected startup closed");
        }
        self.inner.startup.send_replace(Startup::Subscribing);
        self.inner
            .control
            .start(Arc::new(()))
            .map_err(|_| "Pidbox reader start failed")?;
        if !self.inner.control.wait_ready(Duration::from_secs(5)).await {
            return Err("Pidbox subscription was not physically confirmed");
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Err("Selected startup closed");
        }
        self.inner
            .queue
            .start(Arc::new(()))
            .map_err(|_| "Selected worker graph start failed")?;
        self.inner.startup.send_replace(Startup::WorkersRunning);
        self.inner.startup.send_replace(Startup::PublishingRecovery);
        self.inner
            .recovery
            .publish_startup(owner.clone())
            .await
            .map_err(|_| "Recovery/version root publication did not settle")?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err("Selected startup closed");
        }
        // Source PluginsConfig catches startup publication failure and continues.
        if self
            .inner
            .plugin_refresh
            .publish_startup(owner.clone())
            .await
            .is_err()
        {
            eprintln!("Could not enqueue startup plugin repository refresh");
        }
        if self.inner.beat_enabled {
            self.inner
                .scheduler
                .start(Arc::new(()))
                .map_err(|_| "Selected normal Beat start failed")?;
        }
        self.inner.startup.send_replace(Startup::Started);
        Ok(())
    }
    pub fn stop_producers(&self) {
        self.inner.publisher.stop_booking_backpressure();
        self.inner.backup_jobs.close_admission();
        self.inner.channel_http.close_admission();
        self.inner.recording_http.close_admission();
        self.inner.xc_http.close_admission();
        self.inner.recording_playback.close_admission();
        self.inner.closed.store(true, Ordering::Release);
        self.inner.startup.send_replace(Startup::Stopping);
        self.inner.scheduler.stop_admission();
        self.inner.recovery.stop_admission();
        self.inner.dvr.request_shutdown();
        self.inner.queue.stop_admission();
    }
    pub async fn shutdown(&self, budget: Duration) -> bool {
        self.stop_producers();
        let deadline = tokio::time::Instant::now() + budget;
        let startup =
            tokio::time::timeout_at(deadline, self.inner.startup_slots.clone().acquire_owned())
                .await;
        let Ok(Ok(_startup)) = startup else {
            return false;
        };
        if !self
            .inner
            .channel_http
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            return false;
        }
        if !self
            .inner
            .recording_http
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            return false;
        }
        if !self
            .inner
            .xc_http
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            return false;
        }
        if !self
            .inner
            .recording_playback
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            eprintln!("Recording playback drain remains physically unsettled");
            return false;
        }
        if self
            .inner
            .scheduler
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .is_err()
        {
            return false;
        }
        let drained = self
            .inner
            .queue
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await;
        if !drained.complete() {
            return false;
        }
        if self
            .inner
            .scanner
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .is_err()
        {
            return false;
        }
        if self
            .inner
            .publisher
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .is_err()
        {
            return false;
        }
        if !self
            .inner
            .control
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            return false;
        }
        if self
            .inner
            .shared_jobs
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .is_err()
        {
            return false;
        }
        self.inner.startup.send_replace(Startup::Stopped);
        true
    }
}
fn probe(local: SocketAddr, closed: &AtomicBool) -> bool {
    use std::io::{Read, Write};
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !closed.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
        if let Ok(mut stream) =
            std::net::TcpStream::connect_timeout(&local, Duration::from_millis(500))
        {
            let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
            let _ = stream.set_write_timeout(Some(Duration::from_millis(500)));
            if stream
                .write_all(
                    format!(
                        "GET /health/ready HTTP/1.1\r\nHost: {local}\r\nConnection: close\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .is_ok()
            {
                let mut bytes = Vec::with_capacity(4096);
                let mut chunk = [0u8; 1024];
                while bytes.len() < 16384 {
                    match stream.read(&mut chunk) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            bytes.extend_from_slice(&chunk[..n]);
                            if bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                                return bytes.starts_with(b"HTTP/1.1 503 ")
                                    || bytes.starts_with(b"HTTP/1.1 200 ");
                            }
                        }
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}
