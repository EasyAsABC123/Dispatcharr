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
    plugin_runtime: dispatcharr_store::PluginRuntime,
    plugin_jobs: crate::plugin_jobs_bridge::PluginJobsBridge,
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
    compact: Arc<jobs::compact_coordination::CompactCoordinator>,
    recording_playback: dispatcharr_store::RecordingPlayback,
    beat: Beat,
    #[cfg(target_os = "linux")]
    taskless_shutdown: Option<Duration>,
    backups: dispatcharr_store::BackupFiles,
    backup_jobs: jobs::normal_backup::Application,
    channel_http: dispatcharr_store::channel_maintenance_http::ChannelMaintenanceHttp,
    matching_http: dispatcharr_store::channel_epg_match_http::ChannelEpgMatchHttp,
    comskip_http: dispatcharr_store::comskip_http::ComskipHttp,
    sd_source_http: dispatcharr_store::sd_source_http::SdSourceHttp,
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Beat {
    Off,
    Legacy,
    Core,
}

impl Beat {
    pub fn validate_owners(self, standalone_core: bool, taskless: bool) -> Result<(), String> {
        if self == Self::Core && (standalone_core || taskless) {
            return Err(
                "Selected Core Beat requires an exclusive scheduler and no taskless restore".into(),
            );
        }
        Ok(())
    }

    fn enabled(self) -> bool {
        self != Self::Off
    }
}

fn select_beat(worker: bool, legacy: bool, core: bool) -> Result<Beat, String> {
    if core {
        if !worker || legacy {
            return Err(
                "Selected Core Beat requires Selected workers and legacy Selected Beat disabled"
                    .into(),
            );
        }
        return Ok(Beat::Core);
    }
    if legacy && !worker {
        return Err(
            "Selected Beat requires selected workers; configure exactly one Beat instance".into(),
        );
    }
    Ok(if legacy { Beat::Legacy } else { Beat::Off })
}

const WORKERS: &str = "RUST_EXPERIMENTAL_SELECTED_JOBS";
const LEGACY_BEAT: &str = "RUST_EXPERIMENTAL_SELECTED_BEAT";
const CORE_BEAT: &str = "RUST_EXPERIMENTAL_SELECTED_CORE_BEAT";

pub fn main_configuration() -> Result<(bool, Beat), String> {
    // The deployment must actually assign AIO ownership. The descriptive
    // fallback used by configuration reporting does not assign a queue role.
    if env::var("DISPATCHARR_ENV").as_deref() == Ok("aio")
        && ![
            WORKERS,
            LEGACY_BEAT,
            CORE_BEAT,
            "RUST_CORE_SCHEDULE_OWNER",
            "RUST_BACKUP_TASKLESS_RESTORE_OWNER",
            "RUST_BACKUP_CREATE_OWNER",
            "RUST_BACKUP_RESTORE_OWNER",
            "RUST_VERSION_STARTUP_OWNER",
            "RUST_VERSION_TASKS_ENABLED",
            "RUST_EPG_REFRESH_ENABLED",
            "RUST_M3U_REFRESH_ALL_OWNER",
        ]
        .iter()
        .any(|name| env::var_os(name).is_some())
    {
        return Ok((true, Beat::Core));
    }
    configuration()
}

pub fn configuration() -> Result<(bool, Beat), String> {
    let worker = flag(WORKERS)?;
    let legacy = flag(LEGACY_BEAT)?;
    let value = env::var_os(CORE_BEAT);
    let core = match value.as_ref().map(|value| value.to_str()) {
        None | Some(Some("0")) => false,
        Some(Some("1")) => true,
        _ => return Err(format!("{CORE_BEAT} must be 0 or 1")),
    };
    Ok((worker, select_beat(worker, legacy, core)?))
}
fn flag(name: &str) -> Result<bool, String> {
    match env::var(name).as_deref() {
        Ok("1") => Ok(true),
        Ok("0") | Err(_) => Ok(false),
        _ => Err(format!("{name} must be 0 or 1")),
    }
}
fn scheduler_ready(enabled: bool, status: &jobs::normal_beat::Status) -> bool {
    // Running is announced when the scheduler is spawned. Observed means its
    // first tick returned after the SQL and publication work settled.
    !enabled || matches!(status, jobs::normal_beat::Status::Observed(_))
}

async fn after_core_receivers(
    beat: Beat,
    limit: Duration,
    closed: &AtomicBool,
    mut ready: impl FnMut() -> Result<bool, &'static str>,
    action: impl std::future::Future<Output = Result<(), &'static str>>,
) -> Result<(), &'static str> {
    if beat == Beat::Core {
        tokio::time::timeout(limit, async {
            loop {
                if closed.load(Ordering::Acquire) {
                    return Err("Selected startup closed");
                }
                if ready()? {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .map_err(|_| "Selected Core Beat receiver did not become ready")??;
        if closed.load(Ordering::Acquire) {
            return Err("Selected startup closed");
        }
    }
    action.await
}

fn core_receivers_ready(readiness: jobs::normal_queue::Readiness) -> Result<bool, &'static str> {
    use jobs::normal_queue::State;
    if [&readiness.celery, &readiness.dvr]
        .into_iter()
        .any(|lane| matches!(lane, State::Fault(_) | State::Stopped))
    {
        return Err("Selected Core receiver stopped or could not admit work");
    }
    Ok(readiness.ready)
}

async fn start_beat(
    beat: Beat,
    limit: Duration,
    closed: &AtomicBool,
    ready: impl FnMut() -> Result<bool, &'static str>,
    start: impl FnOnce() -> Result<(), jobs::normal_beat::Error>,
) -> Result<(), &'static str> {
    if beat == Beat::Off {
        return Ok(());
    }
    after_core_receivers(beat, limit, closed, ready, async {
        start().map_err(|_| "Selected normal Beat start failed")
    })
    .await
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
        scheduled_vod: dispatcharr_store::ScheduledVod,
        restore_plugins: dispatcharr_store::PluginActions,
        #[cfg(target_os = "linux")] taskless_helper: Option<
            jobs::normal_backup::VerifiedGuardedHelper,
        >,
        cache: &str,
        beat: Beat,
    ) -> Result<Self, String> {
        let local = loopback(actual)?;
        #[cfg(target_os = "linux")]
        beat.validate_owners(false, taskless_helper.is_some())?;
        let beat_identity = auth.producer_identity_source();
        let cache = cache.to_owned();
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
        let shared_jobs = jobs::transport::Jobs::connect_defaults(
            &broker,
            &backend,
            jobs::transport::Config::default(),
        )
        .await
        .map_err(|_| "Selected scanner transport initialization failed")?;
        let comskip = jobs::normal_comskip::Prepared::new(
            shared_jobs.clone(),
            auth.clone(),
            events.clone(),
            auth.producer_identity_source(),
            revoked.clone(),
        )
        .map_err(|_| "Selected manual Comskip authority unavailable")?;
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
                    comskip: comskip.clone(),
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
        if env::var("DISPATCHARR_ENV").as_deref() == Ok("aio") {
            dvr.enable_local_stop_handoff();
        }
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
        let plugin_jobs =
            crate::plugin_jobs_bridge::PluginJobsBridge::prepare(shared_jobs.clone(), Arc::new(()))
                .map_err(|_| "Plugin shared Jobs bridge preparation failed")?;
        let plugin_runtime = dispatcharr_store::PluginRuntime::prepare(
            auth.clone(),
            PathBuf::from("/usr/local/bin/dispatcharr-plugin-python"),
            PathBuf::from("/opt/dispatcharr-python/application"),
            env::var_os("DISPATCHARR_PLUGINS_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/data/plugins")),
            events.clone(),
            Arc::new(plugin_jobs.clone()),
            Arc::new(()),
            1024,
        )
        .map_err(|_| "Plugin lazy runtime preparation failed")?;
        dvr.bind_recording_connect(auth.clone(), plugin_runtime.clone())
            .map_err(|_| "Selected recording Connect runtime binding failed")?;
        let plugin_tasks = Arc::new(
            jobs::normal_registered_plugins::Prepared::prepare(
                shared_jobs.clone(),
                plugin_runtime.clone(),
                revoked.clone(),
            )
            .map_err(|_| "Plugin selected-family preparation failed")?,
        );
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
            auth.clone(),
            cache.clone(),
            backup_database,
            backup_runtime,
            backup_root,
            backup_scratch,
            auth.producer_identity_source(),
        )
        .map_err(|_| "Normal backup preparation unavailable")?;
        backup_jobs
            .bind_restore_plugins(restore_plugins)
            .map_err(|_| "Backup restore plugin reconciliation binding failed")?;
        if beat != Beat::Core && backup_jobs.initialize_schedule(Arc::new(())).await.is_err() {
            eprintln!("Normal backup scheduler startup sync failed");
        }
        let backup_handler = Arc::new(backup_jobs.clone());
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
        let xc_port = Arc::new(xc.clone());
        let compact = Arc::new(jobs::compact_coordination::CompactCoordinator::shared(
            locks.clone(),
        ));
        auth.bind_compact_coordinator(compact.clone())
            .map_err(|_| "Selected compact coordinator binding failed")?;
        let xc_http = dispatcharr_store::xc_account_http::Http::prepare(auth.clone(), xc_port);
        xc_http
            .bind_vod_settings(&auth)
            .map_err(|_| "Selected XC VOD settings producer binding failed")?;
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
            dvr.clone(),
        );
        auth.bind_recording_jobs(recording_jobs.clone())
            .map_err(|_| "Selected recording HTTP Collector binding failed")?;
        let recording_http = dispatcharr_store::recording_http::RecordingHttp::prepare(
            auth.clone(),
            recording_jobs.clone(),
            events.clone(),
        );
        let core_tasks = jobs::normal_core_tasks::Prepared::new(
            shared_jobs.clone(),
            store.clone(),
            auth.clone(),
            recording_jobs.clone(),
            recording_http.clone(),
            scheduled_vod,
            events.clone(),
            revoked.clone(),
        )
        .map_err(|_| "Selected Core downstream task initialization failed")?;
        let plugin_refresh = jobs::normal_plugin_repos::Prepared::with_plugin_publications(
            shared_jobs.clone(),
            auth.clone(),
            revoked.clone(),
            plugin_runtime.publication_owners(),
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
        let matching_tasks = jobs::normal_channel_matching::Prepared::new(
            shared_jobs.clone(),
            auth.clone(),
            events.clone(),
            auth.producer_identity_source(),
            revoked.clone(),
        )
        .map_err(|_| "Selected channel matching authority unavailable")?;
        let matching_http = dispatcharr_store::channel_epg_match_http::ChannelEpgMatchHttp::prepare(
            auth.clone(),
            Arc::new(jobs::normal_channel_matching::Publisher::new(
                shared_jobs.clone(),
                auth.producer_identity_source(),
            )),
        );
        let comskip_http =
            dispatcharr_store::comskip_http::ComskipHttp::prepare(Arc::new(comskip.clone()));
        let xml_cache = env::var("RUST_EPG_CACHE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| media_root.join("cached_epg"));
        let xml_download = dispatcharr_store::xmltv_download::Downloader::prepare(
            auth.clone(),
            events.clone(),
            xml_cache,
        )
        .map_err(|_| "Selected XMLTV download preparation failed")?;
        let sd_guide = jobs::normal_sd_guide::Prepared::new(
            shared_jobs.clone(),
            auth.clone(),
            events.clone(),
            locks.clone(),
            revoked.clone(),
        )
        .map_err(|_| "Selected SD guide initialization failed")?;
        sd_guide
            .bind_hook_runtime(plugin_runtime.clone())
            .map_err(|_| "Selected SD hook runtime binding failed")?;
        let sd_source_http = sd_guide.source_http(existing.refresh.clone());
        let scanner = jobs::normal_scanner::Prepared::connect_with_auto(
            shared_jobs.clone(),
            store.clone(),
            auth.clone(),
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
            recording_jobs,
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
        let (stats, inline_stats) = jobs::normal_queue::prepare_stats_with_inline(
            &broker,
            &backend,
            crate::statistics::task_source(engine),
            events,
            crate::positive("RUST_SELECTED_STATS_BYTES", 3 * 1024 * 1024)?,
        )
        .await
        .map_err(|_| "Selected live statistics initialization failed")?;
        scanner
            .bind_compat_stats(inline_stats)
            .map_err(|_| "Selected compatibility scanner Stats initialization failed")?;
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
                    .into_normal_handler_with_hooks(xml_download.clone(), plugin_runtime.clone())
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
                sd_guide.wrap_tvg(
                    existing
                        .tvg
                        .into_normal_handler_with_deferred_download(
                            shared_jobs.clone(),
                            auth.producer_identity_source(),
                            revoked.clone(),
                            xml_download.clone(),
                            dispatcharr_store::tvg_programme_stage::Service::prepare(auth.clone()),
                        )
                        .map_err(|_| "Selected TVG lifecycle unavailable")?,
                ),
            ),
            (
                Family::VersionCheck,
                existing
                    .version
                    .into_normal_handler()
                    .map_err(|_| "Selected version lifecycle unavailable")?,
            ),
            (Family::BackupCreate, backup_handler.clone()),
            (Family::BackupScheduled, backup_handler.clone()),
            (Family::ProxyStats, stats.clone()),
            (Family::Recording, dvr.clone()),
            (Family::StartupRecovery, Arc::new(recovery.clone())),
        ];
        entries.extend(core_tasks.entries());
        entries.extend(channel_tasks.entries());
        entries.extend([
            (
                Family::ChannelMatchAll,
                matching_tasks.handler(jobs::normal_channel_matching::Kind::All),
            ),
            (
                Family::ChannelMatchSelected,
                matching_tasks.handler(jobs::normal_channel_matching::Kind::Selected),
            ),
            (
                Family::ChannelMatchSingle,
                matching_tasks.handler(jobs::normal_channel_matching::Kind::Single),
            ),
        ]);
        entries.push((Family::ComskipManual, comskip.handler()));
        entries.push(plugin_refresh.entry());
        entries.push((Family::RegisteredPlugin, plugin_tasks.clone()));
        entries.extend(xc.entries());
        entries.push(sd_guide.entry());
        entries.extend(defaults.entries());
        entries.extend(scanner.entries());
        let queue = jobs::normal_queue::Application::prepare_with_registered_plugins(
            &broker,
            &backend,
            entries,
            Duration::from_millis(100),
        )
        .await
        .map_err(|_| "Selected complete registry initialization failed")?;
        plugin_jobs
            .attach_prepared(&queue)
            .map_err(|_| "Plugin actual shared worker registry unavailable")?;
        #[cfg(target_os = "linux")]
        let taskless_shutdown = taskless_helper
            .as_ref()
            .map(|helper| helper.timeout() + Duration::from_secs(4));
        #[cfg(target_os = "linux")]
        if let Some(helper) = taskless_helper {
            if beat.enabled() {
                return Err(
                    "Taskless restore requires the independent Selected scheduler disabled".into(),
                );
            }
            let authority = queue
                .taskless_builtin_authority(&plugin_tasks, &backup_handler)
                .map_err(|_| "Selected taskless builtin authority unavailable")?;
            backup_jobs
                .bind_taskless_restore(authority, helper)
                .map_err(|_| "Selected taskless restore authority binding failed")?;
        }
        // Discover actual enabled callables using the same Auth4/pool8 ports.
        // No Python child exists when there are no enabled plugin configurations.
        plugin_runtime
            .discover_configured(Arc::new(()))
            .await
            .map_err(|_| "Plugin actual startup discovery/SQL settlement failed")?;
        scanner
            .bind_sd(
                sd_guide
                    .capability(&queue)
                    .map_err(|_| "Selected SD guide actual registry unavailable")?,
            )
            .map_err(|_| "Selected SD guide auto composition failed")?;
        matching_tasks
            .bind_sd(
                sd_guide
                    .capability(&queue)
                    .map_err(|_| "Selected matching SD registry unavailable")?,
            )
            .map_err(|_| "Selected matching SD composition failed")?;
        let scheduler = match beat {
            Beat::Core => jobs::normal_beat::Scheduler::prepare_core_schedules_for_selected(
                queue
                    .core_schedule_receiver(&backup_handler, &stats)
                    .map_err(|_| "Selected Core Beat receiver configuration failed")?
                    .with_saved_xc(&scanner)
                    .map_err(|_| "Selected Core Beat XC and VOD receiver binding failed")?,
                Duration::from_secs(5),
            ),
            Beat::Off | Beat::Legacy => jobs::normal_beat::Scheduler::prepare(
                store.pool().clone(),
                shared_jobs.clone(),
                beat_identity,
                Duration::from_secs(5),
            ),
        }
        .map_err(|_| "Selected normal Beat initialization failed")?;
        if beat == Beat::Core {
            backup_jobs
                .initialize_core_schedule(Arc::new(()))
                .await
                .map_err(|_| "Selected Core backup schedule initialization failed")?;
        }
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
        if beat.enabled() {
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
                plugin_runtime,
                plugin_jobs,
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
                compact,
                recording_playback,
                beat,
                #[cfg(target_os = "linux")]
                taskless_shutdown,
                backups,
                backup_jobs,
                channel_http,
                matching_http,
                comskip_http,
                sd_source_http,
            }),
        })
    }
    pub fn plugin_runtime(&self) -> dispatcharr_store::PluginRuntime {
        self.inner.plugin_runtime.clone()
    }
    pub fn backup_files(&self) -> dispatcharr_store::BackupFiles {
        self.inner.backups.clone()
    }
    pub fn sd_source_http(&self) -> dispatcharr_store::sd_source_http::SdSourceHttp {
        self.inner.sd_source_http.clone()
    }
    pub fn recordings_router(&self) -> axum::Router {
        self.inner
            .recording_http
            .clone()
            .router_with_playback_and_comskip(
                self.inner.recording_playback.clone(),
                self.inner.comskip_http.clone(),
            )
            .merge(self.inner.channel_http.clone().router())
            .merge(self.inner.matching_http.clone().router())
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
            && self.inner.plugin_runtime.accepting()
            && matches!(startup, Startup::Started)
            && control == jobs::normal_control::State::Subscribed
            && queue.ready
            && scheduler_ready(self.inner.beat.enabled(), &scheduler);
        let mut report = serde_json::json!({"scope":"experimental selected normal families plus startup recovery; not Source47 parity","ready":ready,"startup":format!("{startup:?}"),"startup_error":startup_error,"control":format!("{control:?}"),"celery":format!("{:?}",queue.celery),"dvr":format!("{:?}",queue.dvr),"clocked_scheduler":format!("{scheduler:?}"),"clocked_scheduler_enabled":self.inner.beat.enabled(),"dvr_plugin_domain":self.inner.dvr.plugin_domain_capability(),"startup_recovery_deferred_unsupported_domain":self.inner.recovery.startup_recovery_deferred(),"parity_complete":false});
        if self.inner.beat == Beat::Core {
            report["clocked_scheduler_scope"] = serde_json::json!("core-saved-families");
        }
        (ready, report)
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
        after_core_receivers(
            self.inner.beat,
            Duration::from_secs(5),
            &self.inner.closed,
            || core_receivers_ready(self.inner.queue.readiness()),
            async {
                self.inner.startup.send_replace(Startup::WorkersRunning);
                self.inner.startup.send_replace(Startup::PublishingRecovery);
                self.inner
                    .recovery
                    .publish_startup(owner.clone())
                    .await
                    .map(|_| ())
                    .map_err(|_| "Recovery/version root publication did not settle")
            },
        )
        .await?;
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
        start_beat(
            self.inner.beat,
            Duration::from_secs(5),
            &self.inner.closed,
            || core_receivers_ready(self.inner.queue.readiness()),
            || self.inner.scheduler.start(Arc::new(())),
        )
        .await?;
        self.inner.startup.send_replace(Startup::Started);
        Ok(())
    }
    pub fn stop_producers(&self) {
        if self.inner.beat == Beat::Core {
            self.inner.scheduler.stop_admission();
        }
        self.inner.publisher.stop_booking_backpressure();
        self.inner.backup_jobs.close_admission();
        self.inner.channel_http.close_admission();
        self.inner.matching_http.close_admission();
        self.inner.comskip_http.close_admission();
        self.inner.sd_source_http.close_admission();
        self.inner.recording_http.close_admission();
        self.inner.xc_http.close_admission();
        self.inner.compact.close_admission();
        self.inner.recording_playback.close_admission();
        self.inner.closed.store(true, Ordering::Release);
        self.inner.startup.send_replace(Startup::Stopping);
        if self.inner.beat != Beat::Core {
            self.inner.scheduler.stop_admission();
        }
        self.inner.recovery.stop_admission();
        self.inner.dvr.request_shutdown();
        self.inner.queue.stop_admission();
    }
    pub fn shutdown_budget(&self) -> Duration {
        #[cfg(target_os = "linux")]
        if let Some(budget) = self.inner.taskless_shutdown {
            return budget;
        }
        Duration::from_secs(12)
    }

    pub async fn shutdown(&self, budget: Duration) -> bool {
        self.stop_producers();
        let deadline = tokio::time::Instant::now() + budget;
        let startup =
            tokio::time::timeout_at(deadline, self.inner.startup_slots.clone().acquire_owned())
                .await;
        let Ok(Ok(_startup)) = startup else {
            return selected_shutdown_failed("startup");
        };
        if !self
            .inner
            .channel_http
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            return selected_shutdown_failed("channel_http");
        }
        if !self
            .inner
            .matching_http
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            return selected_shutdown_failed("matching_http");
        }
        if !self
            .inner
            .comskip_http
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            return selected_shutdown_failed("comskip_http");
        }
        if !self
            .inner
            .sd_source_http
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            return selected_shutdown_failed("sd_source_http");
        }
        if !self
            .inner
            .recording_http
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            return selected_shutdown_failed("recording_http");
        }
        if !self
            .inner
            .xc_http
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            return selected_shutdown_failed("xc_http");
        }
        if !self
            .inner
            .compact
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            return selected_shutdown_failed("compact");
        }
        if !self
            .inner
            .recording_playback
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            eprintln!("Recording playback drain remains physically unsettled");
            return selected_shutdown_failed("recording_playback");
        }
        let scheduler_settled = self
            .inner
            .scheduler
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .is_ok();
        if !scheduler_settled {
            if selected_shutdown_diagnostics_enabled() {
                selected_shutdown_diagnostic("scheduler", None, "unsettled");
                let observed = self.inner.scheduler.shutdown_diagnostic();
                for (stage, outcome) in [
                    ("scheduler_timer", observed.timer),
                    ("scheduler_slot", observed.slot),
                    ("scheduler_cache", observed.cache),
                    ("scheduler_checkpoint", observed.checkpoint),
                    ("scheduler_state", observed.state),
                    ("scheduler_fault", observed.fault),
                ] {
                    selected_shutdown_diagnostic(stage, None, outcome);
                }
            }
            if self.inner.beat != Beat::Core {
                return false;
            }
        }
        let drained = self
            .inner
            .queue
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await;
        if !drained.complete() {
            for (stage, settled) in [
                ("queue_pollers", drained.pollers_settled),
                ("queue_admissions", drained.admissions_settled),
                ("queue_heads", drained.heads_settled),
            ] {
                selected_shutdown_diagnostic(
                    stage,
                    None,
                    if settled { "settled" } else { "unsettled" },
                );
            }
            for (family, result) in &drained.families {
                if let Err(error) = result {
                    selected_shutdown_diagnostic(
                        "queue_family",
                        Some(*family),
                        shutdown_disposition_kind(error),
                    );
                }
            }
            return selected_shutdown_failed("queue");
        }
        if self
            .inner
            .scanner
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .is_err()
        {
            return selected_shutdown_failed("scanner");
        }
        if self
            .inner
            .publisher
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .is_err()
        {
            return selected_shutdown_failed("recording_publisher");
        }
        if !self
            .inner
            .control
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            return selected_shutdown_failed("control");
        }
        if self
            .inner
            .plugin_jobs
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .is_err()
        {
            return selected_shutdown_failed("plugin_jobs");
        }
        if self
            .inner
            .shared_jobs
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
            .is_err()
        {
            return selected_shutdown_failed("shared_jobs");
        }
        if scheduler_settled {
            self.inner.startup.send_replace(Startup::Stopped);
        }
        scheduler_settled
    }
}
// Opt-in, fixed-vocabulary shutdown diagnostics. Never format runtime errors,
// task IDs, arbitrary task names, provider URLs, request bodies or credentials.
// At most64 records for the whole process, including repeated shutdown callers.
fn selected_shutdown_diagnostics_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| env::var("RUST_SELECTED_SHUTDOWN_DIAGNOSTICS").is_ok_and(|v| v == "1"))
}
fn selected_shutdown_diagnostic(
    stage: &'static str,
    family: Option<Family>,
    outcome: &'static str,
) {
    use std::sync::atomic::AtomicUsize;
    static RECORDS: AtomicUsize = AtomicUsize::new(0);
    if !selected_shutdown_diagnostics_enabled() {
        return;
    }
    if RECORDS
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
            (n < 64).then_some(n + 1)
        })
        .is_ok()
    {
        eprintln!("selected_shutdown stage={stage} family={family:?} outcome={outcome}");
    }
}
fn selected_shutdown_failed(stage: &'static str) -> bool {
    selected_shutdown_diagnostic(stage, None, "unsettled");
    false
}
fn shutdown_disposition_kind(value: &jobs::milestone_dispatch::Disposition) -> &'static str {
    use jobs::milestone_dispatch::Disposition as D;
    match value {
        D::Empty => "Empty",
        D::NotDue => "NotDue",
        D::Capacity => "Capacity",
        D::Reserved(_) => "Reserved",
        D::Restored(_) => "Restored",
        D::HeadChanged => "HeadChanged",
        D::Completed(_) => "Completed",
        D::CompletedNamed(_) => "CompletedNamed",
        D::CompletedWithoutId => "CompletedWithoutId",
        D::TaskFailedWithoutId => "TaskFailedWithoutId",
        D::TaskFailed(_) => "TaskFailed",
        D::TaskFailedNamed(_) => "TaskFailedNamed",
        D::Revoked(_) => "Revoked",
        D::RevokedNamed(_) => "RevokedNamed",
        D::UnsupportedHead => "UnsupportedHead",
        D::UnsupportedDomain => "UnsupportedDomain",
        D::Operational(_) => "Operational",
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

#[cfg(test)]
mod readiness_tests {
    use super::*;

    #[test]
    fn selected_core_beat_requires_exclusive_explicit_configuration() {
        assert_eq!(select_beat(false, false, false).unwrap(), Beat::Off);
        assert_eq!(select_beat(true, false, false).unwrap(), Beat::Off);
        assert_eq!(select_beat(true, true, false).unwrap(), Beat::Legacy);
        assert_eq!(select_beat(true, false, true).unwrap(), Beat::Core);
        assert!(select_beat(false, true, false).is_err());
        assert!(select_beat(false, false, true).is_err());
        assert!(select_beat(true, true, true).is_err());
        assert!(Beat::Core.validate_owners(false, false).is_ok());
        assert!(Beat::Core.validate_owners(true, false).is_err());
        assert!(Beat::Core.validate_owners(false, true).is_err());
        assert!(Beat::Off.validate_owners(false, true).is_ok());
    }

    #[tokio::test]
    async fn selected_core_beat_starts_once_after_queue_readiness() {
        let observations = std::cell::Cell::new(0);
        let started = std::cell::Cell::new(0);
        let closed = AtomicBool::new(false);
        let ready = || {
            observations.set(observations.get() + 1);
            Ok(observations.get() >= 2)
        };
        let start = || {
            assert!(observations.get() >= 2);
            started.set(started.get() + 1);
            Ok(())
        };
        start_beat(Beat::Core, Duration::from_secs(1), &closed, ready, start)
            .await
            .unwrap();
        assert_eq!(started.get(), 1);
        assert_eq!(
            start_beat(
                Beat::Core,
                Duration::ZERO,
                &closed,
                || Ok(false),
                || panic!("started without the receiver"),
            )
            .await,
            Err("Selected Core Beat receiver did not become ready")
        );
        closed.store(true, Ordering::Release);
        assert_eq!(
            start_beat(
                Beat::Core,
                Duration::from_secs(1),
                &closed,
                || panic!("inspected the receiver after closure"),
                || panic!("started after closure"),
            )
            .await,
            Err("Selected startup closed")
        );
    }

    #[tokio::test]
    async fn selected_core_startup_dispatch_waits_for_both_receivers() {
        use jobs::normal_queue::State;

        let closed = AtomicBool::new(false);
        let polls = std::cell::Cell::new(0);
        let readiness = |dvr| {
            let ready = matches!(dvr, State::Working(_));
            jobs::normal_queue::Readiness {
                scope: "configured receivers",
                celery: State::Working(0),
                dvr,
                required_families: 1,
                prepared_families: 1,
                scheduler_enabled: true,
                ready,
            }
        };
        after_core_receivers(
            Beat::Core,
            Duration::from_secs(1),
            &closed,
            || {
                polls.set(polls.get() + 1);
                core_receivers_ready(readiness(if polls.get() == 1 {
                    State::Blocked(jobs::milestone_dispatch::Disposition::UnsupportedHead)
                } else {
                    State::Working(0)
                }))
            },
            async {
                assert_eq!(polls.get(), 2);
                Ok(())
            },
        )
        .await
        .unwrap();

        for (limit, dvr, expected) in [
            (
                Duration::ZERO,
                State::Starting,
                "Selected Core Beat receiver did not become ready",
            ),
            (
                Duration::from_secs(1),
                State::Stopped,
                "Selected Core receiver stopped or could not admit work",
            ),
            (
                Duration::from_secs(1),
                State::Fault(jobs::milestone_dispatch::Disposition::Operational(
                    "test".into(),
                )),
                "Selected Core receiver stopped or could not admit work",
            ),
        ] {
            assert_eq!(
                after_core_receivers(
                    Beat::Core,
                    limit,
                    &closed,
                    || core_receivers_ready(readiness(dvr.clone())),
                    async { panic!("dispatched startup with an unavailable receiver") },
                )
                .await,
                Err(expected)
            );
        }

        for beat in [Beat::Off, Beat::Legacy] {
            after_core_receivers(
                beat,
                Duration::ZERO,
                &closed,
                || panic!("legacy or Beat-Off startup inspected scoped readiness"),
                async { Ok(()) },
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn selected_legacy_and_off_startup_keep_existing_behavior() {
        let closed = AtomicBool::new(false);
        start_beat(
            Beat::Off,
            Duration::ZERO,
            &closed,
            || panic!("disabled scheduler inspected queue readiness"),
            || panic!("disabled scheduler started"),
        )
        .await
        .unwrap();
        let started = std::cell::Cell::new(0);
        start_beat(
            Beat::Legacy,
            Duration::ZERO,
            &closed,
            || panic!("legacy scheduler waited for scoped readiness"),
            || {
                started.set(started.get() + 1);
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(started.get(), 1);
    }

    #[test]
    fn configured_beat_waits_for_a_settled_tick() {
        use jobs::normal_beat::{Error, Status, Tick};

        for status in [
            Status::Prepared,
            Status::Running,
            Status::Stopped,
            Status::Fault(Arc::new(Error::Drain)),
        ] {
            assert!(!scheduler_ready(true, &status));
            assert!(scheduler_ready(false, &status));
        }
        let observed = Status::Observed(Tick::default());
        assert!(scheduler_ready(true, &observed));
        assert!(scheduler_ready(false, &observed));
    }
}
