//! Derivative Dispatcharr backend port. Original project credit: ATTRIBUTION.md.
mod admin_ingress;
mod backup_config;
mod combined_statistics;
mod combined_statistics_html;
mod combined_statistics_wire;
mod control;
mod core_scheduled_jobs;
mod database_config;
mod drf_api_roots;
mod m3u_refresh_all_config;
mod managed_logs;
mod manual_backup_config;
mod playback;
mod plugin_gpg_trace;
mod plugin_jobs_bridge;
mod proxy_health;
mod realtime;
mod recording_http_bridge;
mod redis_config;
mod response_headers;
mod selected_jobs;
#[cfg(feature = "sqlite-copied-data-runtime")]
mod sqlite_copied_runtime;
mod static_assets;
mod statistics;
mod vod_react_fallback;
use axum::{
    extract::State,
    http::{header, Method, StatusCode},
    middleware,
    response::{IntoResponse, Response},
    routing::{any, get},
    Json, Router,
};
use dispatcharr_clients::{ClientConfig, ClientStore};
use dispatcharr_coordination::{Coordinator, LeaseConfig};
use dispatcharr_input_store::{InputConfig, InputStore};
use dispatcharr_output_store::{OutputConfig, OutputStore};
use dispatcharr_store::{AuthConfig, AuthService, Store};
use dispatcharr_stream_http::{EngineConfig, StreamEngine};
use std::{env, future::IntoFuture, net::SocketAddr, sync::Arc, time::Duration};

fn positive(name: &str, default: usize) -> Result<usize, String> {
    let value = env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<usize>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if value == 0 {
        return Err(format!("{name} must be positive"));
    }
    Ok(value)
}

fn live_engine_config() -> Result<EngineConfig, String> {
    let mut config = EngineConfig::default();
    config.initial_buffer_chunks = 4;
    config.flush_partial_chunks = false;
    config.source_initialization = true;
    config.startup_timeout = Duration::from_secs(60);
    // Original HTTPStreamReader uses requests timeout=(5,30).
    config.connect_timeout = Duration::from_secs(5);
    config.read_timeout = Duration::from_secs(30);
    config.max_channels = positive("RUST_MAX_CHANNELS", config.max_channels)?;
    config.max_viewers_per_channel = positive(
        "RUST_MAX_VIEWERS_PER_CHANNEL",
        config.max_viewers_per_channel,
    )?;
    config.retained_bytes_per_channel = positive(
        "RUST_RETAINED_BYTES_PER_CHANNEL",
        config.retained_bytes_per_channel,
    )?;
    Ok(config)
}

fn live_engine_settings(
    mut config: EngineConfig,
    proxy_settings: &serde_json::Value,
) -> Result<EngineConfig, String> {
    for (name, default, target) in [
        (
            "stream_startup_timeout",
            8.0,
            &mut config.health_startup_timeout,
        ),
        (
            "connection_timeout",
            10.0,
            &mut config.health_connection_timeout,
        ),
        (
            "channel_init_grace_period",
            60.0,
            &mut config.health_initialization_grace,
        ),
    ] {
        let seconds = proxy_settings
            .get(name)
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(default);
        *target = Duration::try_from_secs_f64(seconds)
            .map_err(|_| format!("Invalid {name} health timeout"))?;
    }
    let configured_retention = proxy_settings
        .get("redis_chunk_ttl")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(60);
    config.retention = Duration::from_secs(positive(
        "RUST_RETENTION_SECONDS",
        usize::try_from(configured_retention).map_err(|_| "Retention overflow")?,
    )? as u64);
    config.chunk_bytes = positive("RUST_CHUNK_PACKETS", 1361)?
        .checked_mul(188)
        .ok_or("Chunk size overflow")?;
    Ok(config)
}

fn live_input_config(config: &EngineConfig) -> Result<InputConfig, String> {
    Ok(InputConfig {
        max_chunk_bytes: config.chunk_bytes,
        max_channel_bytes: config.retained_bytes_per_channel,
        max_chunks: positive("RUST_INPUT_MAX_CHUNKS", 4096)?,
        max_read_bytes: positive("RUST_INPUT_READ_BYTES", 64 * 1024 * 1024)?,
        max_operations: positive("RUST_INPUT_OPERATIONS", 16)?,
        chunk_ttl: config.retention,
        preserve_source_era: config.source_initialization,
        ..Default::default()
    })
}

fn version_task_options() -> dispatcharr_job_transport::version_service::Options {
    let endpoint = env::var("RUST_VERSION_HTTP_ORIGIN")
        .map(|origin| format!("{}/owned-version", origin.trim_end_matches('/')))
        .unwrap_or_else(|_| {
            "https://api.github.com/repos/Dispatcharr/Dispatcharr/releases/latest".into()
        });
    // Bind the stable check to the pinned Original application version.
    dispatcharr_job_transport::version_service::Options {
        version: "0.33.0".into(),
        endpoint,
        ca_bundle: std::path::PathBuf::from(
            env::var("RUST_VERSION_CA_BUNDLE")
                .unwrap_or_else(|_| "/opt/dispatcharr-crypto/trust/cacert.pem".into()),
        ),
    }
}

fn version_startup_owned(
    owner: Option<&std::ffi::OsStr>,
    version: bool,
    selected: bool,
    refresh: bool,
    broker: Option<&std::ffi::OsStr>,
    backend: Option<&std::ffi::OsStr>,
) -> Result<bool, &'static str> {
    use std::ffi::OsStr;

    let Some(owner) = owner else {
        return Ok(false);
    };
    if owner != OsStr::new("rust") {
        return Err("RUST_VERSION_STARTUP_OWNER must be rust when configured");
    }
    if !version || selected || refresh {
        return Err("Rust Version startup requires standalone RUST_VERSION_TASKS_ENABLED=1");
    }
    let explicit_url = |value: Option<&OsStr>| {
        value
            .and_then(OsStr::to_str)
            .is_some_and(|url| !url.is_empty() && url.len() <= 4096 && url.trim() == url)
    };
    if !explicit_url(broker) || !explicit_url(backend) {
        return Err(
            "Rust Version startup requires explicit CELERY_BROKER_URL and CELERY_RESULT_BACKEND",
        );
    }
    Ok(true)
}

fn database_url() -> Result<String, String> {
    database_config::url()
}

#[derive(Clone)]
struct HealthState {
    stream_files: AuthService,
    store: Store,
    selected: Option<selected_jobs::SelectedJobs>,
    core_scheduled: Option<core_scheduled_jobs::CoreScheduledJobs>,
    refresh_tvg: Option<dispatcharr_job_transport::tvg_programme_application::Application>,
}
async fn health(State(state): State<HealthState>) -> (StatusCode, Json<serde_json::Value>) {
    if !state.stream_files.stream_files_ready() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"status":"stream_files_unsettled"})),
        );
    }
    let store = state.store;
    // Readiness verifies the migrated schema as well as database connectivity.
    match store.setting("proxy_settings").await {
        Ok(_) => {
            if let Some(selected) = state.selected {
                let (ready, details) = selected.readiness();
                return (
                    if ready {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    },
                    Json(
                        serde_json::json!({"status":if ready{"ready"}else{"selected_workers_not_ready"},"backend":"rust","parity_complete":false,"selected_workers":details}),
                    ),
                );
            }
            if let Some(owner) = state.core_scheduled {
                let ready = owner.ready();
                return (
                    if ready {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    },
                    Json(serde_json::json!({
                        "status": if ready {"ready"} else {"core_schedule_not_ready"},
                        "backend": "rust",
                        "parity_complete": false,
                        "core_scheduled_jobs": {"ready": ready},
                    })),
                );
            }
            if let Some(tvg) = state.refresh_tvg {
                let ready = tvg.channel_publication_ready();
                return (
                    if ready {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    },
                    Json(serde_json::json!({
                        "status": if ready {"ready"} else {"guide_worker_not_ready"},
                        "backend": "rust",
                        "parity_complete": false,
                        "guide_programmes": {"ready": ready},
                    })),
                );
            }
            (
                StatusCode::OK,
                Json(
                    serde_json::json!({"status":"ready","backend":"rust","parity_complete":false}),
                ),
            )
        }
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"status":"database_unavailable"})),
        ),
    }
}

async fn api_not_found() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({"detail":"Not found."})),
    )
}

async fn legacy_react_alias() -> Response {
    let Some(path) = env::var_os("RUST_FRONTEND_DIR") else {
        return (
            StatusCode::NOT_IMPLEMENTED,
            Json(serde_json::json!({"error":"Bundled React is required for this legacy alias"})),
        )
            .into_response();
    };
    let index = std::path::PathBuf::from(path).join("index.html");
    // Bound this exceptional alias separately from normal static-file streaming.
    if tokio::fs::metadata(&index)
        .await
        .is_ok_and(|metadata| metadata.len() <= 1024 * 1024)
    {
        if let Ok(bytes) = tokio::fs::read(index).await {
            return (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                bytes,
            )
                .into_response();
        }
    }
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({"error":"Bundled React unavailable"})),
    )
        .into_response()
}

// Source's slashless path is the CSRF-protected React TemplateView, not this API.
async fn combined_stats_legacy_alias(request: axum::extract::Request) -> Response {
    let method = request.method().clone();
    let headers = request.headers().clone();
    let body = match axum::body::to_bytes(request.into_body(), 64 * 1024).await {
        Ok(body) => body,
        Err(_) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                "Legacy alias input exceeds capacity",
            )
                .into_response()
        }
    };
    if let Some(body) = dispatcharr_output_http::source_csrf_failure(&method, &headers, &body) {
        return (
            StatusCode::FORBIDDEN,
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            body,
        )
            .into_response();
    }
    if method == Method::GET {
        return legacy_react_alias().await;
    }
    if method == Method::HEAD {
        let (mut parts, body) = legacy_react_alias().await.into_parts();
        let bytes = match axum::body::to_bytes(body, 1024 * 1024).await {
            Ok(bytes) => bytes,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        parts
            .headers
            .insert(header::CONTENT_LENGTH, bytes.len().into());
        return Response::from_parts(parts, axum::body::Body::empty());
    }
    let status = if method == Method::OPTIONS {
        StatusCode::OK
    } else {
        StatusCode::METHOD_NOT_ALLOWED
    };
    (
        status,
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::ALLOW, "GET, HEAD, OPTIONS"),
            (header::CONTENT_LENGTH, "0"),
        ],
        "",
    )
        .into_response()
}

async fn run(logs: &managed_logs::ManagedLogs) -> Result<(), String> {
    let admin_location = admin_ingress::configured_location()?;
    let (selected_jobs_enabled, selected_beat) = selected_jobs::configuration()?;
    let core_scheduled_jobs_enabled = core_scheduled_jobs::enabled()?;
    selected_beat.validate_owners(
        core_scheduled_jobs_enabled,
        env::var_os("RUST_BACKUP_TASKLESS_RESTORE_OWNER").is_some(),
    )?;
    if selected_jobs_enabled && core_scheduled_jobs_enabled {
        return Err("Configure one application queue owner".into());
    }
    let core_plugin_refresh =
        core_scheduled_jobs::plugin_refresh_enabled(core_scheduled_jobs_enabled)?;
    let core_plugin_startup = core_scheduled_jobs::plugin_startup_enabled(core_plugin_refresh)?;
    let core_provider_rehash =
        core_scheduled_jobs::provider_rehash_enabled(core_scheduled_jobs_enabled)?;
    let shutdown_timeout = Duration::from_secs(positive("RUST_SHUTDOWN_SECONDS", 12)? as u64);
    let core_restore_grace =
        core_scheduled_jobs::restore_grace(core_scheduled_jobs_enabled, shutdown_timeout)?;
    // A Selected startup owns real SQL and Redis preparation before the HTTP
    // server exists. Register once before those operations, then await them
    // normally. Never use the signal to drop their in-progress futures.
    let mut selected_stop = if selected_jobs_enabled || core_scheduled_jobs_enabled {
        Some(LatchedShutdown::register()?)
    } else {
        None
    };
    if let Some(signal) = &mut selected_stop {
        let _ = signal.pending().await;
    }
    let cache_url = redis_config::url()?;
    #[cfg(target_os = "linux")]
    let taskless_restore_helper =
        backup_config::taskless_selected(selected_jobs_enabled, cache_url.as_deref())?;
    #[cfg(not(target_os = "linux"))]
    if env::var_os("RUST_BACKUP_TASKLESS_RESTORE_OWNER").is_some() {
        return Err("Taskless restore is only packaged for Linux".into());
    }
    let bind: SocketAddr = env::var("RUST_BIND")
        .unwrap_or_else(|_| "127.0.0.1:9192".into())
        .parse()
        .map_err(|_| "Invalid RUST_BIND")?;
    let secret = env::var("DJANGO_SECRET_KEY").map_err(|_| "DJANGO_SECRET_KEY is required")?;
    let mut auth_config =
        AuthConfig::new(secret.clone()).map_err(|_| "Invalid authentication configuration")?;
    auth_config.trusted_proxies = env::var("DISPATCHARR_TRUSTED_PROXIES").ok();
    let database_url = database_url()?;
    match env::var("RUST_INITIALIZE_EMPTY_DATABASE").as_deref() {
        Ok("1")=>match dispatcharr_store::initialize_empty_database(&database_url).await
            .map_err(|_|"Empty database initialization failed (credentials omitted)")? {
            dispatcharr_store::SchemaInitialization::Created{tables,migrations}=>eprintln!("Created original Dispatcharr schema: {tables} tables, {migrations} migration records"),
            dispatcharr_store::SchemaInitialization::SkippedExisting{objects}=>eprintln!("Skipped initialization: {objects} existing database objects; schema compatibility was not checked"),
        },
        Ok("0")|Err(_)=>{},
        Ok(_)=>return Err("RUST_INITIALIZE_EMPTY_DATABASE must be 0 or 1".into()),
    }
    let store = Store::connect(
        &database_url,
        positive("RUST_DATABASE_POOL_SIZE", 8)?
            .try_into()
            .map_err(|_| "Database pool size overflow")?,
    )
    .await
    .map_err(|_| "Database connection failed (credentials omitted)")?;
    let auth =
        AuthService::new(store.clone(), auth_config).with_native_log_liveness(logs.liveness());
    let auth = if let Some(url) = cache_url.as_deref() {
        auth.with_notification_cache(
            dispatcharr_store::NotificationCache::connect(url)
                .await
                .map_err(|_| "Notification cache initialization failed (credentials omitted)")?,
        )
    } else {
        auth
    };
    // Original startup warms the system group before HTTP, leaving the
    // display-zone debounce untouched until the first real request.
    if let Some(system_settings) = auth.startup_system_settings_group().await {
        logs.seed(&system_settings);
    }
    let realtime_bus = realtime::bus(cache_url.as_deref()).await?;
    // Attach before every resolver/environment/XC clone so original producers
    // publish through the same bounded process transport.
    let auth = if let Some(bus) = &realtime_bus {
        auth.with_event_sink(Arc::new(bus.clone()))
            .with_profile_save_signal(dispatcharr_job_transport::profile_save_signal(Arc::new(
                bus.clone(),
            )))
    } else {
        auth
    };
    // Source CoreConfig.ready synchronizes developer notices before serving.
    // A failed sync is logged, as in Source; it does not prevent app startup.
    if let Err(error) = auth.startup_developer_notifications().await {
        tracing::warn!(%error, "Developer notification startup synchronization failed");
    }
    // Temporary migration capability while exceptional settings shapes and
    // live Main transitions remain outside the measured numeric Source cohort.
    let dynamic_health = match env::var("RUST_DYNAMIC_PROXY_HEALTH_ENABLED").as_deref() {
        Ok("1") => true,
        Ok("0") | Err(_) => false,
        _ => return Err("RUST_DYNAMIC_PROXY_HEALTH_ENABLED must be 0 or 1".into()),
    };
    // One Core instance supplies all qualified settings routes and, when
    // enabled, dynamic health. Pre-engine Auth prevents an EngineControl cycle.
    let core_settings = if realtime_bus.is_some() {
        let effects = dispatcharr_store::CoreSettingsSpecialEffects::new(std::path::PathBuf::from(
            env::var("DISPATCHARR_LOG_DIR").unwrap_or_else(|_| "/data/logs".into()),
        ))
        .map_err(|_| "Invalid Core settings effects directory")?;
        Some(
            dispatcharr_store::CoreSettingsWrites::new_main(auth.clone())
                .and_then(|core| core.with_special_effects(effects))
                .map_err(|_| "Core settings require the shared cache and realtime publisher")?,
        )
    } else {
        if dynamic_health {
            return Err(
                "Dynamic proxy health requires the shared cache and realtime publisher".into(),
            );
        }
        None
    };
    let config = live_engine_config()?;
    let proxy_settings = store
        .setting("proxy_settings")
        .await
        .map_err(|_| "Proxy settings unavailable")?;
    let config = live_engine_settings(config, &proxy_settings)?;
    let mut hls_config = dispatcharr_hls_http::GatewayConfig {
        max_managers: config.max_channels,
        max_sessions: positive(
            "RUST_MAX_CLIENTS",
            config
                .max_channels
                .checked_mul(config.max_viewers_per_channel)
                .ok_or("Client capacity overflow")?,
        )?,
        max_retained_bytes: positive("RUST_HLS_CACHE_BYTES", 512 * 1024 * 1024)?,
        max_segment_bytes: positive("RUST_OUTPUT_SEGMENT_BYTES", 32 * 1024 * 1024)?,
        max_stored_segments: positive("RUST_OUTPUT_MAX_SEGMENTS", 256)?,
        max_requests_per_session: positive("RUST_HLS_REQUESTS_PER_SESSION", 8)?,
        initialization_timeout: config.health_initialization_grace,
        ..Default::default()
    };
    let output_config = OutputConfig {
        max_segment_bytes: hls_config.max_segment_bytes,
        max_segments: hls_config.max_stored_segments,
        max_channel_bytes: positive("RUST_OUTPUT_CHANNEL_BYTES", 256 * 1024 * 1024)?,
        max_read_bytes: positive("RUST_OUTPUT_READ_BYTES", 256 * 1024 * 1024)?,
        max_operations: positive("RUST_OUTPUT_OPERATIONS", 16)?,
        chunk_ttl: Duration::from_secs(positive("RUST_HLS_SEGMENT_TTL_SECONDS", 100)? as u64),
        ..Default::default()
    };
    hls_config.segment_ttl = output_config.chunk_ttl;
    let input_config = live_input_config(&config)?;
    let lease_config = LeaseConfig::default();
    let (coordinator, clients, outputs, inputs) = if let Some(url) = cache_url.as_deref() {
        let coordinator = Coordinator::connect(url, lease_config.clone())
            .await
            .map_err(|_| "Redis coordination unavailable (credentials omitted)")?;
        let client_config = ClientConfig {
            max_clients: positive(
                "RUST_MAX_CLIENTS",
                config
                    .max_channels
                    .checked_mul(config.max_viewers_per_channel)
                    .ok_or("Client capacity overflow")?,
            )?,
            ..ClientConfig::default()
        };
        let clients = ClientStore::connect(url, client_config)
            .await
            .map_err(|_| "Redis client metadata unavailable (credentials omitted)")?;
        let outputs = OutputStore::connect(url, output_config)
            .await
            .map_err(|_| "Redis output storage unavailable (credentials omitted)")?;
        let inputs = InputStore::connect(url, input_config)
            .await
            .map_err(|_| "Redis input storage unavailable (credentials omitted)")?;
        (
            Some(coordinator),
            Some(clients),
            Some(outputs),
            Some(inputs),
        )
    } else {
        (None, None, None, None)
    };
    let engine =
        StreamEngine::new_with_shared_inputs(config, coordinator.clone(), clients.clone(), inputs)
            .and_then(StreamEngine::with_source_http_retry_backoff)
            .map_err(|_| "Invalid streaming configuration")?;
    let engine = match core_settings.as_ref().filter(|_| dynamic_health) {
        Some(core) => engine
            .with_health_settings(Arc::new(proxy_health::CoreProxyHealthSource::new(
                core.clone(),
            )))
            .map_err(|_| "Dynamic proxy health must be bound before engine sharing")?,
        None => engine,
    };
    let live_client_connect = Arc::new(dispatcharr_store::LiveClientConnect::new(auth.clone()));
    let engine = engine
        .with_live_client_connect(live_client_connect.clone())
        .map_err(|_| "Live Connect observer unavailable")?;
    let engine = match outputs.as_ref() {
        Some(outputs) => engine
            .with_output_profiles(outputs.clone(), Arc::new(()))
            .map_err(|_| "Shared output profile owner dispatch unavailable")?,
        None => engine,
    };
    if outputs.is_some() && !engine.output_profiles_ready(Duration::from_secs(5)).await {
        return Err("Shared output profile event reader not ready".into());
    }
    let live_statistics = statistics::start(&engine, clients.as_ref(), realtime_bus.as_ref())?;
    let auth = auth.with_channel_control(Arc::new(control::EngineControl(engine.clone())));
    let stream_regex = dispatcharr_store::StreamRegexPreview::prepare(auth.clone());
    let realtime = realtime::gateway(auth.clone(), realtime_bus.clone(), stream_regex.clone())?;
    let resolver: Arc<dyn dispatcharr_contracts::PlaybackResolver> = Arc::new(auth.clone());
    let hls = clients
        .clone()
        .map(|clients| {
            dispatcharr_hls_http::HlsGateway::new(
                engine.clone(),
                clients,
                resolver.clone(),
                hls_config,
            )
            .and_then(|gateway| {
                gateway.with_output_store(outputs.clone().ok_or_else(|| {
                    dispatcharr_contracts::PlaybackError::new(503, "Shared output unavailable")
                })?)
            })
        })
        .transpose()
        .map_err(|_| "Invalid HLS configuration")?;
    let fmp4 = clients
        .clone()
        .zip(outputs.clone())
        .map(|(clients, outputs)| {
            dispatcharr_fmp4_http::Fmp4Gateway::new(
                engine.clone(),
                clients,
                outputs,
                dispatcharr_fmp4_http::Config {
                    executable: env::var("RUST_FFMPEG_PATH").unwrap_or_else(|_| "ffmpeg".into()),
                    max_remuxers: positive("RUST_MAX_REMUXERS", 32)?,
                    max_init_bytes: positive("RUST_FMP4_INIT_BYTES", 10 * 1024 * 1024)?,
                    max_fragment_bytes: positive("RUST_FMP4_FRAGMENT_BYTES", 16 * 1024 * 1024)?,
                    ..Default::default()
                },
            )
            .map_err(|_| "Invalid fMP4 configuration".to_string())
        })
        .transpose()?;
    // Recovery remains an explicit verification lane until its packaged
    // SQL/authentication/restart and remaining entry semantics are accepted.
    let vod_recovery_enabled = match env::var("RUST_VOD_REDIS_RECOVERY_ENABLED").as_deref() {
        Ok("0") | Err(_) => false,
        Ok("1") => true,
        _ => return Err("RUST_VOD_REDIS_RECOVERY_ENABLED must be 0 or 1".into()),
    };
    if vod_recovery_enabled && (coordinator.is_none() || realtime_bus.is_none()) {
        return Err(
            "Enabled VOD recovery requires Redis coordination and realtime publisher".into(),
        );
    }
    let vod = if let Some(coordinator) = coordinator.clone() {
        let redis_url = cache_url
            .as_deref()
            .ok_or("Redis VOD configuration missing")?;
        let engine = dispatcharr_vod_http::VodEngine::new(
            store.clone(),
            auth.clone(),
            coordinator,
            redis_url,
            dispatcharr_vod_http::VodConfig {
                max_sessions: positive("RUST_MAX_VOD_SESSIONS", 1024)?,
                max_requests: positive("RUST_MAX_VOD_REQUESTS", 2048)?,
                frame_bytes: positive("RUST_VOD_FRAME_BYTES", 64 * 1024)?,
                ..Default::default()
            },
        )
        .await
        .map_err(|_| "VOD initialization failed (credentials omitted)")?;
        let engine = if let Some(bus) = &realtime_bus {
            engine
                .with_events(
                    store.clone(),
                    auth.clone(),
                    redis_url,
                    Arc::new(bus.clone()),
                )
                .await
                .map_err(|_| "VOD event initialization failed (credentials omitted)")?
        } else {
            engine
        };
        let engine = if vod_recovery_enabled {
            engine
                .with_redis_recovery(&lease_config.namespace)
                .map_err(|_| "VOD recovery configuration failed")?
        } else {
            engine
        };
        Some(engine)
    } else {
        None
    };
    let (catchup_sessions, catchup, catchup_management) = if let Some(coordinator) = coordinator {
        let redis_url = cache_url
            .as_deref()
            .ok_or("Redis catch-up configuration missing")?;
        let sessions = dispatcharr_timeshift_http::CatchupSessions::new(
            store.clone(),
            auth.clone(),
            redis_url,
            dispatcharr_timeshift_http::SessionConfig::default(),
        )
        .await
        .map_err(|_| "Catch-up sessions initialization failed (credentials omitted)")?;
        let media = dispatcharr_timeshift_http::CatchupMedia::new(
            store.clone(),
            auth.clone(),
            sessions.clone(),
            coordinator,
            redis_url,
            dispatcharr_timeshift_http::CatchupMediaConfig {
                pool: dispatcharr_timeshift_http::ArchivePoolConfig {
                    max_sessions: positive("RUST_MAX_TIMESHIFT_SESSIONS", 1024)?,
                    ..Default::default()
                },
                max_upstreams: positive("RUST_MAX_TIMESHIFT_UPSTREAMS", 1024)?,
                body_frame_bytes: positive("RUST_TIMESHIFT_FRAME_BYTES", 64 * 1024)?,
                ..Default::default()
            },
        )
        .await
        .map_err(|_| "Catch-up media initialization failed (credentials omitted)")?;
        let media = if let Some(bus) = &realtime_bus {
            media
                .with_events(store.clone(), redis_url, Arc::new(bus.clone()))
                .await
                .map_err(|_| "Catch-up event initialization failed (credentials omitted)")?
        } else {
            media
        };
        let management = dispatcharr_timeshift_http::CatchupManagement::new(
            store.clone(),
            auth.clone(),
            media.clone(),
            redis_url,
        )
        .await
        .map_err(|_| "Catch-up management initialization failed (credentials omitted)")?;
        let management = if let Some(bus) = &realtime_bus {
            management.with_events(Arc::new(bus.clone()))
        } else {
            management
        };
        (Some(sessions), Some(media), Some(management))
    } else {
        (None, None, None)
    };
    let output = if let Some(redis_url) = cache_url.as_deref() {
        Some(
            dispatcharr_output_http::OutputGateway::new(
                store.clone(),
                auth.clone(),
                redis_url,
                dispatcharr_output_http::OutputConfig {
                    max_requests: positive("RUST_MAX_OUTPUT_REQUESTS", 4)?,
                    max_channels: positive("RUST_MAX_OUTPUT_CHANNELS", 50000)?,
                    max_metadata_bytes: positive("RUST_OUTPUT_METADATA_BYTES", 8 * 1024 * 1024)?,
                    max_export_bytes: positive("RUST_OUTPUT_EXPORT_BYTES", 8 * 1024 * 1024)?,
                    max_memory_bytes: positive("RUST_OUTPUT_MEMORY_BYTES", 192 * 1024 * 1024)?,
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| "Output initialization failed (credentials omitted)")?,
        )
    } else {
        None
    };
    let xmltv = if let Some(redis_url) = cache_url.as_deref() {
        Some(
            dispatcharr_xmltv_http::gateway::Gateway::new(
                store.clone(),
                auth.clone(),
                redis_url,
                dispatcharr_xmltv_http::gateway::Config {
                    max_requests: positive("RUST_MAX_XMLTV_REQUESTS", 64)?,
                    max_builds: positive("RUST_MAX_XMLTV_BUILDS", 2)?,
                    max_memory_bytes: positive("RUST_XMLTV_MEMORY_BYTES", 512 * 1024 * 1024)?,
                    max_body_bytes: positive("RUST_XMLTV_BODY_BYTES", 64 * 1024)?,
                    max_frame_bytes: positive("RUST_XMLTV_FRAME_BYTES", 1024 * 1024)?,
                    max_number_bytes: positive("RUST_XMLTV_NUMBER_BYTES", 104 * 1024 * 1024)?,
                    setup_deadline: Duration::from_secs(positive(
                        "RUST_XMLTV_SETUP_TIMEOUT_SECONDS",
                        15,
                    )? as u64),
                    stream_deadline: Duration::from_secs(positive(
                        "RUST_XMLTV_STREAM_TIMEOUT_SECONDS",
                        600,
                    )? as u64),
                    rows: dispatcharr_xmltv_http::rows::Limits {
                        max_channels: positive("RUST_MAX_XMLTV_CHANNELS", 50000)?,
                        max_metadata_bytes: positive(
                            "RUST_XMLTV_METADATA_BYTES",
                            16 * 1024 * 1024,
                        )?,
                        max_programme_metadata_bytes: positive(
                            "RUST_XMLTV_PROGRAMME_METADATA_BYTES",
                            1024 * 1024,
                        )?,
                        max_programmes: positive("RUST_MAX_XMLTV_PROGRAMMES", 2_000_000)?,
                    },
                    cache: dispatcharr_xmltv_http::cache::CacheConfig {
                        max_entries: positive("RUST_XMLTV_CACHE_ENTRIES", 128)?,
                        max_cache_bytes: positive("RUST_XMLTV_CACHE_BYTES", 512 * 1024 * 1024)?,
                        max_export_bytes: positive("RUST_XMLTV_EXPORT_BYTES", 256 * 1024 * 1024)?,
                        max_chunks: positive("RUST_XMLTV_CACHE_CHUNKS", 65536)?,
                        chunk_bytes: positive("RUST_XMLTV_CHUNK_BYTES", 64 * 1024)?,
                        read_batch_bytes: positive("RUST_XMLTV_READ_BATCH_BYTES", 512 * 1024)?,
                        max_read_bytes: positive("RUST_XMLTV_IO_BYTES", 64 * 1024 * 1024)?,
                        max_legacy_chunk_bytes: positive(
                            "RUST_XMLTV_LEGACY_CHUNK_BYTES",
                            8 * 1024 * 1024,
                        )?,
                        ..Default::default()
                    },
                },
            )
            .await
            .map_err(|_| "XMLTV initialization failed (credentials omitted)")?,
        )
    } else {
        None
    };
    let xc = if let Some(redis_url) = cache_url.as_deref() {
        let counts = dispatcharr_xc_http::counts::ConnectionCounts::connect_source(redis_url)
            .await
            .map_err(|_| "XC counter initialization failed (credentials omitted)")?;
        let gateway = dispatcharr_xc_http::XcGateway::new(
            store.clone(),
            auth.clone(),
            counts,
            dispatcharr_xc_http::XcConfig {
                max_requests: positive("RUST_MAX_XC_REQUESTS", 32)?,
                max_rows: positive("RUST_MAX_XC_CHANNELS", 50000)?,
                max_body_bytes: positive("RUST_XC_BODY_BYTES", 64 * 1024)?,
                max_metadata_bytes: positive("RUST_XC_METADATA_BYTES", 8 * 1024 * 1024)?,
                max_response_bytes: positive("RUST_XC_RESPONSE_BYTES", 16 * 1024 * 1024)?,
                max_number_bytes: positive("RUST_XC_NUMBER_BYTES", 128 * 1024 * 1024)?,
                max_memory_bytes: positive("RUST_XC_MEMORY_BYTES", 192 * 1024 * 1024)?,
                request_deadline: Duration::from_secs(positive(
                    "RUST_XC_REQUEST_TIMEOUT_SECONDS",
                    15,
                )? as u64),
            },
        )
        .map_err(|_| "XC initialization failed (credentials omitted)")?
        .with_epg()
        .with_vod_lists();
        Some(
            if env::var_os("RUST_XC_CACHED_VOD_INFO").as_deref() == Some(std::ffi::OsStr::new("1"))
            {
                gateway.with_cached_vod_info()
            } else {
                gateway
            },
        )
    } else {
        None
    };
    let hdhr = dispatcharr_hdhr_http::HdhrGateway::new(
        store.clone(),
        auth.clone(),
        dispatcharr_hdhr_http::HdhrConfig {
            max_requests: positive("RUST_MAX_HDHR_REQUESTS", 32)?,
            max_channels: positive("RUST_MAX_HDHR_CHANNELS", 50000)?,
            max_devices: positive("RUST_MAX_HDHR_DEVICES", 4096)?,
            max_body_bytes: positive("RUST_HDHR_BODY_BYTES", 64 * 1024)?,
            max_metadata_bytes: positive("RUST_HDHR_METADATA_BYTES", 8 * 1024 * 1024)?,
            max_response_bytes: positive("RUST_HDHR_RESPONSE_BYTES", 16 * 1024 * 1024)?,
            max_memory_bytes: positive("RUST_HDHR_MEMORY_BYTES", 128 * 1024 * 1024)?,
            request_deadline: Duration::from_secs(
                positive("RUST_HDHR_REQUEST_TIMEOUT_SECONDS", 15)? as u64,
            ),
        },
    )
    .map_err(|_| "HDHR initialization failed (credentials omitted)")?;
    let output_profiles =
        dispatcharr_store::OutputProfileWrites::new(auth.clone(), cache_url.as_deref())
            .await
            .map_err(|_| "Output profile initialization failed (credentials omitted)")?;
    // Original BACKUP_ROOT semantics; no production restore authority or job queue.
    let backup_files = dispatcharr_store::BackupFiles::new(
        auth.clone(),
        std::path::PathBuf::from(
            env::var("BACKUP_ROOT").unwrap_or_else(|_| "/data/backups".into()),
        ),
        secret,
    )
    .map_err(|_| "Invalid backup file API configuration")?
    .with_nginx_accel(
        env::var("USE_NGINX_ACCEL").is_ok_and(|value| value.eq_ignore_ascii_case("true")),
    );
    let mut epg_programs = dispatcharr_store::EpgProgramReads::new(
        auth.clone(),
        dispatcharr_store::EpgProgramReadLimits {
            concurrent_responses: positive("RUST_MAX_EPG_READ_RESPONSES", 4)?,
            response_bytes: positive("RUST_EPG_READ_RESPONSE_BYTES", 16 * 1024 * 1024)?,
            row_bytes: positive("RUST_EPG_READ_ROW_BYTES", 1024 * 1024)?,
            deadline: Duration::from_millis(positive("RUST_EPG_READ_TIMEOUT_MS", 2000)? as u64),
        },
    )
    .map_err(|_| "Invalid EPG programme read configuration")?;
    // Incomplete queue integration stays explicitly disabled by default until
    // Index/Series/ETA children and second-import workflow are proven. Broker
    // and backend overrides otherwise follow the Source configuration.
    let refresh_enabled = match env::var("RUST_EPG_REFRESH_ENABLED").as_deref() {
        Ok("0") | Err(_) => false,
        Ok("1") => true,
        _ => return Err("RUST_EPG_REFRESH_ENABLED must be 0 or 1".into()),
    };
    let refresh_enabled = refresh_enabled || selected_jobs_enabled;
    let version_tasks_enabled = match env::var("RUST_VERSION_TASKS_ENABLED").as_deref() {
        Ok("0") | Err(_) => false,
        Ok("1") => true,
        _ => return Err("RUST_VERSION_TASKS_ENABLED must be 0 or 1".into()),
    };
    let version_tasks_enabled = version_tasks_enabled || selected_jobs_enabled;
    let version_startup = version_startup_owned(
        env::var_os("RUST_VERSION_STARTUP_OWNER").as_deref(),
        version_tasks_enabled,
        selected_jobs_enabled,
        refresh_enabled,
        env::var_os("CELERY_BROKER_URL").as_deref(),
        env::var_os("CELERY_RESULT_BACKEND").as_deref(),
    )?;
    let original_m3u_refresh_all = m3u_refresh_all_config::prepare(
        auth.clone(),
        selected_jobs_enabled,
        refresh_enabled || version_tasks_enabled,
    )
    .await?;
    let version_tasks = if version_tasks_enabled {
        let cache = cache_url
            .clone()
            .ok_or("Enabled version tasks require Redis configuration")?;
        let bus = realtime_bus
            .as_ref()
            .ok_or("Enabled version tasks require the realtime publisher")?;
        let broker = env::var("CELERY_BROKER_URL").unwrap_or_else(|_| cache.clone());
        let backend = env::var("CELERY_RESULT_BACKEND").unwrap_or_else(|_| broker.clone());
        let app = dispatcharr_job_transport::version_application::Application::prepare(
            store.clone(),
            Arc::new(bus.clone()),
            &broker,
            &backend,
            version_task_options(),
        )
        .await
        .map_err(|_| "Version task initialization failed (credentials omitted)")?;
        Some(app)
    } else {
        None
    };
    let manual_backup = manual_backup_config::prepare(
        selected_jobs_enabled
            || refresh_enabled
            || version_tasks_enabled
            || original_m3u_refresh_all.is_some()
            || core_scheduled_jobs_enabled,
        store.clone(),
        auth.clone(),
        cache_url.as_deref(),
    )
    .await?;
    let artwork_completions = dispatcharr_job_transport::ArtworkCompletions::new();
    let mut epg_index = None;
    let mut epg_series = None;
    let mut epg_artwork = None;
    let mut epg_tvg = None;
    let epg_refresh = if refresh_enabled {
        let cache = cache_url
            .clone()
            .ok_or("Enabled EPG jobs require Redis configuration")?;
        let bus = realtime_bus
            .as_ref()
            .ok_or("Enabled EPG jobs require the realtime publisher")?;
        let broker = env::var("CELERY_BROKER_URL").unwrap_or_else(|_| cache.clone());
        let backend = env::var("CELERY_RESULT_BACKEND").unwrap_or_else(|_| broker.clone());
        // This is the path base for stored relative input files, not a Python
        // source-tree dependency. The packaged image supplies /app and its
        // /app/media -> /data mapping without mounting Original source code.
        let source_cwd = std::path::PathBuf::from(
            env::var("RUST_EPG_SOURCE_CWD").unwrap_or_else(|_| "/app".into()),
        );
        let media_root = std::path::PathBuf::from(
            env::var("RUST_EPG_MEDIA_ROOT").unwrap_or_else(|_| "/app/media".into()),
        );
        if !source_cwd.is_dir() || !media_root.is_dir() {
            return Err(
                "Enabled EPG jobs require their configured input and media directories".into(),
            );
        }
        let cache_root = media_root.join("cached_epg");
        let cache_dir = env::var("RUST_EPG_CACHE_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| cache_root.clone());
        if !cache_dir.is_absolute()
            || cache_dir == std::path::Path::new("/")
            || cache_dir.as_os_str().len() > 4096
        {
            return Err("Invalid configured EPG cache directory".into());
        }
        std::fs::create_dir_all(&cache_dir)
            .map_err(|_| "Configured EPG cache directory unavailable")?;
        use std::os::unix::fs::MetadataExt;
        let input_uid = media_root
            .metadata()
            .map_err(|_| "EPG media root metadata unavailable")?
            .uid();
        // Prepare without intake, so a later constructor failure cannot consume
        // an existing index task before all application dependencies are ready.
        let index = dispatcharr_job_transport::index_application::IndexApplication::connect(
            store.clone(),
            dispatcharr_job_transport::index_application::Options {
                broker: broker.clone(),
                backend: backend.clone(),
                cache: cache.clone(),
                source_cwd,
                media_root: media_root.clone(),
                cache_dir: cache_dir.clone(),
            },
        )
        .await
        .map_err(|_| "EPG index initialization failed (credentials omitted)")?;
        epg_programs = epg_programs
            .with_current_file_lookup(index.lookup(), cache_dir.clone())
            .map_err(|_| "Invalid EPG current-file path configuration")?;
        // The natural Channel post-save task precedes configured descendants
        // in the real default FIFO. Consume it with its own retained worker;
        // never skip it to reach a later Index or Series task.
        let tvg = match dispatcharr_job_transport::tvg_programme_application::Application::prepare(
            store.clone(),
            dispatcharr_job_transport::tvg_programme_application::Options {
                broker: broker.clone(),
                backend: backend.clone(),
                cache: cache.clone(),
                input_roots: vec![media_root, cache_dir.clone()],
                input_uid,
            },
        )
        .await
        {
            Ok(service) => service,
            Err(error) => {
                let _ = index.shutdown(Duration::from_secs(4)).await;
                return Err(error.into());
            }
        };
        let (refresh, series_handoff) =
            match dispatcharr_job_transport::refresh_application::RefreshApplication::prepare_configured(
                store.clone(),
                auth.clone(),
                Arc::new(bus.clone()),
                dispatcharr_job_transport::refresh_application::RefreshApplicationOptions {
                    broker: broker.clone(),
                    backend: backend.clone(),
                    cache: cache.clone(),
                    cache_dir,
                },
            )
            .await
            {
                Ok(service) => service,
                Err(_) => {
                    let _ = tvg.shutdown(Duration::from_secs(4)).await;
                    let _ = index.shutdown(Duration::from_secs(4)).await;
                    return Err("EPG refresh initialization failed (credentials omitted)".into());
                }
            };
        let artwork=match dispatcharr_job_transport::recording_existing_artwork_application::Application::prepare(
            store.pool().clone(),Arc::new(bus.clone()),artwork_completions.clone(),&broker,&backend,Duration::from_millis(100),
        ).await{
            Ok(service)=>service,
            Err(error)=>{
                let _=refresh.shutdown(Duration::from_secs(4)).await;
                let _=tvg.shutdown(Duration::from_secs(4)).await;
                let _=index.shutdown(Duration::from_secs(4)).await;
                return Err(error.into());
            }
        };
        let series =
            match dispatcharr_job_transport::configured_series_application::Application::prepare_with_artwork(
                store.clone(),
                Arc::new(bus.clone()),
                series_handoff,
                dispatcharr_job_transport::configured_series_application::Options {
                    broker,
                    backend,
                    cache,
                    poll_interval: Duration::from_millis(100),
                },
                artwork_completions.clone(),
            )
            .await
            {
                Ok(service) => service,
                Err(error) => {
                    let _ = artwork.shutdown(Duration::from_secs(4)).await;
                    let _ = refresh.shutdown(Duration::from_secs(4)).await;
                    let _ = tvg.shutdown(Duration::from_secs(4)).await;
                    let _ = index.shutdown(Duration::from_secs(4)).await;
                    return Err(error.into());
                }
            };
        if !selected_jobs_enabled
            && (artwork.start().is_err()
                || index.start(Arc::new(())).is_err()
                || series.start().is_err()
                || tvg.start(Arc::new(())).is_err()
                || refresh.start().is_err())
        {
            let _ = refresh.shutdown(Duration::from_secs(4)).await;
            let _ = tvg.shutdown(Duration::from_secs(4)).await;
            artwork.close_producer_admission();
            let _ = series.shutdown(Duration::from_secs(4)).await;
            let _ = artwork.shutdown(Duration::from_secs(4)).await;
            let _ = index.shutdown(Duration::from_secs(4)).await;
            return Err("EPG index intake initialization failed".into());
        }
        if !selected_jobs_enabled {
            auth.bind_tvg_programme_publisher(Arc::new(tvg.clone()))
                .map_err(|_| "TVG programme publisher already composed")?;
        }
        epg_index = Some(index);
        epg_series = Some(series);
        epg_artwork = Some(artwork);
        epg_tvg = Some(tvg);
        Some(refresh)
    } else {
        None
    };
    let vod_reads =
        dispatcharr_store::VodReads::new(auth.clone(), dispatcharr_store::VodReadLimits::default())
            .map_err(|_| "Invalid ordinary VOD read configuration (credentials omitted)")?;
    let grid = dispatcharr_output_http::GridGateway::new(
        store.clone(),
        auth.clone(),
        dispatcharr_output_http::GridReadLimits {
            concurrent_responses: positive("RUST_MAX_GRID_RESPONSES", 2)?,
            memory_bytes: positive("RUST_GRID_MEMORY_BYTES", 192 * 1024 * 1024)?,
            deadline: Duration::from_millis(positive("RUST_GRID_TIMEOUT_MS", 5000)? as u64),
        },
    )
    .map_err(|_| "Invalid EPG grid read configuration")?;
    let xc = xc
        .map(|gateway| {
            gateway
                .with_vod_refresh(vod_reads.xc_vod_refresh())?
                .with_live(playback::dispatcher(
                    engine.clone(),
                    hls.clone(),
                    fmp4.clone(),
                ))
        })
        .transpose()
        .map_err(|_| "XC live dispatcher initialization failed")?;
    let combined_statistics = combined_statistics::CombinedStatistics::connect(
        auth.clone(),
        store.clone(),
        engine.clone(),
        catchup_management.clone(),
        cache_url.as_deref(),
        combined_statistics::Config::default(),
    )
    .await
    .map_err(|_| "Combined statistics initialization failed (credentials omitted)")?;
    // Preserve the shared AuthService admission pool when repository routes join
    // Core and the other application APIs. The fetch adapter still rejects its
    // explicitly unsupported transports and signatures.
    let plugin_repositories =
        dispatcharr_store::PluginRepositories::new(auth.clone()).with_unsigned_manifest_fetches();
    let plugin_schedule =
        Arc::new(dispatcharr_job_transport::normal_beat::PluginRepositoryScheduleAdapter);
    let plugin_actions = dispatcharr_store::PluginActions::prepare(
        auth.clone(),
        std::env::var_os("DISPATCHARR_PLUGINS_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| "/data/plugins".into()),
    )
    .with_repository_schedule(plugin_schedule.clone());
    if plugin_actions.startup_discover(Arc::new(())).await.is_err() {
        eprintln!("Plugin discovery requires compatible runtime or unavailable startup resources");
    }
    if !selected_jobs_enabled
        && !std::env::var("DISPATCHARR_SKIP_PLUGIN_AUTODISCOVERY")
            .ok()
            .is_some_and(|value| {
                matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes")
            })
        && plugin_schedule.initialize_core(&auth).await.is_err()
    {
        eprintln!("Could not set up plugin repository refresh schedule");
    }
    let core_scheduled = if core_scheduled_jobs_enabled {
        Some(
            core_scheduled_jobs::CoreScheduledJobs::prepare(
                store.clone(),
                auth.clone(),
                cache_url.as_deref(),
                Arc::new(
                    realtime_bus
                        .as_ref()
                        .ok_or("Core scheduled jobs require the realtime publisher")?
                        .clone(),
                ),
                version_task_options(),
                core_plugin_refresh,
                core_provider_rehash,
                selected_jobs_enabled
                    || refresh_enabled
                    || version_tasks_enabled
                    || original_m3u_refresh_all.is_some(),
            )
            .await?,
        )
    } else {
        None
    };
    if let Some(cache) = cache_url
        .as_ref()
        .filter(|_| !selected_jobs_enabled && !core_scheduled_jobs_enabled)
    {
        if dispatcharr_job_transport::initialize_operator_backup_schedule(
            store.clone(),
            cache.clone(),
            Arc::new(()),
        )
        .await
        .is_err()
        {
            eprintln!("Backup schedule initialization did not settle");
        }
    }
    let core_compact = if selected_jobs_enabled {
        None
    } else if let Some(cache) = cache_url.as_ref() {
        let owner = Arc::new(
            dispatcharr_job_transport::compact_coordination::CompactCoordinator::lazy(
                cache.clone(),
            ),
        );
        auth.bind_compact_coordinator(owner.clone())
            .map_err(|_| "Core compact coordinator binding failed")?;
        Some(owner)
    } else {
        None
    };
    let core_repack = core_compact
        .as_ref()
        .map(|_| dispatcharr_store::xc_account_http::Repack::prepare(auth.clone()));
    let auth_routes = match &core_settings {
        Some(core) => auth
            .clone()
            .router_with_core_settings(core.clone())
            .map_err(|_| "Core settings must share the original Auth writer budget")?,
        None => auth.clone().router(),
    };
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|_| "Cannot bind HTTP listener")?;
    let selected_jobs = if selected_jobs_enabled {
        let bus = realtime_bus
            .as_ref()
            .ok_or("Selected jobs require the realtime publisher")?;
        let existing = selected_jobs::Existing {
            index: epg_index
                .clone()
                .ok_or("Selected Index preparation missing")?,
            refresh: epg_refresh
                .clone()
                .ok_or("Selected Refresh preparation missing")?,
            series: epg_series
                .clone()
                .ok_or("Selected Series preparation missing")?,
            artwork: epg_artwork
                .clone()
                .ok_or("Selected artwork preparation missing")?,
            tvg: epg_tvg.clone().ok_or("Selected TVG preparation missing")?,
            version: version_tasks
                .clone()
                .ok_or("Selected version preparation missing")?,
            completions: artwork_completions,
            backups: backup_files.clone(),
        };
        Some(
            selected_jobs::SelectedJobs::prepare(
                store.clone(),
                auth.clone(),
                Arc::new(bus.clone()),
                &engine,
                listener
                    .local_addr()
                    .map_err(|_| "Actual Main listener address unavailable")?,
                existing,
                vod_reads.scheduled_vod(),
                plugin_actions.clone(),
                #[cfg(target_os = "linux")]
                taskless_restore_helper,
                cache_url
                    .as_deref()
                    .ok_or("Selected jobs require Redis configuration")?,
                selected_beat,
            )
            .await?,
        )
    } else {
        None
    };
    let plugin_actions = match &selected_jobs {
        Some(selected) => plugin_actions.with_runtime(selected.plugin_runtime()),
        None => plugin_actions,
    };

    let backup_files = match (&selected_jobs, cache_url.as_deref()) {
        (Some(selected), _) => selected.backup_files(),
        (None, Some(cache)) => backup_files.with_schedule_disable(Arc::new(
            dispatcharr_job_transport::BackupScheduleDisable::new(
                store.clone(),
                cache.to_owned(),
                auth.producer_identity_source(),
            ),
        )),
        (None, None) => backup_files,
    };
    let backup_files = match &core_scheduled {
        Some(owner) if core_restore_grace.is_some() => {
            owner.bind_restore_plugins(plugin_actions.clone())?;
            backup_files
                .with_schedule_enable(Arc::new(owner.scheduler()))
                .with_create_restore_jobs(owner.backup_create_api())
        }
        Some(owner) => backup_files
            .with_schedule_enable(Arc::new(owner.scheduler()))
            .with_create_jobs(owner.backup_create_api()),
        None => backup_files,
    };
    let backup_files = match &manual_backup {
        Some(owner) if owner.is_restore_owner() => {
            owner
                .bind_restore_plugins(plugin_actions.clone())
                .map_err(|_| "Manual restore plugin reconciliation binding failed")?;
            backup_files.with_restore_jobs(Arc::new(owner.clone()))
        }
        Some(owner) => backup_files.with_create_jobs(Arc::new(owner.clone())),
        None => backup_files,
    };
    let ordinary_recording_playback = selected_jobs.is_none().then(|| {
        dispatcharr_store::RecordingPlayback::prepare(
            auth.clone(),
            dispatcharr_output_http::source_output_origin,
        )
    });
    let mut app = Router::new()
        .route("/health/ready", get(health))
        .with_state(HealthState {
            stream_files: auth.clone(),
            store,
            selected: selected_jobs.clone(),
            core_scheduled: core_scheduled.clone(),
            refresh_tvg: epg_tvg.clone().filter(|_| !selected_jobs_enabled),
        })
        .merge(auth_routes)
        .merge(drf_api_roots::router(auth.clone()))
        .merge(stream_regex.router())
        .merge(combined_statistics.router())
        .route("/proxy/stats", any(combined_stats_legacy_alias))
        .merge(output_profiles.clone().router())
        .merge(backup_files.clone().router())
        .merge(plugin_repositories.router())
        .merge(plugin_actions.clone().router())
        .merge(epg_programs.clone().router())
        .merge(epg_programs.clone().current_router())
        .merge(vod_reads.router())
        .merge(vod_reads.category_router())
        .merge(vod_reads.unified_router())
        .merge(vod_reads.provider_router())
        .merge(vod_reads.vod_image_router())
        .merge(grid.clone().router())
        .merge(hdhr.router())
        .merge(engine.controls_router(resolver.clone()))
        .merge(playback::router(
            engine.clone(),
            resolver,
            hls.clone(),
            fmp4.clone(),
        ));
    if let Some(refresh) = &epg_refresh {
        app = app.merge(refresh.clone().router());
    }
    if let Some(selected) = &selected_jobs {
        app = app.merge(selected.recordings_router());
        // Supply the actual prepared SD producer to existing provider routes.
        app = app.layer(axum::Extension(selected.sd_source_http()));
    } else if let Some(playback) = &ordinary_recording_playback {
        app = app.merge(playback.clone().router());
    }
    if let Some(repack) = &core_repack {
        app = app.merge(repack.clone().router());
    }
    if let Some(m3u) = &original_m3u_refresh_all {
        app = app.merge(m3u.router());
    }
    if let Some(gateway) = &realtime {
        app = app.merge(gateway.router());
    }
    if let Some(gateway) = &xc {
        app = app.merge(gateway.router()).merge(gateway.live_router());
    }
    if let Some(gateway) = &hls {
        app = app.merge(gateway.router());
    }
    if let Some(vod) = &vod {
        app = app.merge(vod.clone().router());
    }
    if let Some(sessions) = catchup_sessions {
        app = app.merge(sessions.router());
    }
    if let Some(media) = &catchup {
        app = app.merge(media.clone().router());
    }
    if let Some(management) = &catchup_management {
        app = app.merge(management.clone().router());
    }
    if let Some(output) = &output {
        app = app.merge(output.clone().router());
    }
    if let Some(xmltv) = &xmltv {
        app = app.merge(xmltv.clone().router());
    }
    // Source bare-profile redirects duplicate the profile and render React.
    app = app
        .route(
            "/proxy/vod/{kind}/{content_id}/{session_id}/{profile_id}",
            get(legacy_react_alias),
        )
        .route(
            "/proxy/vod/{kind}/{content_id}/{session_id}/{profile_id}/{legacy_profile_id}/",
            get(legacy_react_alias),
        );
    // Backend paths must keep an HTTP error response rather than receiving
    // the React navigation fallback when a route has not been migrated yet.
    for prefix in ["api", "proxy", "output"] {
        app = app
            .route(&format!("/{prefix}"), any(api_not_found))
            .route(&format!("/{prefix}/{{*path}}"), any(api_not_found));
    }
    if let Ok(path) = env::var("RUST_FRONTEND_DIR") {
        let root = std::path::Path::new(&path);
        let index = static_assets::index(root)?;
        // Actual static directories take precedence over the original short
        // three-component XC credential alias, including nested font assets.
        app = static_assets::with_directories(app, root).await?;
        app = app.fallback_service(
            tower_http::services::ServeDir::new(path)
                .fallback(tower_http::services::ServeFile::new(index.clone())),
        );
        // Django converter nonmatches fall through to its final React TemplateView.
        // Classify before the broad native VOD handlers can authenticate/resolve.
        app = vod_react_fallback::with_index(app, index);
    }
    app = app.layer(middleware::from_fn_with_state(
        admin_ingress::AdminIngress {
            xc_live: xc.is_some(),
            location: admin_location,
        },
        admin_ingress::apply,
    ));
    if let (Some(gateway), Some(vod)) = (&xc, &vod) {
        app = gateway.with_vod_playback(app, vod.clone());
    }
    app = app.layer(axum::middleware::from_fn(response_headers::apply));
    eprintln!("Dispatcharr Rust port listening on {bind}; migration parity is incomplete");
    let manual_restore_grace = manual_backup
        .as_ref()
        .filter(|owner| owner.is_restore_owner())
        .map(|_| manual_backup_config::restore_drain_grace(shutdown_timeout))
        .transpose()?;
    if let Some(version) = version_tasks.as_ref().filter(|_| !selected_jobs_enabled) {
        let started = if version_startup {
            let cache = cache_url
                .as_deref()
                .ok_or("Enabled version tasks require Redis configuration")?;
            version
                .start_with_startup(cache, auth.producer_identity_source())
                .await
        } else {
            version.start().map_err(|_| "Version task intake failed")
        };
        if let Err(error) = started {
            let _ = version.shutdown(Duration::from_secs(4)).await;
            return Err(error.into());
        }
    }
    let preready_stop = match &mut selected_stop {
        Some(signal) => signal.pending().await,
        None => false,
    };
    if !preready_stop {
        if let Some(selected) = &selected_jobs {
            selected.start_after_serving()?;
        }
        if let Some(owner) = &manual_backup {
            owner.start().map_err(|_| "Manual backup intake failed")?;
        }
        if let Some(owner) = &core_scheduled {
            owner.start(core_plugin_startup).await?;
        }
    }
    let manual_backup_enabled = manual_backup.is_some();
    let (shutdown_started, mut shutdown_notice) = tokio::sync::oneshot::channel();
    let services = ShutdownServices {
        stream_files: auth.clone(),
        stream_regex: stream_regex.clone(),
        plugin_actions,
        core_settings,
        selected_jobs,
        core_scheduled,
        core_restore_grace,
        core_compact: core_compact.clone(),
        core_repack: core_repack.clone(),
        ordinary_recording_playback: ordinary_recording_playback.clone(),
        original_m3u_refresh_all: original_m3u_refresh_all.clone(),
        live_client_connect: live_client_connect.clone(),
        engine,
        clients,
        hls,
        fmp4,
        vod,
        catchup,
        catchup_management,
        realtime,
        output,
        xmltv,
        xc,
        output_profiles,
        backup_files,
        manual_backup,
        manual_restore_grace,
        epg_programs,
        epg_refresh,
        epg_index,
        epg_series,
        epg_artwork,
        epg_tvg,
        version_tasks,
        vod_reads,
        grid,
        hdhr,
        live_statistics,
        combined_statistics,
    };
    if preready_stop {
        drop(listener);
        drop(app);
        stop_service_admission(&services);
        return tokio::time::timeout(shutdown_timeout, shutdown_components(services))
            .await
            .map_err(|_| {
                "Selected preready shutdown deadline expired; physical service drain is unverified"
                    .to_owned()
            })?;
    }
    let signal_services = services.clone();
    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        match selected_stop {
            Some(mut signal) => signal.wait().await,
            None => shutdown_signal().await,
        }
        stop_service_admission(&signal_services);
        let cleanup = tokio::spawn(shutdown_components(signal_services));
        let _ = shutdown_started.send(cleanup);
        // Return immediately: Axum stops accepting while cleanup runs.
    })
    .into_future();
    tokio::pin!(server);
    let shutdown_mode = if refresh_enabled && !selected_jobs_enabled {
        "Refresh worker"
    } else if original_m3u_refresh_all.is_some() {
        "Original M3U publisher"
    } else if manual_backup_enabled {
        "Manual backup worker"
    } else if core_scheduled_jobs_enabled {
        "Core scheduled jobs"
    } else {
        "Selected"
    };
    let shutdown_requires_drain = || {
        refresh_enabled
            || original_m3u_refresh_all.is_some()
            || manual_backup_enabled
            || core_scheduled_jobs_enabled
            || core_compact.as_ref().is_some_and(|owner| owner.was_used())
            || core_repack
                .as_ref()
                .is_some_and(dispatcharr_store::xc_account_http::Repack::was_used)
            || auth.stream_files_were_used()
            || live_client_connect.was_used()
            || stream_regex.was_used()
            || ordinary_recording_playback
                .as_ref()
                .is_some_and(dispatcharr_store::RecordingPlayback::was_used)
    };
    tokio::select! {
        result=&mut server=>{
            let cleanup=tokio::time::timeout(shutdown_timeout,finish_shutdown_after_server(&mut shutdown_notice,shutdown_components(services),shutdown_mode)).await;
            if shutdown_requires_drain() {match cleanup{Ok(result)=>result?,Err(_)=>return Err(format!("{shutdown_mode} shutdown deadline expired; process termination may close outstanding resources"))}}
            result.map_err(|_|"HTTP server failed".to_string())
        },
        cleanup=&mut shutdown_notice=>{
            let mut cleanup=cleanup.map_err(|_|"Shutdown coordination failed")?;
            let drained=tokio::time::timeout(shutdown_timeout,async{
                let result=(&mut server).await;
                let cleaned=(&mut cleanup).await;
                (result,cleaned)
            }).await;
            match drained{
                Ok((result,cleaned))=>{
                    if shutdown_requires_drain() {cleaned.map_err(|_|format!("{shutdown_mode} shutdown supervisor failed; process termination may close outstanding resources"))??;}
                    result.map_err(|_|"HTTP server failed".to_string())
                },
                Err(_)=>{cleanup.abort();if shutdown_requires_drain() {Err(format!("{shutdown_mode} shutdown deadline expired; cleanup was cancelled and process termination may close outstanding resources"))}else{eprintln!("Shutdown deadline reached; closing remaining HTTP connections");Ok(())}},
            }
        }
    }
}

// Axum may complete in the same poll that its graceful callback publishes
// cleanup. The server-ready select branch must join that actual owner, never
// run the shared physical service lifecycles a second time.
async fn finish_shutdown_after_server<F>(
    notice: &mut tokio::sync::oneshot::Receiver<tokio::task::JoinHandle<Result<(), String>>>,
    fallback: F,
    shutdown_mode: &str,
) -> Result<(), String>
where
    F: std::future::Future<Output = Result<(), String>>,
{
    match notice.try_recv() {
        Ok(cleanup) => cleanup
            .await
            .map_err(|_| format!("{shutdown_mode} shutdown supervisor failed; process termination may close outstanding resources"))?,
        // A completed server can no longer poll the graceful callback. An empty
        // or closed notice therefore means no published cleanup owner exists.
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        | Err(tokio::sync::oneshot::error::TryRecvError::Closed) => fallback.await,
    }
}

fn stop_service_admission(services: &ShutdownServices) {
    services.stream_files.stop_stream_file_admission();
    services.stream_regex.close_admission();
    services.plugin_actions.stop_admission();
    if let Some(repack) = &services.core_repack {
        repack.close_admission();
    }
    if let Some(owner) = &services.core_compact {
        owner.close_admission();
    }
    if let Some(owner) = &services.core_scheduled {
        owner.stop_admission();
    }
    if let Some(owner) = &services.manual_backup {
        owner.stop_admission();
    }
    if let Some(m3u) = &services.original_m3u_refresh_all {
        m3u.close_admission();
    }
    if let Some(playback) = &services.ordinary_recording_playback {
        playback.close_admission();
    }
    if let Some(selected) = &services.selected_jobs {
        selected.stop_producers();
    } else {
        if let Some(tvg) = &services.epg_tvg {
            tvg.stop_channel_publications();
        }
        services.engine.shutdown();
    }
}

// One already registered signal future crosses Selected preparation and the
// runtime handoff. A checkpoint may poll it once but cannot replace or drop
// the pending receiver. The normal HTTP shutdown uses the same instance.
struct LatchedShutdown {
    signal: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    observed: bool,
}

impl LatchedShutdown {
    fn from_signal(signal: impl std::future::Future<Output = ()> + Send + 'static) -> Self {
        Self {
            signal: Box::pin(signal),
            observed: false,
        }
    }

    #[cfg(unix)]
    fn register() -> Result<Self, String> {
        use tokio::signal::unix::{signal, SignalKind};
        let mut terminate = signal(SignalKind::terminate())
            .map_err(|_| "Selected startup SIGTERM handler installation failed")?;
        let mut interrupt = signal(SignalKind::interrupt())
            .map_err(|_| "Selected startup SIGINT handler installation failed")?;
        Ok(Self::from_signal(async move {
            tokio::select! {
                _ = terminate.recv() => {},
                _ = interrupt.recv() => {},
            }
        }))
    }

    #[cfg(not(unix))]
    fn register() -> Result<Self, String> {
        Ok(Self::from_signal(async {
            let _ = tokio::signal::ctrl_c().await;
        }))
    }

    async fn wait(&mut self) {
        if !self.observed {
            self.signal.as_mut().await;
            self.observed = true;
        }
    }

    async fn pending(&mut self) -> bool {
        tokio::select! {
            biased;
            _ = self.wait() => true,
            _ = std::future::ready(()) => false,
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {_=terminate.recv()=>{},_=tokio::signal::ctrl_c()=>{}}
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
#[derive(Clone)]
struct ShutdownServices {
    stream_files: AuthService,
    stream_regex: dispatcharr_store::StreamRegexPreview,
    plugin_actions: dispatcharr_store::PluginActions,
    core_settings: Option<dispatcharr_store::CoreSettingsWrites>,
    selected_jobs: Option<selected_jobs::SelectedJobs>,
    core_scheduled: Option<core_scheduled_jobs::CoreScheduledJobs>,
    core_restore_grace: Option<Duration>,
    core_compact: Option<Arc<dispatcharr_job_transport::compact_coordination::CompactCoordinator>>,
    core_repack: Option<dispatcharr_store::xc_account_http::Repack>,
    ordinary_recording_playback: Option<dispatcharr_store::RecordingPlayback>,
    original_m3u_refresh_all: Option<dispatcharr_job_transport::transport::OriginalM3uRefreshAll>,
    engine: StreamEngine,
    live_client_connect: Arc<dispatcharr_store::LiveClientConnect>,
    clients: Option<ClientStore>,
    hls: Option<dispatcharr_hls_http::HlsGateway>,
    fmp4: Option<dispatcharr_fmp4_http::Fmp4Gateway>,
    vod: Option<dispatcharr_vod_http::VodEngine>,
    catchup: Option<dispatcharr_timeshift_http::CatchupMedia>,
    catchup_management: Option<dispatcharr_timeshift_http::CatchupManagement>,
    realtime: Option<dispatcharr_realtime::RealtimeGateway>,
    output: Option<dispatcharr_output_http::OutputGateway>,
    xmltv: Option<dispatcharr_xmltv_http::gateway::Gateway>,
    xc: Option<dispatcharr_xc_http::XcGateway>,
    output_profiles: dispatcharr_store::OutputProfileWrites,
    backup_files: dispatcharr_store::BackupFiles,
    manual_backup: Option<dispatcharr_job_transport::normal_backup::ManualApplication>,
    manual_restore_grace: Option<Duration>,
    epg_programs: dispatcharr_store::EpgProgramReads,
    epg_refresh: Option<dispatcharr_job_transport::refresh_application::RefreshApplication>,
    epg_index: Option<dispatcharr_job_transport::index_application::IndexApplication>,
    epg_series: Option<dispatcharr_job_transport::configured_series_application::Application>,
    epg_artwork:
        Option<dispatcharr_job_transport::recording_existing_artwork_application::Application>,
    epg_tvg: Option<dispatcharr_job_transport::tvg_programme_application::Application>,
    version_tasks: Option<dispatcharr_job_transport::version_application::Application>,
    vod_reads: dispatcharr_store::VodReads,
    grid: dispatcharr_output_http::GridGateway,
    hdhr: dispatcharr_hdhr_http::HdhrGateway,
    live_statistics: Option<Arc<dispatcharr_realtime::LiveStatistics>>,
    combined_statistics: combined_statistics::CombinedStatistics,
}
async fn shutdown_components(services: ShutdownServices) -> Result<(), String> {
    let ShutdownServices {
        stream_files,
        stream_regex,
        plugin_actions,
        core_settings,
        selected_jobs,
        core_scheduled,
        core_restore_grace,
        core_compact,
        core_repack,
        ordinary_recording_playback,
        original_m3u_refresh_all,
        engine,
        live_client_connect,
        clients,
        hls,
        fmp4,
        vod,
        catchup,
        catchup_management,
        realtime,
        output,
        xmltv,
        xc,
        output_profiles,
        backup_files,
        manual_backup,
        manual_restore_grace,
        epg_programs,
        mut epg_refresh,
        mut epg_index,
        mut epg_series,
        mut epg_artwork,
        mut epg_tvg,
        mut version_tasks,
        vod_reads,
        grid,
        hdhr,
        live_statistics,
        combined_statistics,
    } = services;
    if let Some(owner) = &manual_backup {
        owner.stop_admission();
    }
    let core_scheduled_mode = core_scheduled.is_some();
    let core_scheduled_shutdown = match core_scheduled {
        Some(ref owner) => {
            let result = owner
                .shutdown(core_restore_grace.unwrap_or(Duration::from_secs(12)))
                .await;
            if core_restore_grace.is_some() {
                result?;
                Ok(())
            } else {
                result
            }
        }
        None => Ok(()),
    };
    if selected_jobs.is_none() {
        if let Some(tvg) = &epg_tvg {
            tvg.stop_channel_publications();
        }
    }
    if let Some(playback) = &ordinary_recording_playback {
        playback.close_admission();
    }
    if let Some(m3u) = &original_m3u_refresh_all {
        m3u.close_admission();
    }
    if let Some(repack) = &core_repack {
        repack.close_admission();
    }
    if let Some(owner) = &core_compact {
        owner.close_admission();
    }
    let (
        stream_files_drained,
        stream_regex_drained,
        original_m3u_drained,
        manual_restore_drained,
        repack_drained,
        compact_drained,
    ) = tokio::join!(
        stream_files.shutdown_stream_files(Duration::from_secs(8)),
        stream_regex.shutdown(Duration::from_secs(8)),
        async {
            match original_m3u_refresh_all {
                Some(m3u) => m3u.shutdown(Duration::from_secs(8)).await,
                None => true,
            }
        },
        async {
            match manual_restore_grace {
                Some(grace) => backup_files.shutdown(grace).await,
                None => true,
            }
        },
        async {
            match core_repack {
                Some(repack) => repack.shutdown(Duration::from_secs(8)).await,
                None => true,
            }
        },
        async {
            match core_compact {
                Some(owner) => owner.shutdown(Duration::from_secs(8)).await,
                None => true,
            }
        },
    );
    if !repack_drained {
        return Err("Core manual repack HTTP and SQL ownership did not settle".into());
    }
    if !compact_drained {
        return Err("Core compact numbering ownership did not settle".into());
    }
    if !manual_restore_drained {
        return Err("Manual restore worker or accepted restore did not settle cleanly".into());
    }
    plugin_actions
        .shutdown(Duration::from_secs(8))
        .await
        .map_err(|_| "Plugin filesystem/SQL/reader drain incomplete; physical owners remain")?;
    let selected_mode = selected_jobs.is_some();
    if let Some(selected) = selected_jobs {
        selected.stop_producers();
        if !selected.shutdown(selected.shutdown_budget()).await {
            return Err("Selected worker drain is incomplete; process termination may close outstanding resources".into());
        }
        // The normal graph owned these actual family lifecycles/ports. Legacy
        // pollers were never started, so do not close the same ports twice.
        epg_refresh = None;
        epg_index = None;
        epg_series = None;
        epg_artwork = None;
        epg_tvg = None;
        version_tasks = None;
    }
    engine.shutdown();
    combined_statistics.close_admission();
    // Refresh reporter work retains the realtime publisher until actual drain.
    // A bounded incomplete drain is reported honestly before other shutdowns.
    // Version work retains SQL, HTTP runtime and realtime owners through its
    // actual completion; a bounded timeout is reported without closing Jobs.
    let version_drained = match version_tasks {
        Some(version) => version.shutdown(Duration::from_secs(4)).await,
        None => true,
    };
    if !version_drained {
        eprintln!("Version task remains physically unsettled");
    }
    // Series can still be reading a completed Refresh parent. Stop and settle
    // that intake before Refresh shutdown closes the shared parent reader.
    if let Some(artwork) = &epg_artwork {
        artwork.close_producer_admission();
    }
    let series_parent_reads_drained = match &epg_series {
        Some(series) => series.drain_intake(Duration::from_secs(4)).await,
        None => true,
    };
    if !series_parent_reads_drained {
        eprintln!("Configured Series parent reads remain physically unsettled");
    }
    let artwork_drained = if series_parent_reads_drained {
        match epg_artwork {
            Some(artwork) => artwork.shutdown(Duration::from_secs(4)).await.is_ok(),
            None => true,
        }
    } else {
        false
    };
    if !artwork_drained {
        eprintln!("Recording artwork remains physically unsettled");
    }
    let (refresh_drained, index_intake_drained, series_drained, tvg_drained) = tokio::join!(
        async {
            match epg_refresh {
                Some(refresh) => refresh.shutdown(Duration::from_secs(4)).await,
                None => true,
            }
        },
        async {
            match &epg_index {
                Some(index) => index.drain_intake(Duration::from_secs(4)).await,
                None => true,
            }
        },
        async {
            match epg_series {
                Some(series) => series.shutdown(Duration::from_secs(4)).await.is_ok(),
                None => true,
            }
        },
        async {
            match epg_tvg {
                Some(tvg) => tvg.shutdown(Duration::from_secs(4)).await,
                None => true,
            }
        },
    );
    if !tvg_drained {
        eprintln!("Per-channel EPG worker remains physically unsettled");
    }
    let (
        live,
        _,
        _,
        vod,
        catchup,
        management,
        realtime,
        output,
        xmltv,
        xc,
        profiles,
        backups,
        programs,
        grid,
        hdhr,
        vod_read_drained,
        combined_stats_drained,
    ) = tokio::join!(
        engine.shutdown_wait(Duration::from_secs(8)),
        async {
            if let Some(hls) = hls {
                hls.shutdown().await;
            }
        },
        async {
            if let Some(fmp4) = fmp4 {
                fmp4.shutdown().await;
            }
        },
        async {
            if let Some(vod) = vod {
                vod.shutdown().await
            } else {
                true
            }
        },
        async {
            if let Some(media) = catchup {
                media.shutdown().await
            } else {
                true
            }
        },
        async {
            if let Some(management) = catchup_management {
                management.shutdown().await
            } else {
                true
            }
        },
        async {
            if let Some(gateway) = realtime {
                gateway.shutdown().await
            } else {
                true
            }
        },
        async {
            if let Some(output) = output {
                output.shutdown().await
            } else {
                true
            }
        },
        async {
            if let Some(xmltv) = xmltv {
                xmltv.shutdown().await
            } else {
                true
            }
        },
        async {
            if let Some(xc) = xc {
                xc.shutdown().await
            } else {
                true
            }
        },
        output_profiles.shutdown(Duration::from_secs(8)),
        async {
            if manual_restore_grace.is_some() {
                true
            } else {
                backup_files.shutdown(Duration::from_secs(8)).await
            }
        },
        epg_programs.shutdown(Duration::from_secs(8)),
        grid.shutdown(Duration::from_secs(8)),
        hdhr.shutdown(),
        vod_reads.shutdown(Duration::from_secs(8)),
        combined_statistics.shutdown(Duration::from_secs(8)),
    );
    let ordinary_recording_playback_drained = match ordinary_recording_playback {
        Some(playback) => playback.shutdown(Duration::from_secs(8)).await,
        None => true,
    };
    let core_settings_drained = match core_settings {
        Some(core) => core.shutdown().await,
        None => true,
    };
    // Accepted HTTP reads have now drained or explicitly timed out. Close the
    // file/publication services last; late missing-index publications remain
    // durable for the next startup rather than losing their transport owner.
    let index_drained = match epg_index {
        Some(index) => index.shutdown(Duration::from_secs(4)).await,
        None => true,
    };
    let incomplete = !core_settings_drained
        || !combined_stats_drained
        || !refresh_drained
        || !index_intake_drained
        || !series_drained
        || !index_drained
        || !live
        || !vod
        || !catchup
        || !management
        || !realtime
        || !output
        || !xmltv
        || !xc
        || !profiles
        || !backups
        || !programs
        || !grid
        || !hdhr
        || !vod_read_drained;
    if incomplete {
        eprintln!("Service drain deadline reached; pending reservations require recovery");
    }
    if let Some(clients) = clients {
        clients.shutdown().await;
    }
    let statistics_drained = match live_statistics {
        Some(statistics) => statistics.shutdown(Duration::from_millis(500)).await,
        None => true,
    };
    if !statistics_drained {
        eprintln!(
            "Statistics drain deadline reached; process termination may close pending callbacks"
        );
    }
    if !live && live_client_connect.was_used() {
        return Err("Live Connect drain incomplete".into());
    }
    core_scheduled_shutdown?;
    if (selected_mode || manual_restore_grace.is_some() || core_scheduled_mode)
        && (incomplete || !statistics_drained)
    {
        return Err(
            "Service drain incomplete; process termination may close outstanding resources".into(),
        );
    }
    if !tvg_drained {
        return Err("Per-channel EPG worker and publisher remain physically unsettled".into());
    }
    if !ordinary_recording_playback_drained {
        return Err(
            "Recording playback files or response bodies remain physically unsettled".into(),
        );
    }
    if !stream_regex_drained {
        return Err("Stream regex preview dependency remains physically unsettled".into());
    }
    if !original_m3u_drained {
        return Err("Original M3U aggregate broker publication remains unsettled".into());
    }
    if manual_backup.is_some() && !backups {
        return Err(
            "Manual backup consumer or accepted archive work did not settle cleanly".into(),
        );
    }
    if !stream_files_drained {
        return Err("Stream upload storage/SQL remains physically unsettled".into());
    }
    Ok(())
}

#[tokio::main]
async fn serve(logs: &managed_logs::ManagedLogs) -> Result<(), String> {
    #[cfg(feature = "sqlite-copied-data-runtime")]
    if std::env::args_os()
        .skip(1)
        .eq([std::ffi::OsString::from("--sqlite-copied-data-runtime")])
    {
        return sqlite_copied_runtime::run().await;
    }
    plugin_gpg_trace::install_from_environment()?;
    run(logs).await
}

fn main() -> std::process::ExitCode {
    let logs = managed_logs::ManagedLogs::start();
    match serve(&logs) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Dispatcharr Rust startup failed: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod version_startup_tests {
    use super::version_startup_owned;
    use std::ffi::OsStr;

    #[test]
    fn ordinary_version_startup_requires_explicit_exclusive_config() {
        let owner = Some(OsStr::new("rust"));
        let broker = Some(OsStr::new("redis://owned/1"));
        let backend = Some(OsStr::new("redis://owned/2"));

        assert_eq!(
            version_startup_owned(None, false, false, false, None, None),
            Ok(false)
        );
        assert_eq!(
            version_startup_owned(None, true, true, true, None, None),
            Ok(false)
        );
        assert_eq!(
            version_startup_owned(owner, true, false, false, broker, backend),
            Ok(true)
        );
        for (version, selected, refresh) in [
            (false, false, false),
            (true, true, false),
            (true, false, true),
        ] {
            assert!(
                version_startup_owned(owner, version, selected, refresh, broker, backend).is_err()
            );
        }
        assert!(version_startup_owned(owner, true, false, false, None, backend).is_err());
        assert!(version_startup_owned(owner, true, false, false, broker, None).is_err());
        assert!(version_startup_owned(
            Some(OsStr::new("unknown")),
            true,
            false,
            false,
            broker,
            backend
        )
        .is_err());
    }
}

#[cfg(test)]
mod shutdown_owner_tests {
    use super::{finish_shutdown_after_server, LatchedShutdown};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[tokio::test]
    async fn selected_signal_keeps_the_receiver_across_the_startup_checkpoint() {
        let (request, received) = tokio::sync::oneshot::channel();
        let mut signal = LatchedShutdown::from_signal(async move { received.await.unwrap() });
        assert!(!signal.pending().await);
        request.send(()).unwrap();
        assert!(signal.pending().await);
        // The startup path does not re-poll a completed receive future.
        signal.wait().await;
        assert!(signal.pending().await);
    }

    #[tokio::test]
    async fn selected_runtime_wait_inherits_the_preready_receiver() {
        let (request, received) = tokio::sync::oneshot::channel();
        let mut signal = LatchedShutdown::from_signal(async move { received.await.unwrap() });
        assert!(!signal.pending().await);
        request.send(()).unwrap();
        signal.wait().await;
        assert!(signal.pending().await);
    }

    // Force the same race as actual Main: Axum's graceful callback has sent
    // the real cleanup JoinHandle and returned, so BOTH outer select branches
    // are ready. Force server-completion selection and use the production join.
    #[tokio::test]
    async fn completed_server_joins_same_signal_cleanup_success_and_failure() {
        for failure in [false, true] {
            let calls = Arc::new(AtomicUsize::new(0));
            let owner = calls.clone();
            let (started, mut notice) = tokio::sync::oneshot::channel();
            let cleanup = tokio::spawn(async move {
                owner.fetch_add(1, Ordering::SeqCst);
                if failure {
                    Err("actual cleanup failure".to_owned())
                } else {
                    Ok(())
                }
            });
            started.send(cleanup).unwrap();
            let result = tokio::select! {
                biased;
                _ = std::future::ready(()) => {
                    finish_shutdown_after_server(&mut notice, async {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err("duplicate cleanup must not execute".to_owned())
                    }, "Selected").await
                },
                _ = &mut notice => panic!("test must select the completed-server branch"),
            };
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                result,
                if failure {
                    Err("actual cleanup failure".to_owned())
                } else {
                    Ok(())
                }
            );
        }
    }

    #[tokio::test]
    async fn server_completion_waits_for_published_physical_owner() {
        let (release, wait) = tokio::sync::oneshot::channel();
        let (started, mut notice) = tokio::sync::oneshot::channel();
        started
            .send(tokio::spawn(async {
                wait.await.unwrap();
                Ok(())
            }))
            .unwrap();
        let finishing = finish_shutdown_after_server(
            &mut notice,
            async { panic!("an existing cleanup owner cannot be replaced") },
            "Selected",
        );
        tokio::pin!(finishing);
        tokio::select! {
            biased;
            _ = &mut finishing => panic!("logical server completion is not physical drain"),
            _ = tokio::task::yield_now() => {},
        }
        release.send(()).unwrap();
        assert_eq!(finishing.await, Ok(()));
    }

    #[tokio::test]
    async fn unsignaled_server_failure_starts_fallback_cleanup() {
        for closed in [false, true] {
            let (started, mut notice) = tokio::sync::oneshot::channel();
            let sender = if closed {
                drop(started);
                None
            } else {
                Some(started)
            };
            let calls = AtomicUsize::new(0);
            let result = finish_shutdown_after_server(
                &mut notice,
                async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err("actual fallback failure".to_owned())
                },
                "Selected",
            )
            .await;
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(result, Err("actual fallback failure".to_owned()));
            drop(sender);
        }
    }
}
