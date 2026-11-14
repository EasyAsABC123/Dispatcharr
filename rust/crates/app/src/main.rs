//! Derivative Dispatcharr backend port. Original project credit: ATTRIBUTION.md.
mod combined_statistics;
mod combined_statistics_html;
mod combined_statistics_wire;
mod control;
mod playback;
mod plugin_gpg_trace;
mod proxy_health;
mod realtime;
mod response_headers;
mod static_assets;
mod statistics;
mod vod_react_fallback;
use axum::{
    extract::State,
    http::{header, Method, StatusCode},
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

fn database_url() -> Result<String, String> {
    if let Ok(value) = env::var("DATABASE_URL") {
        return Ok(value);
    }
    let mut url = url::Url::parse("postgresql://localhost/dispatcharr").unwrap();
    url.set_host(Some(
        &env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".into()),
    ))
    .map_err(|_| "Invalid POSTGRES_HOST")?;
    url.set_port(Some(
        env::var("POSTGRES_PORT")
            .unwrap_or_else(|_| "5432".into())
            .parse()
            .map_err(|_| "Invalid POSTGRES_PORT")?,
    ))
    .map_err(|_| "Invalid POSTGRES_PORT")?;
    url.set_username(&env::var("POSTGRES_USER").unwrap_or_else(|_| "dispatch".into()))
        .map_err(|_| "Invalid POSTGRES_USER")?;
    url.set_password(Some(
        &env::var("POSTGRES_PASSWORD").map_err(|_| "POSTGRES_PASSWORD is required")?,
    ))
    .map_err(|_| "Invalid POSTGRES_PASSWORD")?;
    url.set_path(&env::var("POSTGRES_DB").unwrap_or_else(|_| "dispatcharr".into()));
    Ok(url.into())
}

async fn health(State(store): State<Store>) -> (StatusCode, Json<serde_json::Value>) {
    // Readiness verifies the migrated schema as well as database connectivity.
    match store.setting("proxy_settings").await {
        Ok(_) => (
            StatusCode::OK,
            Json(serde_json::json!({"status":"ready","backend":"rust","parity_complete":false})),
        ),
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

async fn run() -> Result<(), String> {
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
    let auth = AuthService::new(store.clone(), auth_config);
    let auth = if let Ok(url) = env::var("REDIS_URL") {
        auth.with_notification_cache(
            dispatcharr_store::NotificationCache::connect(&url)
                .await
                .map_err(|_| "Notification cache initialization failed (credentials omitted)")?,
        )
    } else {
        auth
    };
    // Original startup warms the system group before HTTP, leaving the
    // display-zone debounce untouched until the first real request.
    auth.startup_system_settings_group().await;
    let realtime_bus = realtime::bus().await?;
    // Attach before every resolver/environment/XC clone so original producers
    // publish through the same bounded process transport.
    let auth = if let Some(bus) = &realtime_bus {
        auth.with_event_sink(Arc::new(bus.clone()))
    } else {
        auth
    };
    // Temporary migration capability while exceptional settings shapes and
    // live Main transitions remain outside the measured numeric Source cohort.
    let dynamic_health = match env::var("RUST_DYNAMIC_PROXY_HEALTH_ENABLED").as_deref() {
        Ok("1") => true,
        Ok("0") | Err(_) => false,
        _ => return Err("RUST_DYNAMIC_PROXY_HEALTH_ENABLED must be 0 or 1".into()),
    };
    // Construct from pre-engine AuthService to avoid a Core -> EngineControl ->
    // engine -> health-source -> Core cycle. All share the original four slots.
    let proxy_health_core = if dynamic_health {
        let effects = dispatcharr_store::CoreSettingsSpecialEffects::new(std::path::PathBuf::from(
            env::var("DISPATCHARR_LOG_DIR").unwrap_or_else(|_| "/data/logs".into()),
        ))
        .map_err(|_| "Invalid proxy health effects directory")?;
        Some(
            dispatcharr_store::CoreSettingsWrites::new(auth.clone())
                .and_then(|core| core.with_special_effects(effects))
                .map_err(|_| {
                    "Dynamic proxy health requires the shared cache and realtime publisher"
                })?,
        )
    } else {
        None
    };
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
    let proxy_settings = store
        .setting("proxy_settings")
        .await
        .map_err(|_| "Proxy settings unavailable")?;
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
    let input_config = InputConfig {
        max_chunk_bytes: config.chunk_bytes,
        max_channel_bytes: config.retained_bytes_per_channel,
        max_chunks: positive("RUST_INPUT_MAX_CHUNKS", 4096)?,
        max_read_bytes: positive("RUST_INPUT_READ_BYTES", 64 * 1024 * 1024)?,
        max_operations: positive("RUST_INPUT_OPERATIONS", 16)?,
        chunk_ttl: config.retention,
        preserve_source_era: config.source_initialization,
        ..Default::default()
    };
    let lease_config = LeaseConfig::default();
    let (coordinator, clients, outputs, inputs) = if let Ok(url) = env::var("REDIS_URL") {
        let coordinator = Coordinator::connect(&url, lease_config.clone())
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
        let clients = ClientStore::connect(&url, client_config)
            .await
            .map_err(|_| "Redis client metadata unavailable (credentials omitted)")?;
        let outputs = OutputStore::connect(&url, output_config)
            .await
            .map_err(|_| "Redis output storage unavailable (credentials omitted)")?;
        let inputs = InputStore::connect(&url, input_config)
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
            .map_err(|_| "Invalid streaming configuration")?;
    let engine = match &proxy_health_core {
        Some(core) => engine
            .with_health_settings(Arc::new(proxy_health::CoreProxyHealthSource::new(
                core.clone(),
            )))
            .map_err(|_| "Dynamic proxy health must be bound before engine sharing")?,
        None => engine,
    };
    let live_statistics = statistics::start(&engine, clients.as_ref(), realtime_bus.as_ref())?;
    let auth = auth.with_channel_control(Arc::new(control::EngineControl(engine.clone())));
    let realtime = realtime::gateway(auth.clone(), realtime_bus.clone())?;
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
        let redis_url = env::var("REDIS_URL").map_err(|_| "Redis VOD configuration missing")?;
        let engine = dispatcharr_vod_http::VodEngine::new(
            store.clone(),
            auth.clone(),
            coordinator,
            &redis_url,
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
                    &redis_url,
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
        let redis_url =
            env::var("REDIS_URL").map_err(|_| "Redis catch-up configuration missing")?;
        let sessions = dispatcharr_timeshift_http::CatchupSessions::new(
            store.clone(),
            auth.clone(),
            &redis_url,
            dispatcharr_timeshift_http::SessionConfig::default(),
        )
        .await
        .map_err(|_| "Catch-up sessions initialization failed (credentials omitted)")?;
        let media = dispatcharr_timeshift_http::CatchupMedia::new(
            store.clone(),
            auth.clone(),
            sessions.clone(),
            coordinator,
            &redis_url,
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
                .with_events(store.clone(), &redis_url, Arc::new(bus.clone()))
                .await
                .map_err(|_| "Catch-up event initialization failed (credentials omitted)")?
        } else {
            media
        };
        let management = dispatcharr_timeshift_http::CatchupManagement::new(
            store.clone(),
            auth.clone(),
            media.clone(),
            &redis_url,
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
    let output = if let Ok(redis_url) = env::var("REDIS_URL") {
        Some(
            dispatcharr_output_http::OutputGateway::new(
                store.clone(),
                auth.clone(),
                &redis_url,
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
    let xmltv = if let Ok(redis_url) = env::var("REDIS_URL") {
        Some(
            dispatcharr_xmltv_http::gateway::Gateway::new(
                store.clone(),
                auth.clone(),
                &redis_url,
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
    let xc = if let Ok(redis_url) = env::var("REDIS_URL") {
        let counts = dispatcharr_xc_http::counts::ConnectionCounts::connect_source(&redis_url)
            .await
            .map_err(|_| "XC counter initialization failed (credentials omitted)")?;
        Some(
            dispatcharr_xc_http::XcGateway::new(
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
            .with_vod_lists(),
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
    let profile_redis = env::var("REDIS_URL").ok();
    let output_profiles =
        dispatcharr_store::OutputProfileWrites::new(auth.clone(), profile_redis.as_deref())
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
    .map_err(|_| "Invalid backup file API configuration")?;
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
    let version_tasks_enabled = match env::var("RUST_VERSION_TASKS_ENABLED").as_deref() {
        Ok("0") | Err(_) => false,
        Ok("1") => true,
        _ => return Err("RUST_VERSION_TASKS_ENABLED must be 0 or 1".into()),
    };
    let version_tasks = if version_tasks_enabled {
        let cache = env::var("REDIS_URL").map_err(|_| "Enabled version tasks require REDIS_URL")?;
        let bus = realtime_bus
            .as_ref()
            .ok_or("Enabled version tasks require the realtime publisher")?;
        let broker = env::var("CELERY_BROKER_URL").unwrap_or_else(|_| cache.clone());
        let backend = env::var("CELERY_RESULT_BACKEND").unwrap_or_else(|_| broker.clone());
        let endpoint = env::var("RUST_VERSION_HTTP_ORIGIN")
            .map(|origin| format!("{}/owned-version", origin.trim_end_matches('/')))
            .unwrap_or_else(|_| {
                "https://api.github.com/repos/Dispatcharr/Dispatcharr/releases/latest".into()
            });
        // Coupled to the pinned Original version.py stable build. Development
        // timestamp checks require their own Source scope and are not implied.
        let app = dispatcharr_job_transport::version_application::Application::prepare(
            store.clone(),
            Arc::new(bus.clone()),
            &broker,
            &backend,
            dispatcharr_job_transport::version_service::Options {
                version: "0.33.0".into(),
                endpoint,
                ca_bundle: std::path::PathBuf::from(
                    env::var("RUST_VERSION_CA_BUNDLE")
                        .unwrap_or_else(|_| "/opt/dispatcharr-crypto/trust/cacert.pem".into()),
                ),
            },
        )
        .await
        .map_err(|_| "Version task initialization failed (credentials omitted)")?;
        Some(app)
    } else {
        None
    };
    let mut epg_index = None;
    let mut epg_series = None;
    let mut epg_artwork = None;
    let mut epg_tvg = None;
    let epg_refresh = if refresh_enabled {
        let cache = env::var("REDIS_URL").map_err(|_| "Enabled EPG jobs require REDIS_URL")?;
        let bus = realtime_bus
            .as_ref()
            .ok_or("Enabled EPG jobs require the realtime publisher")?;
        let broker = env::var("CELERY_BROKER_URL").unwrap_or_else(|_| cache.clone());
        let backend = env::var("CELERY_RESULT_BACKEND").unwrap_or_else(|_| broker.clone());
        let source_cwd = std::path::PathBuf::from(
            env::var("RUST_EPG_SOURCE_CWD").unwrap_or_else(|_| "/app".into()),
        );
        let media_root = std::path::PathBuf::from(
            env::var("RUST_EPG_MEDIA_ROOT").unwrap_or_else(|_| "/app/media".into()),
        );
        if !source_cwd.is_dir() || !media_root.is_dir() {
            return Err("Enabled EPG jobs require the original source/media path mounts".into());
        }
        let cache_root = media_root.join("cached_epg");
        let cache_dir = env::var("RUST_EPG_CACHE_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| cache_root.clone());
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
            .with_current_file_lookup(index.lookup(), cache_root)
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
                input_roots: vec![media_root],
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
        let artwork_completions = dispatcharr_job_transport::ArtworkCompletions::new();
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
                artwork_completions,
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
        if artwork.start().is_err()
            || index.start(Arc::new(())).is_err()
            || series.start().is_err()
            || tvg.start(Arc::new(())).is_err()
            || refresh.start().is_err()
        {
            let _ = refresh.shutdown(Duration::from_secs(4)).await;
            let _ = tvg.shutdown(Duration::from_secs(4)).await;
            artwork.close_producer_admission();
            let _ = series.shutdown(Duration::from_secs(4)).await;
            let _ = artwork.shutdown(Duration::from_secs(4)).await;
            let _ = index.shutdown(Duration::from_secs(4)).await;
            return Err("EPG index intake initialization failed".into());
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
            gateway.with_live(playback::dispatcher(
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
        env::var("REDIS_URL").ok().as_deref(),
        combined_statistics::Config::default(),
    )
    .await
    .map_err(|_| "Combined statistics initialization failed (credentials omitted)")?;
    // Preserve the shared AuthService admission pool when repository routes join
    // Core and the other application APIs. The fetch adapter still rejects its
    // explicitly unsupported transports and signatures.
    let plugin_repositories =
        dispatcharr_store::PluginRepositories::new(auth.clone()).with_unsigned_manifest_fetches();
    let mut app = Router::new()
        .route("/health/ready", get(health))
        .with_state(store)
        .merge(auth.clone().router())
        .merge(combined_statistics.router())
        .route("/proxy/stats", any(combined_stats_legacy_alias))
        .merge(output_profiles.clone().router())
        .merge(backup_files.clone().router())
        .merge(plugin_repositories.router())
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
    for prefix in ["api", "proxy", "output", "admin"] {
        app = app
            .route(&format!("/{prefix}"), any(api_not_found))
            .route(&format!("/{prefix}/{{*path}}"), any(api_not_found));
    }
    if let Ok(path) = env::var("RUST_FRONTEND_DIR") {
        let index = std::path::Path::new(&path).join("index.html");
        if !index.is_file() {
            return Err("RUST_FRONTEND_DIR must contain index.html".into());
        }
        // Actual static directories take precedence over the original short
        // three-component XC credential alias, including nested font assets.
        for prefix in ["assets", "static"] {
            let directory = std::path::Path::new(&path).join(prefix);
            if directory.is_dir() {
                if prefix == "static" {
                    if let Some(assets) = static_assets::load(&directory)
                        .await
                        .map_err(|error| format!("Pinned static assets: {error}"))?
                    {
                        let files = Router::new()
                            .fallback_service(tower_http::services::ServeDir::new(directory))
                            .layer(axum::middleware::from_fn_with_state(
                                assets,
                                static_assets::apply,
                            ));
                        app = app.nest_service("/static", files.into_service());
                        continue;
                    }
                }
                app = app.nest_service(
                    &format!("/{prefix}"),
                    tower_http::services::ServeDir::new(directory),
                );
            }
        }
        app = app.fallback_service(
            tower_http::services::ServeDir::new(path)
                .fallback(tower_http::services::ServeFile::new(index.clone())),
        );
        // Django converter nonmatches fall through to its final React TemplateView.
        // Classify before the broad native VOD handlers can authenticate/resolve.
        app = vod_react_fallback::with_index(app, index);
    }
    app = app.layer(axum::middleware::from_fn(response_headers::apply));
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|_| "Cannot bind HTTP listener")?;
    eprintln!("Dispatcharr Rust port listening on {bind}; migration parity is incomplete");
    let shutdown_timeout = Duration::from_secs(positive("RUST_SHUTDOWN_SECONDS", 12)? as u64);
    if let Some(version) = &version_tasks {
        if version.start().is_err() {
            let _ = version.shutdown(Duration::from_secs(4)).await;
            return Err("Version task intake failed".into());
        }
    }
    let (shutdown_started, mut shutdown_notice) = tokio::sync::oneshot::channel();
    let services = ShutdownServices {
        proxy_health_core,
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
    let signal_services = services.clone();
    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown_signal().await;
        signal_services.engine.shutdown();
        let cleanup = tokio::spawn(shutdown_components(signal_services));
        let _ = shutdown_started.send(cleanup);
        // Return immediately: Axum stops accepting while cleanup runs.
    })
    .into_future();
    tokio::pin!(server);
    tokio::select! {
        result=&mut server=>{
            let _=tokio::time::timeout(shutdown_timeout,shutdown_components(services)).await;
            result.map_err(|_|"HTTP server failed".to_string())
        },
        cleanup=&mut shutdown_notice=>{
            let mut cleanup=cleanup.map_err(|_|"Shutdown coordination failed")?;
            let drained=tokio::time::timeout(shutdown_timeout,async{
                let result=(&mut server).await;
                let _=(&mut cleanup).await;
                result
            }).await;
            match drained{
                Ok(result)=>result.map_err(|_|"HTTP server failed".to_string()),
                Err(_)=>{cleanup.abort();eprintln!("Shutdown deadline reached; closing remaining HTTP connections");Ok(())},
            }
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
    proxy_health_core: Option<dispatcharr_store::CoreSettingsWrites>,
    engine: StreamEngine,
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
async fn shutdown_components(services: ShutdownServices) {
    let ShutdownServices {
        proxy_health_core,
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
    } = services;
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
        backup_files.shutdown(Duration::from_secs(8)),
        epg_programs.shutdown(Duration::from_secs(8)),
        grid.shutdown(Duration::from_secs(8)),
        hdhr.shutdown(),
        vod_reads.shutdown(Duration::from_secs(8)),
        combined_statistics.shutdown(Duration::from_secs(8)),
    );
    let proxy_health_drained = match proxy_health_core {
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
    if !proxy_health_drained
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
        || !vod_read_drained
    {
        eprintln!("Service drain deadline reached; pending reservations require recovery");
    }
    if let Some(clients) = clients {
        clients.shutdown().await;
    }
    if let Some(statistics) = live_statistics {
        if !statistics.shutdown(Duration::from_millis(500)).await {
            eprintln!("Statistics drain deadline reached; pending callbacks require recovery");
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = plugin_gpg_trace::install_from_environment() {
        eprintln!("Dispatcharr Rust startup failed: {error}");
        std::process::exit(1);
    }
    if let Err(error) = run().await {
        eprintln!("Dispatcharr Rust startup failed: {error}");
        std::process::exit(1);
    }
}
