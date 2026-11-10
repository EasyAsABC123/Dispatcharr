//! Derivative Dispatcharr backend port. Original project credit: ATTRIBUTION.md.
mod control;
mod playback;
mod realtime;
mod response_headers;
mod statistics;
use axum::{
    extract::State,
    http::{header, StatusCode},
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
    let realtime_bus = realtime::bus().await?;
    // Attach before every resolver/environment/XC clone so original producers
    // publish through the same bounded process transport.
    let auth = if let Some(bus) = &realtime_bus {
        auth.with_event_sink(Arc::new(bus.clone()))
    } else {
        auth
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
        ..Default::default()
    };
    let (coordinator, clients, outputs, inputs) = if let Ok(url) = env::var("REDIS_URL") {
        let coordinator = Coordinator::connect(&url, LeaseConfig::default())
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
            .with_epg(),
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
    let epg_programs = dispatcharr_store::EpgProgramReads::new(
        auth.clone(),
        dispatcharr_store::EpgProgramReadLimits {
            concurrent_responses: positive("RUST_MAX_EPG_READ_RESPONSES", 4)?,
            response_bytes: positive("RUST_EPG_READ_RESPONSE_BYTES", 16 * 1024 * 1024)?,
            row_bytes: positive("RUST_EPG_READ_ROW_BYTES", 1024 * 1024)?,
            deadline: Duration::from_millis(positive("RUST_EPG_READ_TIMEOUT_MS", 2000)? as u64),
        },
    )
    .map_err(|_| "Invalid EPG programme read configuration")?;
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
    let mut app = Router::new()
        .route("/health/ready", get(health))
        .with_state(store)
        .merge(auth.clone().router())
        .merge(output_profiles.clone().router())
        .merge(backup_files.clone().router())
        .merge(epg_programs.clone().router())
        .merge(epg_programs.clone().current_router())
        .merge(grid.clone().router())
        .merge(hdhr.router())
        .merge(engine.controls_router(resolver.clone()))
        .merge(playback::router(
            engine.clone(),
            resolver,
            hls.clone(),
            fmp4.clone(),
        ));
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
                app = app.nest_service(
                    &format!("/{prefix}"),
                    tower_http::services::ServeDir::new(directory),
                );
            }
        }
        app = app.fallback_service(
            tower_http::services::ServeDir::new(path)
                .fallback(tower_http::services::ServeFile::new(index)),
        );
    }
    app = app.layer(axum::middleware::from_fn(response_headers::apply));
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|_| "Cannot bind HTTP listener")?;
    eprintln!("Dispatcharr Rust port listening on {bind}; migration parity is incomplete");
    let shutdown_timeout = Duration::from_secs(positive("RUST_SHUTDOWN_SECONDS", 12)? as u64);
    let (shutdown_started, mut shutdown_notice) = tokio::sync::oneshot::channel();
    let services = ShutdownServices {
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
        grid,
        hdhr,
        live_statistics,
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
    grid: dispatcharr_output_http::GridGateway,
    hdhr: dispatcharr_hdhr_http::HdhrGateway,
    live_statistics: Option<Arc<dispatcharr_realtime::LiveStatistics>>,
}
async fn shutdown_components(services: ShutdownServices) {
    let ShutdownServices {
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
        grid,
        hdhr,
        live_statistics,
    } = services;
    engine.shutdown();
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
    );
    if !live
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
    {
        eprintln!("Media drain deadline reached; pending reservations require recovery");
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
    if let Err(error) = run().await {
        eprintln!("Dispatcharr Rust startup failed: {error}");
        std::process::exit(1);
    }
}
