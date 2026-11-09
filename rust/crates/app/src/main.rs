//! Derivative Dispatcharr backend port. Original project credit: ATTRIBUTION.md.
mod control;
mod playback;
use axum::{
    extract::State,
    http::StatusCode,
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

async fn run() -> Result<(), String> {
    let bind: SocketAddr = env::var("RUST_BIND")
        .unwrap_or_else(|_| "127.0.0.1:9192".into())
        .parse()
        .map_err(|_| "Invalid RUST_BIND")?;
    let secret = env::var("DJANGO_SECRET_KEY").map_err(|_| "DJANGO_SECRET_KEY is required")?;
    let mut auth_config =
        AuthConfig::new(secret).map_err(|_| "Invalid authentication configuration")?;
    auth_config.trusted_proxies = env::var("DISPATCHARR_TRUSTED_PROXIES").ok();
    let store = Store::connect(
        &database_url()?,
        positive("RUST_DATABASE_POOL_SIZE", 8)?
            .try_into()
            .map_err(|_| "Database pool size overflow")?,
    )
    .await
    .map_err(|_| "Database connection failed (credentials omitted)")?;
    let auth = AuthService::new(store.clone(), auth_config);
    let mut config = EngineConfig::default();
    config.initial_buffer_chunks = 4;
    config.flush_partial_chunks = false;
    config.source_initialization = true;
    config.startup_timeout = Duration::from_secs(60);
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
    let auth = auth.with_channel_control(Arc::new(control::EngineControl(engine.clone())));
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
    let vod = if let Some(coordinator) = coordinator {
        Some(
            dispatcharr_vod_http::VodEngine::new(
                store.clone(),
                auth.clone(),
                coordinator,
                &env::var("REDIS_URL").map_err(|_| "Redis VOD configuration missing")?,
                dispatcharr_vod_http::VodConfig {
                    max_sessions: positive("RUST_MAX_VOD_SESSIONS", 1024)?,
                    max_requests: positive("RUST_MAX_VOD_REQUESTS", 2048)?,
                    frame_bytes: positive("RUST_VOD_FRAME_BYTES", 64 * 1024)?,
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| "VOD initialization failed (credentials omitted)")?,
        )
    } else {
        None
    };
    let mut app = Router::new()
        .route("/health/ready", get(health))
        .with_state(store)
        .merge(auth.clone().router())
        .merge(engine.controls_router(resolver.clone()))
        .merge(playback::router(
            engine.clone(),
            resolver,
            hls.clone(),
            fmp4.clone(),
        ));
    if let Some(gateway) = &hls {
        app = app.merge(gateway.router());
    }
    if let Some(vod) = &vod {
        app = app.merge(vod.clone().router());
    }
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
        app = app.fallback_service(
            tower_http::services::ServeDir::new(path)
                .fallback(tower_http::services::ServeFile::new(index)),
        );
    }
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|_| "Cannot bind HTTP listener")?;
    eprintln!("Dispatcharr Rust port listening on {bind}; migration parity is incomplete");
    let shutdown_timeout = Duration::from_secs(positive("RUST_SHUTDOWN_SECONDS", 12)? as u64);
    let (shutdown_started, mut shutdown_notice) = tokio::sync::oneshot::channel();
    let signal_engine = engine.clone();
    let signal_clients = clients.clone();
    let signal_hls = hls.clone();
    let signal_fmp4 = fmp4.clone();
    let signal_vod = vod.clone();
    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown_signal().await;
        signal_engine.shutdown();
        let cleanup = tokio::spawn(shutdown_components(
            signal_engine,
            signal_clients,
            signal_hls,
            signal_fmp4,
            signal_vod,
        ));
        let _ = shutdown_started.send(cleanup);
        // Return immediately: Axum stops accepting while cleanup runs.
    })
    .into_future();
    tokio::pin!(server);
    tokio::select! {
        result=&mut server=>{
            let _=tokio::time::timeout(shutdown_timeout,shutdown_components(engine,clients,hls,fmp4,vod)).await;
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
async fn shutdown_components(
    engine: StreamEngine,
    clients: Option<ClientStore>,
    hls: Option<dispatcharr_hls_http::HlsGateway>,
    fmp4: Option<dispatcharr_fmp4_http::Fmp4Gateway>,
    vod: Option<dispatcharr_vod_http::VodEngine>,
) {
    engine.shutdown();
    let (live, _, _, vod) = tokio::join!(
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
    );
    if !live || !vod {
        eprintln!("Media drain deadline reached; pending reservations require recovery");
    }
    if let Some(clients) = clients {
        clients.shutdown().await;
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("Dispatcharr Rust startup failed: {error}");
        std::process::exit(1);
    }
}
