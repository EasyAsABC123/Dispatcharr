//! Derivative Dispatcharr backend port. Original project credit: ATTRIBUTION.md.
mod playback;
use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use dispatcharr_clients::{ClientConfig, ClientStore};
use dispatcharr_coordination::{Coordinator, LeaseConfig};
use dispatcharr_store::{AuthConfig, AuthService, Store};
use dispatcharr_stream_http::{EngineConfig, StreamEngine};
use std::{env, net::SocketAddr, sync::Arc, time::Duration};

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
    config.max_channels = positive("RUST_MAX_CHANNELS", config.max_channels)?;
    config.max_viewers_per_channel = positive(
        "RUST_MAX_VIEWERS_PER_CHANNEL",
        config.max_viewers_per_channel,
    )?;
    config.retained_bytes_per_channel = positive(
        "RUST_RETAINED_BYTES_PER_CHANNEL",
        config.retained_bytes_per_channel,
    )?;
    config.retention = Duration::from_secs(positive("RUST_RETENTION_SECONDS", 60)? as u64);
    config.chunk_bytes = positive("RUST_CHUNK_PACKETS", 1361)?
        .checked_mul(188)
        .ok_or("Chunk size overflow")?;
    let (coordinator, clients) = if let Ok(url) = env::var("REDIS_URL") {
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
        (Some(coordinator), Some(clients))
    } else {
        (None, None)
    };
    let engine = StreamEngine::new_with_services(config, coordinator, clients.clone())
        .map_err(|_| "Invalid streaming configuration")?;
    let resolver: Arc<dyn dispatcharr_contracts::PlaybackResolver> = Arc::new(auth.clone());
    let hls = clients
        .clone()
        .map(|clients| {
            dispatcharr_hls_http::HlsGateway::new(
                engine.clone(),
                clients,
                resolver.clone(),
                Default::default(),
            )
        })
        .transpose()
        .map_err(|_| "Invalid HLS configuration")?;
    let mut app = Router::new()
        .route("/health/ready", get(health))
        .with_state(store)
        .merge(auth.clone().router())
        .merge(engine.controls_router(resolver.clone()))
        .merge(playback::router(engine.clone(), resolver, hls.clone()));
    if let Some(gateway) = &hls {
        app = app.merge(gateway.router());
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
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        #[cfg(unix)]
        {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut terminate) => {
                    tokio::select! { _ = terminate.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
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
        if let Some(hls) = hls {
            hls.shutdown().await;
        }
        engine.shutdown();
        if let Some(clients) = clients {
            clients.shutdown().await;
        }
    })
    .await
    .map_err(|_| "HTTP server failed".to_string())
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("Dispatcharr Rust startup failed: {error}");
        std::process::exit(1);
    }
}
