//! Feature-gated owned SQLite runtime experiment. Original Dispatcharr authors retain credit.
use super::{
    database_config::{self, DatastoreSelection},
    response_headers, static_assets, LatchedShutdown,
};
use axum::{middleware, routing::get_service, Router};
use dispatcharr_store::{AuthConfig, CopiedSqliteRuntime};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};
use tower_http::services::ServeFile;

async fn frontend(root: &Path) -> Result<Router, String> {
    let index = static_assets::index(root)?;
    let shell = get_service(ServeFile::new(index));
    let app = Router::new()
        .route("/", shell.clone())
        .route("/channels", shell);
    Ok(static_assets::with_directories(app, root)
        .await?
        .layer(middleware::from_fn(response_headers::apply)))
}

fn copied_path(source: &Path) -> Result<PathBuf, String> {
    let root = PathBuf::from(
        std::env::var_os("RUST_SQLITE_COPIED_DATA_ROOT")
            .ok_or("SQLite copied runtime requires a private detached data directory")?,
    );
    let inspect = std::fs::symlink_metadata(&root)
        .map_err(|_| "SQLite copied runtime requires an existing private directory")?;
    let canonical = root
        .canonicalize()
        .map_err(|_| "SQLite copied runtime requires an existing private directory")?;
    if !inspect.is_dir() || root != canonical || source.parent() == Some(&canonical) {
        return Err("SQLite copied runtime requires a private detached data directory".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if inspect.permissions().mode() & 0o077 != 0 {
            return Err("SQLite copied runtime directory must be private".into());
        }
    }
    Ok(canonical.join(source.file_name().ok_or("Invalid configured SQLite name")?))
}

pub(super) async fn run() -> Result<(), String> {
    let DatastoreSelection::Sqlite(configured) =
        database_config::selection(|name| std::env::var(name).ok())
    else {
        return Err("SQLite copied runtime requires DB_ENGINE=sqlite".into());
    };
    let copy = copied_path(configured)?;
    let bind: SocketAddr = std::env::var("RUST_BIND")
        .unwrap_or_else(|_| "127.0.0.1:0".into())
        .parse()
        .map_err(|_| "SQLite copied runtime requires a valid loopback bind")?;
    if !bind.ip().is_loopback() {
        return Err("SQLite copied runtime requires a loopback bind".into());
    }
    let redis =
        super::redis_config::url()?.ok_or("SQLite copied runtime requires owned loopback Redis")?;
    let opened = url::Url::parse(&redis).map_err(|_| "Invalid owned Redis configuration")?;
    let ip = opened.host_str().and_then(|host| {
        host.trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .ok()
    });
    if !ip.is_some_and(|address| address.is_loopback()) {
        return Err("SQLite copied runtime requires owned loopback Redis".into());
    }
    let secret = std::env::var("DJANGO_SECRET_KEY").map_err(|_| "DJANGO_SECRET_KEY is required")?;
    let auth = AuthConfig::new(secret).map_err(|_| "Invalid authentication configuration")?;
    let mut stop = LatchedShutdown::register()?;
    let frontend = match std::env::var("RUST_FRONTEND_DIR") {
        Ok(path) => frontend(Path::new(&path)).await?,
        Err(_) => Router::new(),
    };
    let ip_bus = super::realtime::bus(Some(&redis)).await?;
    let runtime = CopiedSqliteRuntime::open(&copy, auth, &redis).await?;
    let runtime = match ip_bus {
        Some(bus) => runtime.with_ip_event_sink(Arc::new(bus)),
        None => runtime,
    };
    let result = async {
        let listener = tokio::net::TcpListener::bind(bind)
            .await
            .map_err(|_| "SQLite copied runtime loopback bind failed")?;
        let address = listener
            .local_addr()
            .map_err(|_| "SQLite copied runtime bind address unavailable")?;
        println!("dispatcharr-sqlite-copied-ready {address}");
        axum::serve(
            listener,
            runtime
                .router()
                .merge(frontend)
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move { stop.wait().await })
        .await
        .map_err(|_| "SQLite copied runtime HTTP stopped with an error")
    }
    .await;
    let retired = runtime.close().await;
    result.and(retired).map_err(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::{header, Method, Request, StatusCode},
        routing::get,
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn serves_packaged_shell_and_assets_with_get_and_head_without_claiming_channel_api() {
        let root =
            std::env::temp_dir().join(format!("dispatcharr-copied-shell-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let index = b"<!doctype html><script src=\"/assets/index.js\"></script>";
        let script = b"console.log('owned fixture');";
        std::fs::create_dir(root.join("assets")).unwrap();
        std::fs::write(root.join("index.html"), index).unwrap();
        std::fs::write(root.join("assets/index.js"), script).unwrap();
        let app = Router::new()
            .route("/healthz", get(|| async { "copied runtime ready" }))
            .merge(frontend(&root).await.unwrap());
        for (path, body, mime) in [
            ("/", index.as_slice(), "text/html; charset=utf-8"),
            ("/channels", index.as_slice(), "text/html; charset=utf-8"),
            ("/assets/index.js", script.as_slice(), "text/javascript"),
        ] {
            for method in [Method::GET, Method::HEAD] {
                let response = app
                    .clone()
                    .oneshot(
                        Request::builder()
                            .method(method.clone())
                            .uri(path)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                assert_eq!(response.headers()[header::CONTENT_TYPE], mime);
                assert_eq!(
                    response.headers()[header::CONTENT_LENGTH],
                    body.len().to_string()
                );
                let received = to_bytes(response.into_body(), 4096).await.unwrap();
                assert_eq!(
                    received.as_ref(),
                    if method == Method::HEAD { &[] } else { body }
                );
            }
        }
        for path in [
            "/api/channels/channels/?page=1&page_size=500",
            "/assets/absent.js",
            "/arbitrary-page",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }
        let health = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            to_bytes(health.into_body(), 64).await.unwrap(),
            "copied runtime ready"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
