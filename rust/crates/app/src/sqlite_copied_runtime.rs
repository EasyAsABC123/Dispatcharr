//! Feature-gated owned SQLite runtime experiment. Original Dispatcharr authors retain credit.
use super::{
    database_config::{self, DatastoreSelection},
    LatchedShutdown,
};
use dispatcharr_store::{AuthConfig, CopiedSqliteRuntime};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

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
    let runtime = CopiedSqliteRuntime::open(&copy, auth, &redis).await?;
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
