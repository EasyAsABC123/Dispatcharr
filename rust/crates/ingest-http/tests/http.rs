use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, Response, StatusCode},
    routing::get,
    Router,
};
use bytes::Bytes;
use dispatcharr_ingest_http::{Downloader, FetchConfig, FetchError};
use futures_util::StreamExt;
use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
static NEXT: AtomicU64 = AtomicU64::new(0);
const PLAYLIST: &[u8] = b"#EXTM3U\n#EXTINF:-1,Fixture\nhttp://fixture/live\n";
#[derive(Clone, Default)]
struct Records(Arc<Mutex<Vec<(String, HeaderMap)>>>);
async fn origin(
    State(records): State<Records>,
    Path(mode): Path<String>,
    headers: HeaderMap,
) -> Response<Body> {
    records
        .0
        .lock()
        .unwrap()
        .push((mode.clone(), headers.clone()));
    if mode == "cookie" {
        return Response::builder()
            .status(302)
            .header(header::LOCATION, "/cookie-final")
            .header(header::SET_COOKIE, "fixture=present; Path=/")
            .body(Body::empty())
            .unwrap();
    }
    if mode == "cookie-final"
        && headers
            .get(header::COOKIE)
            .is_none_or(|value| value != "fixture=present")
    {
        return Response::builder()
            .status(403)
            .body(Body::from("missing cookie"))
            .unwrap();
    }
    if mode == "fresh" && headers.contains_key(header::COOKIE) {
        return Response::builder()
            .status(409)
            .body(Body::from("leaked cookie"))
            .unwrap();
    }
    if mode == "hold" {
        let stream =
            futures_util::stream::once(async { Ok::<_, io::Error>(Bytes::from_static(PLAYLIST)) })
                .chain(futures_util::stream::pending());
        return Response::builder()
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from_stream(stream))
            .unwrap();
    }
    let (status, data) = match mode.as_str() {
        "partial" => (StatusCode::PARTIAL_CONTENT, PLAYLIST.to_vec()),
        "empty" => (StatusCode::OK, Vec::new()),
        "html" => (StatusCode::OK, b"<html>unauthorized".to_vec()),
        "error" => (StatusCode::FORBIDDEN, b"denied\xe9".to_vec()),
        "large-error" => (StatusCode::BAD_GATEWAY, vec![b'x'; 65537]),
        _ => (StatusCode::OK, PLAYLIST.to_vec()),
    };
    if mode == "gzip" {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&data).unwrap();
        return Response::builder()
            .status(status)
            .header(header::CONTENT_ENCODING, "gzip")
            .body(Body::from(encoder.finish().unwrap()))
            .unwrap();
    }
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::from(data))
        .unwrap()
}
async fn serve() -> (String, Records, tokio::task::JoinHandle<()>) {
    let records = Records::default();
    let app = Router::new()
        .route("/{mode}", get(origin))
        .with_state(records.clone());
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", socket.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(socket, app).await.unwrap();
    });
    (url, records, task)
}
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "dispatcharr-fetch-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn cache(&self, name: &str) -> PathBuf {
        let path = self.0.join(format!("{name}.m3u"));
        fs::write(&path, b"previous cache").unwrap();
        path
    }
    fn temporary(&self, name: &str) -> PathBuf {
        self.0.join(format!("{name}.m3u.tmp"))
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn accepts_all_source_success_statuses_and_reuses_no_cookie_between_downloads() {
    let fixture = Fixture::new();
    let (url, records, server) = serve().await;
    let downloader = Downloader::new(FetchConfig::default()).unwrap();
    for (index, mode) in ["plain", "partial", "gzip", "cookie", "fresh"]
        .iter()
        .enumerate()
    {
        let path = fixture.cache(&index.to_string());
        let result = downloader
            .fetch(&format!("{url}/{mode}"), "Café", &path)
            .await
            .unwrap();
        assert_eq!(result.stats.bytes, PLAYLIST.len() as u64);
        assert_eq!(result.stats.lines, 3);
        assert_eq!(fs::read(path).unwrap(), PLAYLIST);
        assert_eq!(result.status, if *mode == "partial" { 206 } else { 200 });
    }
    {
        let records = records.0.lock().unwrap();
        for (_, headers) in records.iter() {
            assert_eq!(headers[header::USER_AGENT].as_bytes(), b"Caf\xe9");
            assert_eq!(headers[header::ACCEPT_ENCODING], "gzip, deflate");
        }
        assert!(records
            .iter()
            .find(|(mode, _)| mode == "cookie-final")
            .unwrap()
            .1
            .contains_key(header::COOKIE));
        assert!(!records
            .iter()
            .find(|(mode, _)| mode == "fresh")
            .unwrap()
            .1
            .contains_key(header::COOKIE));
    }
    assert!(downloader.shutdown_wait(Duration::from_secs(1)).await);
    server.abort();
}

#[tokio::test]
async fn invalid_empty_oversize_and_http_error_keep_prior_cache_and_clean_staging() {
    let fixture = Fixture::new();
    let (url, _, server) = serve().await;
    let downloader = Downloader::new(FetchConfig {
        maximum_bytes: 32,
        ..FetchConfig::default()
    })
    .unwrap();
    for mode in ["html", "empty", "plain", "error", "large-error"] {
        let path = fixture.cache(mode);
        let error = downloader
            .fetch(&format!("{url}/{mode}"), "", &path)
            .await
            .unwrap_err();
        if mode == "error" {
            match error {
                FetchError::Status { status, body, .. } => {
                    assert_eq!(status, 403);
                    assert_eq!(body, b"denied\xe9");
                }
                other => panic!("{other:?}"),
            }
        } else if mode == "large-error" {
            assert!(matches!(error, FetchError::ErrorBodyCapacity));
        } else {
            assert!(matches!(error, FetchError::Cache(_)));
        }
        assert_eq!(fs::read(path).unwrap(), b"previous cache");
        assert!(!fixture.temporary(mode).exists());
        assert_eq!(downloader.active_jobs(), 0);
    }
    assert!(downloader.shutdown_wait(Duration::from_secs(1)).await);
    server.abort();
}

#[tokio::test]
async fn abort_and_shutdown_hold_admission_until_actual_worker_cleanup() {
    let fixture = Fixture::new();
    let (url, records, server) = serve().await;
    let downloader = Downloader::new(FetchConfig {
        concurrent_jobs: 1,
        ..FetchConfig::default()
    })
    .unwrap();
    let path = fixture.cache("abort");
    let active = downloader.clone();
    let source = format!("{url}/hold");
    let task = tokio::spawn(async move { active.fetch(&source, "", &path).await });
    until(|| {
        fixture
            .temporary("abort")
            .metadata()
            .is_ok_and(|meta| meta.len() > 0)
    })
    .await;
    assert_eq!(downloader.active_jobs(), 1);
    task.abort();
    let _ = task.await;
    until(|| downloader.active_jobs() == 0).await;
    assert!(!fixture.temporary("abort").exists());
    assert_eq!(
        fs::read(fixture.0.join("abort.m3u")).unwrap(),
        b"previous cache"
    );
    let path = fixture.cache("shutdown");
    let active = downloader.clone();
    let source = format!("{url}/hold");
    let first = tokio::spawn(async move { active.fetch(&source, "", &path).await });
    until(|| fixture.temporary("shutdown").exists()).await;
    let path = fixture.cache("queued");
    let active = downloader.clone();
    let source = format!("{url}/plain");
    let queued = tokio::spawn(async move { active.fetch(&source, "", &path).await });
    tokio::task::yield_now().await;
    assert!(downloader.shutdown_wait(Duration::from_secs(2)).await);
    assert!(matches!(first.await.unwrap(), Err(FetchError::Shutdown)));
    assert!(matches!(queued.await.unwrap(), Err(FetchError::Shutdown)));
    assert_eq!(records.0.lock().unwrap().len(), 2);
    assert!(!fixture.temporary("shutdown").exists());
    assert!(!fixture.temporary("queued").exists());
    server.abort();
}

#[tokio::test]
async fn unknown_staging_owner_fails_promptly_even_with_stalled_body() {
    let fixture = Fixture::new();
    let (url, _, server) = serve().await;
    let downloader = Downloader::new(FetchConfig::default()).unwrap();
    let path = fixture.cache("existing");
    fs::write(fixture.temporary("existing"), b"another owner").unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        downloader.fetch(&format!("{url}/hold"), "", &path),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(FetchError::Cache(_))));
    assert_eq!(
        fs::read(fixture.temporary("existing")).unwrap(),
        b"another owner"
    );
    assert_eq!(downloader.active_jobs(), 0);
    assert!(downloader.shutdown_wait(Duration::from_secs(1)).await);
    server.abort();
}
