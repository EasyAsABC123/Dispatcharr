//! Real loopback HTTP contracts for the source-compatible archive selector.
//! Original Dispatcharr application credit belongs to upstream contributors.
use axum::{
    body::Body,
    extract::{Path, State},
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use dispatcharr_timeshift_http::{ArchiveClient, ArchiveOpen, ArchiveRequest};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::task::JoinHandle;

struct Server {
    base: String,
    task: JoinHandle<()>,
    seen: Arc<Mutex<Vec<String>>>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn ts() -> Vec<u8> {
    let mut bytes = vec![0u8; 188 * 12];
    for chunk in bytes.chunks_mut(188) {
        chunk[0] = 0x47;
    }
    bytes
}
async fn handler(
    State(seen): State<Arc<Mutex<Vec<String>>>>,
    Path(path): Path<String>,
    request: Request<Body>,
) -> Response {
    assert_eq!(request.headers()["accept-encoding"], "identity");
    seen.lock().unwrap().push(path.clone());
    match path.as_str() {
        "html" => (
            [("content-type", "text/html")],
            "<html>provider error</html>",
        )
            .into_response(),
        "json" => ([("content-type", "application/json")], "{\"error\":true}").into_response(),
        "ts" => {
            let mut payload = b"preamble".to_vec();
            payload.extend(ts());
            ([("content-type", "video/mp2t")], payload).into_response()
        }
        "partial" => {
            assert_eq!(request.headers()["range"], "bytes=7-100");
            (
                StatusCode::PARTIAL_CONTENT,
                [
                    ("content-type", "video/mp2t"),
                    ("content-range", "bytes 7-100/500"),
                ],
                vec![0x42u8; 94],
            )
                .into_response()
        }
        "unsatisfiable" => (
            StatusCode::RANGE_NOT_SATISFIABLE,
            [("content-range", "bytes */500")],
            "must not be returned as media",
        )
            .into_response(),
        "redirect" => (StatusCode::FOUND, [("location", "/ts")]).into_response(),
        "403" => StatusCode::FORBIDDEN.into_response(),
        "404" => StatusCode::NOT_FOUND.into_response(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
async fn server() -> Server {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let router = Router::new()
        .route("/{path}", get(handler))
        .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Server { base, task, seen }
}
fn client(max: usize) -> ArchiveClient {
    ArchiveClient::new(max, Duration::from_secs(1), Duration::from_secs(1)).unwrap()
}
fn request(urls: &[String]) -> ArchiveRequest<'_> {
    ArchiveRequest {
        urls,
        cached_format: None,
        final_url: None,
        user_agent: "Dispatcharr-contract",
        range: None,
    }
}
async fn body(mut media: dispatcharr_timeshift_http::ArchiveMedia) -> Vec<u8> {
    let mut bytes = Vec::new();
    while let Some(chunk) = media.next_chunk().await.unwrap() {
        bytes.extend_from_slice(&chunk);
    }
    bytes
}
#[tokio::test]
async fn php_errors_cascade_full_ts_trims_prefix_and_cached_format_is_prioritized() {
    let server = server().await;
    let client = client(2);
    let urls = vec![
        format!("{}/html", server.base),
        format!("{}/json", server.base),
        format!("{}/ts", server.base),
    ];
    let media = match client.open(request(&urls)).await {
        Ok(ArchiveOpen::Media(media)) => media,
        _ => panic!("expected media"),
    };
    assert_eq!(media.format_index, Some(2));
    assert_eq!(body(media).await, ts());
    assert_eq!(*server.seen.lock().unwrap(), ["html", "json", "ts"]);
    server.seen.lock().unwrap().clear();
    let mut req = request(&urls);
    req.cached_format = Some(2);
    assert!(matches!(client.open(req).await, Ok(ArchiveOpen::Media(_))));
    assert_eq!(*server.seen.lock().unwrap(), ["ts"]);
}
#[tokio::test]
async fn partial_arbitrary_offsets_are_verbatim_and_416_is_terminal() {
    let server = server().await;
    let client = client(2);
    let urls = vec![format!("{}/partial", server.base)];
    let mut req = request(&urls);
    req.range = Some("bytes=7-100");
    let media = match client.open(req).await {
        Ok(ArchiveOpen::Media(media)) => media,
        _ => panic!("expected partial"),
    };
    assert_eq!(body(media).await, vec![0x42; 94]);
    server.seen.lock().unwrap().clear();
    let urls = vec![
        format!("{}/unsatisfiable", server.base),
        format!("{}/ts", server.base),
    ];
    match client.open(request(&urls)).await {
        Ok(ArchiveOpen::Unsatisfiable { content_range }) => {
            assert_eq!(content_range.as_deref(), Some("bytes */500"))
        }
        _ => panic!("416 must not cascade"),
    };
    assert_eq!(*server.seen.lock().unwrap(), ["unsatisfiable"]);
}
#[tokio::test]
async fn decisive_errors_stop_but_server_errors_cascade_and_cached_cdn_redirect_does_not_follow() {
    let server = server().await;
    let client = client(2);
    for (path, status, message, expected_attempts) in [
        ("403", 403, "Provider denied access", 1),
        ("404", 404, "Catch-up not available yet", 2),
        ("500", 400, "Provider error", 2),
    ] {
        server.seen.lock().unwrap().clear();
        let urls = vec![format!("{}/{path}", server.base); 2];
        let error = match client.open(request(&urls)).await {
            Err(error) => error,
            _ => panic!("expected provider rejection"),
        };
        assert_eq!(error.status.as_u16(), status);
        assert_eq!(error.message, message);
        assert_eq!(error.decisive, path == "403");
        assert_eq!(server.seen.lock().unwrap().len(), expected_attempts);
    }
    server.seen.lock().unwrap().clear();
    let urls = vec![format!("{}/ts", server.base)];
    let cdn = format!("{}/redirect", server.base);
    let mut req = request(&urls);
    req.final_url = Some(&cdn);
    let media = match client.open(req).await {
        Ok(ArchiveOpen::Media(media)) => media,
        _ => panic!("portal fallback"),
    };
    assert!(!media.used_cached_final);
    assert_eq!(*server.seen.lock().unwrap(), ["redirect", "ts"]);
}
#[tokio::test]
async fn held_body_retains_capacity_drop_releases_and_initial_network_failure_does_not_cascade() {
    let server = server().await;
    let client = client(1);
    let urls = vec![format!("{}/ts", server.base)];
    let media = match client.open(request(&urls)).await {
        Ok(ArchiveOpen::Media(media)) => media,
        _ => panic!("expected media"),
    };
    let error = match client.open(request(&urls)).await {
        Err(error) => error,
        _ => panic!("capacity must remain held"),
    };
    assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
    drop(media);
    assert!(matches!(
        client.open(request(&urls)).await,
        Ok(ArchiveOpen::Media(_))
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let refused = format!("http://{}/secret-fixture", listener.local_addr().unwrap());
    drop(listener);
    let urls = vec![refused, format!("{}/ts", server.base)];
    server.seen.lock().unwrap().clear();
    let error = match client.open(request(&urls)).await {
        Err(error) => error,
        _ => panic!("initial connection error"),
    };
    assert_eq!(error.message, "Provider connection error");
    assert!(server.seen.lock().unwrap().is_empty());
}
