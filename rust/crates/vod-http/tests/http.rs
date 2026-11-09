//! Real loopback HTTP and disposable Redis contracts. Original Dispatcharr
//! authors retain all application credit; see ATTRIBUTION.md.
use axum::{
    body::Body,
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use dispatcharr_contracts::{BoxFuture, PlaybackRequest};
use dispatcharr_coordination::{Coordinator, LeaseConfig};
use dispatcharr_store::{StoreError, User};
use dispatcharr_vod_http::*;
use futures_util::stream;
use redis::AsyncCommands;
use std::{
    net::{IpAddr, Ipv4Addr},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{net::TcpListener, task::JoinHandle};
struct Resolver {
    source: VodSource,
}
impl VodResolver for Resolver {
    fn authorize<'a>(
        &'a self,
        _: &'a VodRequest,
    ) -> BoxFuture<'a, Result<Option<User>, StoreError>> {
        Box::pin(async { Ok(None) })
    }
    fn resolve<'a>(
        &'a self,
        _: &'a VodRequest,
        _: Option<User>,
    ) -> BoxFuture<'a, Result<VodSource, StoreError>> {
        Box::pin(async { Ok(self.source.clone()) })
    }
    fn default_redirect(&self) -> BoxFuture<'_, Result<bool, StoreError>> {
        Box::pin(async { Ok(self.source.redirect) })
    }
    fn admin<'a>(&'a self, _: &'a PlaybackRequest) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async { Ok(()) })
    }
}
async fn origin(request: Request) -> Response {
    let range = request.headers().get("range").and_then(|v| v.to_str().ok());
    let (status, start, end) = match range {
        Some("bytes=900-") => {
            return Response::builder()
                .status(416)
                .header("content-range", "bytes */256")
                .body(Body::empty())
                .unwrap()
        }
        Some("bytes=-10") => (206, 246, 255),
        Some(value) => {
            let (start, end) = value
                .strip_prefix("bytes=")
                .unwrap()
                .split_once('-')
                .unwrap();
            (
                206,
                start.parse::<u32>().unwrap(),
                end.parse::<u32>().unwrap(),
            )
        }
        None => (200, 0, 255),
    };
    let mut response = Response::builder()
        .status(status)
        .header("content-type", "video/mp4")
        .header("content-length", end - start + 1)
        .header("accept-ranges", "bytes")
        .header("etag", "\"fixture-v1\"")
        .header("last-modified", "Wed, 01 Jan 2020 00:00:00 GMT");
    if status == 206 {
        response = response.header("content-range", format!("bytes {start}-{end}/256"));
    }
    response
        .body(Body::from(
            (start..=end).map(|v| v as u8).collect::<Vec<_>>(),
        ))
        .unwrap()
}
async fn paced() -> Response {
    let chunks = stream::unfold(0u32, |n| async move {
        if n == 1000 {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        Some((Ok::<_, std::io::Error>(vec![n as u8; 1024]), n + 1))
    });
    ([("content-type", "video/mp4")], Body::from_stream(chunks)).into_response()
}
async fn retry_origin(request: Request, attempts: Arc<AtomicUsize>, always_fail: bool) -> Response {
    let attempt = attempts.fetch_add(1, Ordering::Relaxed);
    let range = request.headers().get("range").and_then(|v| v.to_str().ok());
    let (start, end) = range
        .and_then(|v| v.strip_prefix("bytes="))
        .and_then(|v| v.split_once('-'))
        .map(|(start, end)| {
            (
                start.parse::<u64>().unwrap_or(0),
                end.parse::<u64>().unwrap_or(65535),
            )
        })
        .unwrap_or((0, 65535));
    let mut builder = Response::builder()
        .status(if range.is_some() { 206 } else { 200 })
        .header("content-type", "video/mp4")
        .header("content-length", end - start + 1);
    if range.is_some() {
        builder = builder.header("content-range", format!("bytes {start}-{end}/65536"));
    }
    if attempt == 0 || always_fail {
        let body = stream::unfold(0, move |phase| async move {
            if phase == 0 && !always_fail {
                return Some((
                    Ok((start..start + 16384)
                        .map(|v| (v % 251) as u8)
                        .collect::<Vec<_>>()),
                    1,
                ));
            }
            if phase >= 2 {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            Some((
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "fixture drop",
                )),
                2,
            ))
        });
        builder.body(Body::from_stream(body)).unwrap()
    } else {
        builder
            .body(Body::from(
                (start..=end).map(|v| (v % 251) as u8).collect::<Vec<_>>(),
            ))
            .unwrap()
    }
}
async fn serve(router: Router) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (format!("http://{address}"), task)
}
async fn fixture(
    path: &str,
) -> (
    VodEngine,
    redis::aio::MultiplexedConnection,
    i64,
    JoinHandle<()>,
) {
    fixture_with_user(path, None).await
}
async fn fixture_with_user(
    path: &str,
    user: Option<User>,
) -> (
    VodEngine,
    redis::aio::MultiplexedConnection,
    i64,
    JoinHandle<()>,
) {
    let url = std::env::var("REDIS_TEST_URL").expect("disposable REDIS_TEST_URL required");
    let mut connection = redis::Client::open(url.clone())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let id = (u64::from_le_bytes(uuid::Uuid::new_v4().as_bytes()[..8].try_into().unwrap())
        & 0x3fff_ffff_ffff_ffff) as i64
        + 1;
    let attempts = Arc::new(AtomicUsize::new(0));
    let retry_state = attempts.clone();
    let (base, task) = serve(
        Router::new()
            .route("/bytes", get(origin))
            .route("/paced", get(paced))
            .route(
                "/retry",
                get(move |request: Request| retry_origin(request, retry_state.clone(), false)),
            )
            .route(
                "/always-error",
                get(move |request: Request| retry_origin(request, attempts.clone(), true)),
            )
            .route("/error", get(|| async { StatusCode::BAD_GATEWAY })),
    )
    .await;
    let config = LeaseConfig {
        namespace: format!("test:vod:{}", uuid::Uuid::new_v4()),
        ttl: Duration::from_secs(10),
        heartbeat_interval: Some(Duration::from_secs(1)),
        ..Default::default()
    };
    let coordinator = Coordinator::connect(&url, config).await.unwrap();
    let selected_path = if matches!(path, "fallback" | "preempt-full" | "redirect") {
        "paced"
    } else {
        path
    };
    let mut candidates = vec![VodCandidate {
        relation_id: 1,
        account_id: 1,
        profile_id: id,
        max_streams: if user.is_some() && path != "preempt-full" {
            2
        } else {
            1
        },
        url: format!("{base}/{selected_path}"),
        user_agent: "VOD-contract-test".into(),
    }];
    if path == "fallback" {
        candidates.push(VodCandidate {
            relation_id: 2,
            account_id: 2,
            profile_id: id + 1,
            max_streams: 1,
            url: format!("{base}/paced"),
            user_agent: "VOD-contract-test".into(),
        });
    }
    let resolver = Arc::new(Resolver {
        source: VodSource {
            kind: ContentKind::Movie,
            content_uuid: uuid::Uuid::new_v4().to_string(),
            name: "fixture".into(),
            user: user.clone(),
            redirect: path == "redirect",
            limit_policy: dispatcharr_contracts::UserLimitPolicy {
                terminate_on_limit_exceeded: path == "preempt-full",
                ..Default::default()
            },
            candidates,
        },
    });
    let engine = VodEngine::with_resolver(
        resolver,
        coordinator,
        &url,
        VodConfig {
            idle_grace: Duration::from_millis(20),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let _: () = connection
        .del(format!("profile_connections:{id}"))
        .await
        .unwrap();
    (engine, connection, id, task)
}
fn request(session: &str, range: Option<&str>) -> VodRequest {
    VodRequest {
        resolved_client_ip: None,
        playback: PlaybackRequest {
            channel_id: "fixture".into(),
            query: vec![],
            headers: range
                .map(|v| vec![("range".into(), v.into())])
                .unwrap_or_default(),
            peer_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        },
        kind: "movie".into(),
        content_id: "fixture".into(),
        session_id: Some(session.into()),
        profile_id: None,
    }
}
async fn clean(connection: &mut redis::aio::MultiplexedConnection, session: &str, profile: i64) {
    for _ in 0..300 {
        let exists: bool = connection
            .exists(format!("vod_persistent_connection:{session}"))
            .await
            .unwrap();
        let count: i64 = connection
            .get(format!("profile_connections:{profile}"))
            .await
            .unwrap_or(0);
        if !exists && count == 0 {
            let _: () = connection
                .del(format!("profile_connections:{profile}"))
                .await
                .unwrap();
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("session metadata/provider reservation did not clean up");
}
#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn python_range_suffix_if_range_and_416_quirks_are_preserved() {
    let (engine, mut redis, profile, origin) = fixture("bytes").await;
    let session = format!("test-{}", uuid::Uuid::new_v4());
    for (range, status, expected) in [
        (None, 200, (0..=255).collect::<Vec<u8>>()),
        (Some(""), 200, (0..=255).collect::<Vec<u8>>()),
        (Some("bytes=10-19"), 206, (10..=19).collect()),
        (Some("bytes=-10"), 206, (0..=10).collect()),
        (
            Some("bytes=900-"),
            416,
            b"Requested Range Not Satisfiable".to_vec(),
        ),
    ] {
        let response = engine.respond(request(&session, range)).await;
        assert_eq!(response.status().as_u16(), status);
        assert!(!response.headers().contains_key("etag"));
        if status == 416 {
            assert!(!response.headers().contains_key("content-range"));
        }
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap()
                .to_vec(),
            expected
        );
    }
    let mut conditional = request(&session, Some("bytes=10-19"));
    conditional
        .playback
        .headers
        .push(("if-range".into(), "different".into()));
    let response = engine.respond(conditional).await;
    assert_eq!(response.status(), 206);
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap()
            .to_vec(),
        (10..=19).collect::<Vec<u8>>()
    );
    clean(&mut redis, &session, profile).await;
    origin.abort();
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn concurrent_ranges_share_one_provider_slot_disconnect_and_stop_clean_up() {
    let (engine, mut redis, profile, origin) = fixture("paced").await;
    let session = format!("test-{}", uuid::Uuid::new_v4());
    let first = engine.respond(request(&session, None)).await;
    let second = engine.respond(request(&session, None)).await;
    assert_eq!(first.status(), 200);
    assert_eq!(second.status(), 200);
    assert_eq!(
        redis
            .get::<_, i64>(format!("profile_connections:{profile}"))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        redis
            .hget::<_, _, i64>(
                format!("vod_persistent_connection:{session}"),
                "active_streams"
            )
            .await
            .unwrap(),
        2
    );
    let blocked = format!("test-{}", uuid::Uuid::new_v4());
    assert_eq!(engine.respond(request(&blocked, None)).await.status(), 503);
    drop(first);
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        redis
            .get::<_, i64>(format!("profile_connections:{profile}"))
            .await
            .unwrap(),
        1
    );
    let _: () = redis
        .set_ex(format!("vod_proxy:client:{session}:stop"), "true", 5)
        .await
        .unwrap();
    assert!(tokio::time::timeout(
        Duration::from_secs(3),
        axum::body::to_bytes(second.into_body(), 1024 * 1024)
    )
    .await
    .unwrap()
    .is_err());
    clean(&mut redis, &session, profile).await;
    let _: () = redis
        .del(format!("vod_proxy:client:{session}:stop"))
        .await
        .unwrap();
    origin.abort();
}
#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn native_head_is_405_and_initial_provider_error_releases_slot() {
    let (engine, mut redis, profile, origin) = fixture("error").await;
    let session = format!("test-{}", uuid::Uuid::new_v4());
    let response = engine.respond(request(&session, None)).await;
    assert_eq!(response.status(), 500);
    let _ = axum::body::to_bytes(response.into_body(), 1024)
        .await
        .unwrap();
    clean(&mut redis, &session, profile).await;
    let (base, server) = serve(engine.router()).await;
    let response = reqwest::Client::new()
        .head(format!(
            "{base}/proxy/vod/movie/{}/session",
            uuid::Uuid::new_v4()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 405);
    assert_eq!(response.headers()["allow"], "GET, OPTIONS");
    server.abort();
    origin.abort();
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn idle_adoption_caches_length_and_deleted_metadata_is_not_resurrected() {
    let (engine, mut redis, profile, origin) = fixture("bytes").await;
    let session = format!("test-{}", uuid::Uuid::new_v4());
    let response = engine.respond(request(&session, None)).await;
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap()
            .len(),
        256
    );
    // Wait for request teardown, within the configured idle grace.
    for _ in 0..50 {
        let count: i64 = redis
            .hget(
                format!("vod_persistent_connection:{session}"),
                "active_streams",
            )
            .await
            .unwrap_or(-1);
        if count == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    let minted = format!("test-{}", uuid::Uuid::new_v4());
    let response = engine.respond(request(&minted, Some("bytes=-10"))).await;
    assert_eq!(response.status(), 206);
    assert_eq!(response.headers()["content-range"], "bytes 0-10/256");
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap()
            .to_vec(),
        (0..=10).collect::<Vec<u8>>()
    );
    assert!(!redis
        .exists::<_, bool>(format!("vod_persistent_connection:{minted}"))
        .await
        .unwrap());
    clean(&mut redis, &session, profile).await;
    origin.abort();

    let (engine, mut redis, profile, origin) = fixture("paced").await;
    let session = format!("test-{}", uuid::Uuid::new_v4());
    let response = engine.respond(request(&session, None)).await;
    let _: () = redis
        .del(format!("vod_persistent_connection:{session}"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(!redis
        .exists::<_, bool>(format!("vod_persistent_connection:{session}"))
        .await
        .unwrap());
    assert!(axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .is_err());
    clean(&mut redis, &session, profile).await;
    origin.abort();
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn stop_releases_provider_even_when_client_never_polls_body() {
    let (engine, mut redis, profile, origin) = fixture("paced").await;
    let session = format!("test-{}", uuid::Uuid::new_v4());
    let response = engine.respond(request(&session, None)).await;
    // Fill the bounded sender. Keep the body alive, deliberately unread.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _: () = redis
        .set_ex(format!("vod_proxy:client:{session}:stop"), "true", 5)
        .await
        .unwrap();
    clean(&mut redis, &session, profile).await;
    assert!(axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .is_err());
    let _: () = redis
        .del(format!("vod_proxy:client:{session}:stop"))
        .await
        .unwrap();
    origin.abort();
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn interrupted_body_resumes_exact_absolute_range_with_end_bound_and_cleanup() {
    for range in [None, Some("bytes=1000-50000")] {
        let (engine, mut redis, profile, origin) = fixture("retry").await;
        let session = format!("test-{}", uuid::Uuid::new_v4());
        let response = engine.respond(request(&session, range)).await;
        assert_eq!(
            response.status().as_u16(),
            if range.is_some() { 206 } else { 200 }
        );
        let bytes = tokio::time::timeout(
            Duration::from_secs(3),
            axum::body::to_bytes(response.into_body(), 100000),
        )
        .await
        .unwrap()
        .unwrap();
        let (start, end) = if range.is_some() {
            (1000, 50000)
        } else {
            (0, 65535)
        };
        assert_eq!(
            bytes.to_vec(),
            (start..=end).map(|v| (v % 251) as u8).collect::<Vec<_>>()
        );
        clean(&mut redis, &session, profile).await;
        origin.abort();
    }
    let (engine, mut redis, profile, origin) = fixture("always-error").await;
    let session = format!("test-{}", uuid::Uuid::new_v4());
    let response = engine.respond(request(&session, None)).await;
    assert!(tokio::time::timeout(
        Duration::from_secs(3),
        axum::body::to_bytes(response.into_body(), 100000)
    )
    .await
    .unwrap()
    .is_err());
    clean(&mut redis, &session, profile).await;
    origin.abort();
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn positive_user_limit_rejects_new_session_and_authenticated_reentry_without_provider_leak() {
    let user = User {
        id: (uuid::Uuid::new_v4().as_u128() % 1_000_000_000) as i64 + 1000,
        username: "fixture".into(),
        password: "!".into(),
        is_active: true,
        user_level: 0,
        stream_limit: 1,
        custom_properties: serde_json::json!({}),
    };
    let (engine, mut redis, profile, origin) = fixture_with_user("paced", Some(user)).await;
    let first_id = format!("test-{}", uuid::Uuid::new_v4());
    let response = engine.respond(request(&first_id, None)).await;
    assert_eq!(response.status(), 200);
    let second_id = format!("test-{}", uuid::Uuid::new_v4());
    let denied = engine.respond(request(&second_id, None)).await;
    assert_eq!(denied.status(), 429);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &axum::body::to_bytes(denied.into_body(), 1024)
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::json!({"error":"Stream limit exceeded (1 concurrent streams allowed)"})
    );
    // Source checks limits on authenticated requests even to the same session.
    assert_eq!(engine.respond(request(&first_id, None)).await.status(), 429);
    drop(response);
    clean(&mut redis, &first_id, profile).await;
    assert!(!redis
        .exists::<_, bool>(format!("vod_persistent_connection:{second_id}"))
        .await
        .unwrap());
    origin.abort();
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn provider_capacity_falls_back_without_touching_existing_primary_reservation() {
    let (engine, mut redis, profile, origin) = fixture("fallback").await;
    let _: () = redis
        .set(format!("profile_connections:{profile}"), 1)
        .await
        .unwrap();
    let session = format!("test-{}", uuid::Uuid::new_v4());
    let response = engine.respond(request(&session, None)).await;
    assert_eq!(response.status(), 200);
    assert_eq!(
        redis
            .hget::<_, _, i64>(
                format!("vod_persistent_connection:{session}"),
                "m3u_profile_id"
            )
            .await
            .unwrap(),
        profile + 1
    );
    assert_eq!(
        redis
            .get::<_, i64>(format!("profile_connections:{profile}"))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        redis
            .get::<_, i64>(format!("profile_connections:{}", profile + 1))
            .await
            .unwrap(),
        1
    );
    drop(response);
    clean(&mut redis, &session, profile + 1).await;
    assert_eq!(
        redis
            .get::<_, i64>(format!("profile_connections:{profile}"))
            .await
            .unwrap(),
        1
    );
    let _: () = redis
        .del(format!("profile_connections:{profile}"))
        .await
        .unwrap();
    origin.abort();
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn graceful_shutdown_releases_all_provider_metadata_with_held_unread_bodies() {
    let (engine, mut redis, profile, origin) = fixture("paced").await;
    let session = format!("test-{}", uuid::Uuid::new_v4());
    let mut held = Vec::new();
    for _ in 0..8 {
        let response = engine.respond(request(&session, None)).await;
        assert_eq!(response.status(), 200);
        held.push(response);
    }
    assert_eq!(
        redis
            .get::<_, i64>(format!("profile_connections:{profile}"))
            .await
            .unwrap(),
        1
    );
    // No downstream body is read or dropped until shutdown has completed.
    let started = std::time::Instant::now();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), engine.shutdown())
            .await
            .unwrap()
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(!redis
        .exists::<_, bool>(format!("vod_persistent_connection:{session}"))
        .await
        .unwrap());
    assert_eq!(
        redis
            .get::<_, i64>(format!("profile_connections:{profile}"))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        engine
            .respond(request(&format!("test-{}", uuid::Uuid::new_v4()), None))
            .await
            .status(),
        503
    );
    assert!(engine.shutdown().await);
    for response in held {
        assert!(axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .is_err());
    }
    let _: () = redis
        .del(format!("profile_connections:{profile}"))
        .await
        .unwrap();
    origin.abort();
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn missing_socket_peer_metadata_fails_closed_before_provider_admission() {
    let (engine, mut redis, profile, origin) = fixture("paced").await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = engine.router();
    // Deliberately omit the adapter required by the application router.
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let response = reqwest::Client::new()
        .get(format!(
            "http://{address}/proxy/vod/movie/{}/peer-test",
            uuid::Uuid::new_v4()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 501);
    assert!(!redis
        .exists::<_, bool>(format!("profile_connections:{profile}"))
        .await
        .unwrap());
    server.abort();
    origin.abort();
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn user_preemption_runs_before_a_full_provider_blocks_new_session() {
    let user = User {
        id: (uuid::Uuid::new_v4().as_u128() % 1_000_000_000) as i64 + 1000,
        username: "fixture".into(),
        password: "!".into(),
        is_active: true,
        user_level: 0,
        stream_limit: 1,
        custom_properties: serde_json::json!({}),
    };
    let (engine, mut redis, profile, origin) = fixture_with_user("preempt-full", Some(user)).await;
    let old_id = format!("test-{}", uuid::Uuid::new_v4());
    let new_id = format!("test-{}", uuid::Uuid::new_v4());
    let old = engine.respond(request(&old_id, None)).await;
    assert_eq!(old.status(), 200);
    assert_eq!(
        redis
            .get::<_, i64>(format!("profile_connections:{profile}"))
            .await
            .unwrap(),
        1
    );
    let incoming = engine.respond(request(&new_id, None)).await;
    assert!(matches!(incoming.status().as_u16(), 200 | 503));
    // The old viewer must have been preempted even if its provider slot has not
    // yet drained. Before the ordering fix provider-full returned without this.
    assert!(redis
        .exists::<_, bool>(format!("vod_proxy:client:{old_id}:stop"))
        .await
        .unwrap());
    drop(incoming);
    clean(&mut redis, &old_id, profile).await;
    clean(&mut redis, &new_id, profile).await;
    assert!(axum::body::to_bytes(old.into_body(), 1024 * 1024)
        .await
        .is_err());
    let retry_id = format!("test-{}", uuid::Uuid::new_v4());
    let retry = engine.respond(request(&retry_id, None)).await;
    assert_eq!(retry.status(), 200);
    drop(retry);
    clean(&mut redis, &retry_id, profile).await;
    let _: () = redis
        .del(format!("vod_proxy:client:{old_id}:stop"))
        .await
        .unwrap();
    origin.abort();
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn initial_transport_failure_is_source_500_without_provider_url_disclosure() {
    let (engine, mut redis, profile, origin) = fixture("bytes").await;
    origin.abort();
    tokio::task::yield_now().await;
    let session = format!("test-{}", uuid::Uuid::new_v4());
    let response = engine.respond(request(&session, None)).await;
    assert_eq!(response.status(), 500);
    assert_eq!(response.headers()["allow"], "GET, OPTIONS");
    assert_eq!(
        response.headers()["content-type"],
        "text/html; charset=utf-8"
    );
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap(),
        "Streaming error: VOD provider request failed"
    );
    clean(&mut redis, &session, profile).await;
}
#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn native_stop_client_http_contract_cancels_body_and_preserves_method_headers() {
    let (engine, mut redis, profile, origin) = fixture("paced").await;
    let session = format!("test-{}", uuid::Uuid::new_v4());
    let held = engine.respond(request(&session, None)).await;
    assert_eq!(held.status(), 200);
    let (base, server) = serve(engine.router()).await;
    let client = reqwest::Client::new();
    let url = format!("{base}/proxy/vod/stop_client/");
    let missing = client
        .post(&url)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 400);
    assert_eq!(missing.headers()["allow"], "POST, OPTIONS");
    assert_eq!(
        missing.json::<serde_json::Value>().await.unwrap(),
        serde_json::json!({"error":"No client_id provided"})
    );
    let response = client
        .post(&url)
        .json(&serde_json::json!({"client_id":session}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["allow"], "POST, OPTIONS");
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap(),
        serde_json::json!({"message":"VOD client stop signal sent","client_id":session,"stop_key":format!("vod_proxy:client:{session}:stop")})
    );
    clean(&mut redis, &session, profile).await;
    assert!(axum::body::to_bytes(held.into_body(), 1024 * 1024)
        .await
        .is_err());
    let _: () = redis
        .del(format!("vod_proxy:client:{session}:stop"))
        .await
        .unwrap();
    server.abort();
    origin.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn direct_redirect_checks_capacity_without_any_counter_reservation_even_concurrently() {
    let (engine, mut redis, profile, origin) = fixture("redirect").await;
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..64 {
        let engine = engine.clone();
        let mut request = request("unused", None);
        request.session_id = None;
        tasks.spawn(async move {
            let response = engine.respond(request).await;
            assert_eq!(response.status(), 302);
            assert!(response.headers()["location"]
                .to_str()
                .unwrap()
                .ends_with("/paced"));
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    assert!(!redis
        .exists::<_, bool>(format!("profile_connections:{profile}"))
        .await
        .unwrap());
    let _: () = redis
        .set(format!("profile_connections:{profile}"), 1)
        .await
        .unwrap();
    let mut full = request("unused", None);
    full.session_id = None;
    assert_eq!(engine.respond(full).await.status(), 503);
    assert_eq!(
        redis
            .get::<_, i64>(format!("profile_connections:{profile}"))
            .await
            .unwrap(),
        1
    );
    let _: () = redis
        .del(format!("profile_connections:{profile}"))
        .await
        .unwrap();
    assert!(engine.shutdown().await);
    origin.abort();
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn shutdown_reports_failed_quota_cleanup_instead_of_false_success() {
    let (engine, mut redis, profile, origin) = fixture("paced").await;
    let session = format!("test-{}", uuid::Uuid::new_v4());
    let held = engine.respond(request(&session, None)).await;
    assert_eq!(held.status(), 200);
    let _: () = redis
        .set(
            format!("profile_connections:{profile}"),
            "corrupt-fixture-counter",
        )
        .await
        .unwrap();
    assert!(!engine.shutdown().await);
    assert!(axum::body::to_bytes(held.into_body(), 1024 * 1024)
        .await
        .is_err());
    assert!(!redis
        .exists::<_, bool>(format!("vod_persistent_connection:{session}"))
        .await
        .unwrap());
    // Only remove the exact namespace derived from this test's unique owner.
    // This deliberately corrupt counter cannot be safely released by Lua.
    let owners = {
        let mut cursor = redis
            .scan_match::<_, String>(format!("test:vod:*:vod:session:{session}:owner"))
            .await
            .unwrap();
        let mut keys = Vec::new();
        while let Some(key) = cursor.next_item().await {
            keys.push(key);
        }
        keys
    };
    assert_eq!(owners.len(), 1);
    let suffix = format!(":vod:session:{session}:owner");
    let namespace = owners[0].strip_suffix(&suffix).unwrap();
    assert!(namespace.starts_with("test:vod:"));
    let keys = {
        let mut cursor = redis
            .scan_match::<_, String>(format!("{namespace}:*"))
            .await
            .unwrap();
        let mut keys = Vec::new();
        while let Some(key) = cursor.next_item().await {
            keys.push(key);
        }
        keys
    };
    let _: () = redis.del(&keys).await.unwrap();
    let _: () = redis
        .del(format!("profile_connections:{profile}"))
        .await
        .unwrap();
    origin.abort();
}
