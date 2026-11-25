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
use dispatcharr_store::{RequestAuthMethod, StoreError, User};
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
    signed_viewers: Vec<User>,
}
impl VodResolver for Resolver {
    fn authorize<'a>(
        &'a self,
        _: &'a VodRequest,
    ) -> BoxFuture<'a, Result<Option<User>, StoreError>> {
        Box::pin(async { Ok(None) })
    }
    fn authorize_entry<'a>(
        &'a self,
        request: &'a VodRequest,
    ) -> BoxFuture<'a, Result<(Option<User>, Option<RequestAuthMethod>), StoreError>> {
        Box::pin(async move {
            let id = request
                .playback
                .header("fixture-viewer")
                .and_then(|value| value.parse::<i64>().ok());
            let user = self.signed_viewers.iter().find(|user| Some(user.id) == id);
            Ok((
                user.cloned(),
                user.map(|_| RequestAuthMethod::SignedBearerHeader),
            ))
        })
    }
    fn restore_user(&self, id: i64) -> BoxFuture<'_, Result<Option<User>, StoreError>> {
        Box::pin(async move {
            Ok(self
                .signed_viewers
                .iter()
                .find(|user| user.id == id)
                .cloned())
        })
    }
    fn resolve<'a>(
        &'a self,
        request: &'a VodRequest,
        user: Option<User>,
    ) -> BoxFuture<'a, Result<VodSource, StoreError>> {
        Box::pin(async move {
            let mut source = self.source.clone();
            if !self.signed_viewers.is_empty() {
                source.content_uuid.clone_from(&request.content_id);
                source.user = user;
            }
            Ok(source)
        })
    }
    fn default_redirect(&self) -> BoxFuture<'_, Result<bool, StoreError>> {
        Box::pin(async { Ok(self.source.redirect) })
    }
    fn admin<'a>(&'a self, _: &'a PlaybackRequest) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async { Ok(()) })
    }
}
async fn origin(request: Request) -> Response {
    let size: u32 = if request.uri().path() == "/wire-range" {
        4096
    } else {
        256
    };
    let range = request.headers().get("range").and_then(|v| v.to_str().ok());
    let (status, start, end) = match range {
        Some(value) => {
            let (start, end) = value
                .strip_prefix("bytes=")
                .unwrap()
                .split_once('-')
                .unwrap();
            if start.is_empty() {
                (
                    206,
                    size.saturating_sub(end.parse::<u32>().unwrap()),
                    size - 1,
                )
            } else {
                let start = start.parse::<u32>().unwrap();
                if start >= size {
                    return Response::builder()
                        .status(416)
                        .header("content-range", format!("bytes */{size}"))
                        .body(Body::empty())
                        .unwrap();
                }
                let end = if end.is_empty() {
                    size - 1
                } else {
                    end.parse::<u32>().unwrap()
                };
                // Reject an unrelated change that forwards If-Range.
                assert!(!request.headers().contains_key("if-range"));
                (206, start, end)
            }
        }
        None => (200, 0, size - 1),
    };
    let mut response = Response::builder()
        .status(status)
        .header("content-type", "video/mp4")
        .header("content-length", end - start + 1)
        .header("accept-ranges", "bytes")
        .header("etag", "\"fixture-v1\"")
        .header("last-modified", "Wed, 01 Jan 2020 00:00:00 GMT");
    if status == 206 {
        response = response.header("content-range", format!("bytes {start}-{end}/{size}"));
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
    let idle_grace = if path == "recovery-idle" {
        Duration::from_secs(1)
    } else {
        Duration::from_millis(20)
    };
    fixture_with_idle_grace(path, user, idle_grace).await
}
async fn fixture_with_idle_grace(
    path: &str,
    user: Option<User>,
    idle_grace: Duration,
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
            .route("/wire-range", get(origin))
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
    let recovery_namespace = config.namespace.clone();
    let coordinator = Coordinator::connect(&url, config).await.unwrap();
    let selected_path = if matches!(
        path,
        "recovery-idle"
            | "signed-supervised"
            | "signed-idle"
            | "signed-restart"
            | "signed-claim-timeout"
    ) {
        "bytes"
    } else if matches!(path, "fallback" | "preempt-full" | "redirect") {
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
        credential_pool: None,
        url: format!("{base}/{selected_path}"),
        user_agent: "VOD-contract-test".into(),
    }];
    if path == "fallback" {
        candidates.push(VodCandidate {
            relation_id: 2,
            account_id: 2,
            profile_id: id + 1,
            max_streams: 1,
            credential_pool: None,
            url: format!("{base}/paced"),
            user_agent: "VOD-contract-test".into(),
        });
    }
    let signed_viewers = if path.starts_with("signed-") {
        let prior = user.clone().unwrap();
        let requester = User {
            id: prior.id + 1,
            ..prior.clone()
        };
        let second_owner = User {
            id: prior.id + 2,
            ..prior.clone()
        };
        vec![prior, requester, second_owner]
    } else {
        Vec::new()
    };
    let resolver = Arc::new(Resolver {
        signed_viewers,
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
            idle_grace,
            max_requests: if path == "signed-claim-timeout" {
                1
            } else {
                VodConfig::default().max_requests
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let engine = if matches!(path, "recovery-idle" | "signed-supervised") {
        engine.with_redis_recovery(&recovery_namespace).unwrap()
    } else if path == "signed-restart" {
        engine
            .with_authenticated_redis_restart(&recovery_namespace)
            .unwrap()
    } else {
        engine
    };
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
async fn fresh_vod_ranges_match_original_selected_wire() {
    let (engine, mut redis, profile, origin) = fixture("wire-range").await;
    let (base, server) = serve(engine.clone().router()).await;
    let client = reqwest::Client::new();
    let movie = uuid::Uuid::new_v4();
    let head = client
        .head(format!("{base}/proxy/vod/movie/{movie}"))
        .header("range", "bytes=-256")
        .send()
        .await
        .unwrap();
    assert_eq!(head.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(head.headers()["allow"], "OPTIONS, GET");
    assert_eq!(head.headers()["content-type"], "application/json");
    assert_eq!(head.headers()["content-length"], "41");
    assert!(!head.headers().contains_key("content-range"));
    assert!(head.bytes().await.unwrap().is_empty());

    // Read the socket body directly because the Original advertises an extra
    // byte on the first suffix request and then closes the HTTP/1 connection.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let authority = base.strip_prefix("http://").unwrap();
    let session = format!("test-{}", uuid::Uuid::new_v4());
    let mut socket = tokio::net::TcpStream::connect(authority).await.unwrap();
    let message = format!(
        "GET /proxy/vod/movie/{movie}/{session} HTTP/1.1\r\nHost: {authority}\r\nRange: bytes=-257\r\n\r\n"
    );
    socket.write_all(message.as_bytes()).await.unwrap();
    let mut wire = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), socket.read_to_end(&mut wire))
        .await
        .unwrap()
        .unwrap();
    let split = wire
        .windows(4)
        .position(|piece| piece == b"\r\n\r\n")
        .unwrap();
    let fields = std::str::from_utf8(&wire[..split])
        .unwrap()
        .to_ascii_lowercase();
    assert!(fields.starts_with("http/1.1 206 "));
    assert!(fields.contains("\r\ncontent-length: 258"));
    assert!(fields.contains("\r\ncontent-range: bytes 0-257/4096"));
    let tail = (3839..=4095).map(|value| value as u8).collect::<Vec<_>>();
    assert_eq!(&wire[split + 4..], tail.as_slice());
    clean(&mut redis, &session, profile).await;

    // Every other case uses a new session.
    for (range, conditional, status, bounds) in [
        (Some("bytes=9000000-"), false, 500, None),
        (Some("bytes=0-255"), false, 206, Some((0, 255))),
        (Some("bytes=1000-1255"), false, 206, Some((1000, 1255))),
        (Some("bytes=1000-"), false, 206, Some((1000, 4095))),
        (Some("bytes=0-255"), true, 206, Some((0, 255))),
        (None, false, 200, None),
    ] {
        let session = format!("test-{}", uuid::Uuid::new_v4());
        let mut req = client.get(format!("{base}/proxy/vod/movie/{movie}/{session}"));
        if let Some(range) = range {
            req = req.header("range", range);
        }
        if conditional {
            req = req.header("if-range", "\"different\"");
        }
        let response = req.send().await.unwrap();
        assert_eq!(response.status().as_u16(), status);
        assert_eq!(response.headers()["allow"], "GET, OPTIONS");
        assert!(!response.headers().contains_key("etag"));
        let body = if status == 500 {
            assert_eq!(
                response.headers()["content-type"],
                "text/html; charset=utf-8"
            );
            b"Streaming error: VOD provider request failed".to_vec()
        } else {
            assert_eq!(response.headers()["content-type"], "video/mp4");
            let (start, end) = bounds.unwrap_or((0, 4095));
            (start..=end).map(|value| value as u8).collect::<Vec<_>>()
        };
        if let Some((start, end)) = bounds {
            assert_eq!(
                response.headers()["content-range"].to_str().unwrap(),
                format!("bytes {start}-{end}/4096")
            );
        } else {
            assert!(!response.headers().contains_key("content-range"));
        }
        assert_eq!(
            response.headers()["content-length"]
                .to_str()
                .unwrap()
                .parse::<usize>()
                .unwrap(),
            body.len()
        );
        assert_eq!(response.bytes().await.unwrap().as_ref(), body.as_slice());
        clean(&mut redis, &session, profile).await;
    }
    assert!(engine.shutdown().await);
    server.abort();
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
    assert_eq!(response.headers()["allow"], "OPTIONS, GET");
    server.abort();
    origin.abort();
}

async fn wait_for_idle_metadata(
    redis: &mut redis::aio::MultiplexedConnection,
    metadata: &str,
    provider: &str,
) {
    tokio::time::timeout(Duration::from_millis(750), async {
        loop {
            let readers: Option<i64> = redis.hget(metadata, "active_streams").await.unwrap();
            let grants: Option<i64> = redis.get(provider).await.unwrap();
            if readers == Some(0) && grants.unwrap_or(0) == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

fn signed_full_request(viewer: i64, movie: &str, sid: Option<&str>) -> VodRequest {
    let mut request = request(sid.unwrap_or_default(), None);
    request.session_id = sid.map(str::to_owned);
    request.content_id = movie.to_owned();
    request
        .playback
        .headers
        .push(("fixture-viewer".into(), viewer.to_string()));
    request
}

fn fixture_signed_user() -> User {
    User {
        id: (uuid::Uuid::new_v4().as_u128() % 1_000_000_000) as i64 + 1000,
        username: "fixture-viewer".into(),
        password: "!".into(),
        is_active: true,
        user_level: 0,
        stream_limit: 1,
        custom_properties: serde_json::json!({}),
    }
}

async fn mint_signed_full(engine: &VodEngine, viewer: i64, movie: &str) -> String {
    let response = engine
        .respond(signed_full_request(viewer, movie, None))
        .await;
    assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
    response.headers()["location"]
        .to_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .to_owned()
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn authenticated_scoped_restart_preserves_pending_signed_claim() {
    let user = fixture_signed_user();
    let viewer = user.id;
    let (engine, mut redis, profile, origin) =
        fixture_with_idle_grace("signed-restart", Some(user), Duration::from_secs(5)).await;
    let movie = uuid::Uuid::new_v4().to_string();
    let sid = mint_signed_full(&engine, viewer, &movie).await;
    let follow = || signed_full_request(viewer, &movie, Some(&sid));
    let marker = format!("vod_signed_idle_alias:{sid}");
    let claim: String = redis.get(&marker).await.unwrap();
    let metadata = format!("vod_persistent_connection:{sid}");
    let stop = format!("vod_proxy:client:{sid}:stop");
    let provider = format!("profile_connections:{profile}");
    let first = engine.respond(follow()).await;
    assert_eq!(first.status(), StatusCode::OK);
    let old_token: String = redis.hget(&metadata, "rust_owner_token").await.unwrap();
    let owner_suffix = format!(":vod:session:{sid}:owner");
    let owner: String = redis
        .scan_match(format!("test:vod:*{owner_suffix}"))
        .await
        .unwrap()
        .next_item()
        .await
        .unwrap();
    let namespace = owner.strip_suffix(&owner_suffix).unwrap();
    let lease_token: String = redis.get(&owner).await.unwrap();
    let ledger = format!("{namespace}:lease:{lease_token}");
    let removed: usize = redis
        .del(&[&owner, &metadata, &ledger, &provider])
        .await
        .unwrap();
    assert_eq!(removed, 4);

    let _: () = redis.set_ex(&marker, "used", 300).await.unwrap();
    assert_eq!(
        engine.respond(follow()).await.status(),
        StatusCode::FORBIDDEN
    );
    let _: () = redis.set_ex(&marker, &claim, 300).await.unwrap();
    let _: () = redis.set_ex(&stop, "true", 300).await.unwrap();
    assert_eq!(
        engine.respond(follow()).await.status(),
        StatusCode::FORBIDDEN
    );
    let _: () = redis.del(&stop).await.unwrap();
    assert!(!redis.exists::<_, bool>(&metadata).await.unwrap());

    let reopened = engine.respond(follow()).await;
    assert_eq!(reopened.status(), StatusCode::OK);
    let token: String = redis.hget(&metadata, "rust_owner_token").await.unwrap();
    assert_ne!(token, old_token);
    assert_eq!(redis.get::<_, String>(&marker).await.unwrap(), claim);
    assert_eq!(
        redis.hget::<_, _, i64>(&metadata, "user_id").await.unwrap(),
        viewer
    );
    assert_eq!(redis.get::<_, i64>(&provider).await.unwrap(), 1);
    drop(first);
    assert_eq!(
        axum::body::to_bytes(reopened.into_body(), 256)
            .await
            .unwrap()
            .len(),
        256
    );
    assert!(engine.shutdown().await);
    clean(&mut redis, &sid, profile).await;
    origin.abort();
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn stalled_signed_claim_read_releases_the_only_request_permit() {
    let user = fixture_signed_user();
    let viewer = user.id;
    let (engine, mut redis, profile, origin) = fixture_with_idle_grace(
        "signed-claim-timeout",
        Some(user),
        Duration::from_millis(20),
    )
    .await;
    let movie = uuid::Uuid::new_v4().to_string();
    let sid = mint_signed_full(&engine, viewer, &movie).await;
    let follow = || signed_full_request(viewer, &movie, Some(&sid));
    let started = tokio::time::Instant::now();
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(3000)
        .arg("ALL")
        .query_async(&mut redis)
        .await
        .unwrap();
    let status = tokio::time::timeout(Duration::from_millis(2750), engine.respond(follow()))
        .await
        .unwrap()
        .status();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(started.elapsed() >= Duration::from_millis(1800));
    // Drop another caller while its detached read is paused. Its permit must
    // still be returned at the inner deadline.
    let _: () = redis::cmd("CLIENT")
        .arg("UNPAUSE")
        .query_async(&mut redis)
        .await
        .unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(3000)
        .arg("ALL")
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), engine.respond(follow()))
            .await
            .is_err()
    );
    tokio::time::sleep(Duration::from_millis(1850)).await;
    let next = engine.respond(follow()).await;
    assert_eq!(next.status(), StatusCode::OK);
    drop(next);
    clean(&mut redis, &sid, profile).await;
    assert!(engine.shutdown().await);
    origin.abort();
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn signed_idle_alias_preserves_invalid_seek_and_ambiguous_candidate_state() {
    let prior = User {
        id: (uuid::Uuid::new_v4().as_u128() % 1_000_000_000) as i64 + 1000,
        username: "prior".into(),
        password: "!".into(),
        is_active: true,
        user_level: 0,
        stream_limit: 1,
        custom_properties: serde_json::json!({}),
    };
    let requester = prior.id + 1;
    let (engine, mut redis, profile, origin) =
        fixture_with_idle_grace("signed-idle", Some(prior.clone()), Duration::from_secs(5)).await;
    let movie = uuid::Uuid::new_v4().to_string();
    let signed = |viewer: i64, sid: Option<&str>, range: &str| {
        let mut request = request(sid.unwrap_or_default(), Some(range));
        request.session_id = sid.map(str::to_owned);
        request.content_id = movie.clone();
        request.playback.headers.extend([
            ("user-agent".into(), "fixture-player".into()),
            ("fixture-viewer".into(), viewer.to_string()),
        ]);
        request
    };
    let mint = |response: Response| {
        assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
        response.headers()["location"]
            .to_str()
            .unwrap()
            .rsplit('/')
            .next()
            .unwrap()
            .to_owned()
    };
    let prior_sid = mint(engine.respond(signed(prior.id, None, "bytes=0-15")).await);
    let first = engine
        .respond(signed(prior.id, Some(&prior_sid), "bytes=0-15"))
        .await;
    assert_eq!(first.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        axum::body::to_bytes(first.into_body(), 256)
            .await
            .unwrap()
            .len(),
        16
    );
    let metadata = format!("vod_persistent_connection:{prior_sid}");
    let provider = format!("profile_connections:{profile}");
    wait_for_idle_metadata(&mut redis, &metadata, &provider).await;
    let alias = mint(
        engine
            .respond(signed(requester, None, "bytes=9000000-"))
            .await,
    );
    let marker = format!("vod_signed_idle_alias:{alias}");
    let before: (String, String, String) = redis
        .hget(
            &metadata,
            &["request_count", "active_streams", "last_activity"],
        )
        .await
        .unwrap();
    let claim: String = redis.get(&marker).await.unwrap();

    let invalid = engine
        .respond(signed(requester, Some(&alias), "bytes=9000000-"))
        .await;
    assert_eq!(invalid.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        axum::body::to_bytes(invalid.into_body(), 256)
            .await
            .unwrap(),
        "Requested Range Not Satisfiable"
    );
    assert_eq!(redis.get::<_, String>(&marker).await.unwrap(), claim);
    assert!(!redis
        .exists::<_, bool>(format!("vod_persistent_connection:{alias}"))
        .await
        .unwrap());
    let after: (String, String, String) = redis
        .hget(
            &metadata,
            &["request_count", "active_streams", "last_activity"],
        )
        .await
        .unwrap();
    assert_eq!(after, before);
    assert_eq!(
        redis
            .get::<_, Option<i64>>(&provider)
            .await
            .unwrap()
            .unwrap_or(0),
        0
    );

    let valid = engine
        .respond(signed(requester, Some(&alias), "bytes=0-15"))
        .await;
    let status = valid.status();
    if status != StatusCode::PARTIAL_CONTENT {
        let body = axum::body::to_bytes(valid.into_body(), 256).await.unwrap();
        panic!("expected retained range, got {status}: {body:?}");
    }
    assert_eq!(redis.get::<_, String>(&marker).await.unwrap(), "used");
    assert!(!redis
        .exists::<_, bool>(format!("vod_persistent_connection:{alias}"))
        .await
        .unwrap());
    assert_eq!(
        redis.hget::<_, _, i64>(&metadata, "user_id").await.unwrap(),
        prior.id
    );
    // A second owner gets its own row while the first retained row is active.
    let second_sid = mint(
        engine
            .respond(signed(prior.id + 2, None, "bytes=0-15"))
            .await,
    );
    let second = engine
        .respond(signed(prior.id + 2, Some(&second_sid), "bytes=0-15"))
        .await;
    assert_eq!(second.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        axum::body::to_bytes(second.into_body(), 256)
            .await
            .unwrap()
            .len(),
        16
    );
    drop(valid);
    let second_metadata = format!("vod_persistent_connection:{second_sid}");
    wait_for_idle_metadata(&mut redis, &metadata, &provider).await;
    wait_for_idle_metadata(&mut redis, &second_metadata, &provider).await;
    let ambiguous = mint(engine.respond(signed(requester, None, "bytes=0-15")).await);
    let own = engine
        .respond(signed(requester, Some(&ambiguous), "bytes=0-15"))
        .await;
    assert_eq!(own.status(), StatusCode::PARTIAL_CONTENT);
    let own_metadata = format!("vod_persistent_connection:{ambiguous}");
    assert_eq!(
        redis
            .hget::<_, _, i64>(&own_metadata, "user_id")
            .await
            .unwrap(),
        requester
    );
    assert_eq!(
        redis
            .hget::<_, _, i64>(&metadata, "request_count")
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        redis
            .hget::<_, _, i64>(&second_metadata, "request_count")
            .await
            .unwrap(),
        1
    );
    drop(own);
    wait_for_idle_metadata(&mut redis, &own_metadata, &provider).await;
    assert!(engine.shutdown().await);
    clean(&mut redis, &prior_sid, profile).await;
    origin.abort();
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn explicit_vod_id_reuses_only_its_own_idle_session() {
    let (engine, mut redis, profile, origin) =
        fixture_with_idle_grace("bytes", None, Duration::from_secs(2)).await;
    let session = format!("test-{}", uuid::Uuid::new_v4());
    let metadata = format!("vod_persistent_connection:{session}");
    let provider = format!("profile_connections:{profile}");
    let response = engine.respond(request(&session, None)).await;
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap()
            .len(),
        256
    );
    wait_for_idle_metadata(&mut redis, &metadata, &provider).await;
    let response = engine.respond(request(&session, Some("bytes=-10"))).await;
    assert_eq!(response.status(), 206);
    assert_eq!(response.headers()["content-range"], "bytes 0-10/256");
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap()
            .to_vec(),
        (0..=10).collect::<Vec<u8>>()
    );
    wait_for_idle_metadata(&mut redis, &metadata, &provider).await;
    let fresh = format!("test-{}", uuid::Uuid::new_v4());
    let response = engine.respond(request(&fresh, Some("bytes=-10"))).await;
    assert_eq!(response.status(), 206);
    // The public compatibility header normalizes the request independently
    // of the fresh upstream response's suffix bytes.
    assert_eq!(response.headers()["content-range"], "bytes 0-10/256");
    let old_count: i64 = redis.hget(&metadata, "request_count").await.unwrap();
    let new_count: i64 = redis
        .hget(
            format!("vod_persistent_connection:{fresh}"),
            "request_count",
        )
        .await
        .unwrap();
    assert_eq!((old_count, new_count), (2, 1));
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap()
            .to_vec(),
        (246..=255).collect::<Vec<u8>>()
    );
    clean(&mut redis, &session, profile).await;
    clean(&mut redis, &fresh, profile).await;
    origin.abort();
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn deleted_metadata_is_not_resurrected() {
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

#[tokio::test]
#[ignore = "requires actual owned Redis and loopback Source2 changed-ID recovery bridge"]
async fn recovery_idle_changed_requested_id_retains_one_metadata_identity() {
    let (engine, mut redis, profile, origin) = fixture("recovery-idle").await;
    let retained = format!("test-{}", uuid::Uuid::new_v4());
    let first = engine.respond(request(&retained, None)).await;
    assert_eq!(first.status(), 200);
    let bytes = axum::body::to_bytes(first.into_body(), 1024).await.unwrap();
    let metadata = format!("vod_persistent_connection:{retained}");
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if redis
                .hget::<_, _, i64>(&metadata, "active_streams")
                .await
                .unwrap_or(-1)
                == 0
                && redis
                    .get::<_, i64>(format!("profile_connections:{profile}"))
                    .await
                    .unwrap_or(-1)
                    == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let before: std::collections::BTreeMap<String, String> =
        redis.hgetall(&metadata).await.unwrap();
    let requested = format!("test-{}", uuid::Uuid::new_v4());
    let requested_key = format!("vod_persistent_connection:{requested}");
    let stop = format!("vod_proxy:client:{requested}:stop");
    let _: () = redis
        .hset(&requested_key, "rust_owner_token", "foreign")
        .await
        .unwrap();
    assert_eq!(
        engine.respond(request(&requested, None)).await.status(),
        StatusCode::NOT_IMPLEMENTED
    );
    assert_eq!(
        redis
            .hget::<_, _, String>(&requested_key, "rust_owner_token")
            .await
            .unwrap(),
        "foreign"
    );
    let _: () = redis.del(&requested_key).await.unwrap();
    let _: () = redis.set_ex(&stop, "true", 60).await.unwrap();
    assert_eq!(
        engine.respond(request(&requested, None)).await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let _: () = redis.del(&stop).await.unwrap();
    assert_eq!(
        redis
            .hgetall::<_, std::collections::BTreeMap<String, String>>(&metadata)
            .await
            .unwrap(),
        before
    );
    let response = engine.respond(request(&requested, None)).await;
    assert_eq!(response.status(), 200);
    let after: std::collections::BTreeMap<String, String> = redis.hgetall(&metadata).await.unwrap();
    assert_eq!(after["session_id"], retained);
    assert_eq!(after["created_at"], before["created_at"]);
    assert_eq!(
        after["request_count"].parse::<u64>().unwrap(),
        before["request_count"].parse::<u64>().unwrap() + 1
    );
    assert!(!redis
        .exists::<_, bool>(format!("vod_persistent_connection:{requested}"))
        .await
        .unwrap());
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap(),
        bytes
    );
    clean(&mut redis, &retained, profile).await;
    assert!(engine.shutdown().await);
    let owners: Vec<String> = {
        let mut scan = redis
            .scan_match::<_, String>(format!("*:vod:session:{retained}:owner"))
            .await
            .unwrap();
        let mut keys = Vec::new();
        while let Some(key) = scan.next_item().await {
            keys.push(key);
        }
        keys
    };
    assert!(owners.is_empty(), "retained generation owner must retire");
    origin.abort();
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback networking"]
async fn supervised_changed_id_keeps_different_viewers_separate() {
    let user = fixture_signed_user();
    let viewer = user.id;
    let (engine, mut redis, profile, origin) =
        fixture_with_idle_grace("signed-supervised", Some(user), Duration::from_secs(2)).await;
    let movie = uuid::Uuid::new_v4().to_string();
    let retained = format!("test-{}", uuid::Uuid::new_v4());
    let requested = format!("test-{}", uuid::Uuid::new_v4());
    let first = engine
        .respond(signed_full_request(viewer, &movie, Some(&retained)))
        .await;
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(
        axum::body::to_bytes(first.into_body(), 256)
            .await
            .unwrap()
            .len(),
        256
    );
    let metadata = format!("vod_persistent_connection:{retained}");
    let provider = format!("profile_connections:{profile}");
    wait_for_idle_metadata(&mut redis, &metadata, &provider).await;

    let second = engine
        .respond(signed_full_request(viewer + 1, &movie, Some(&requested)))
        .await;
    assert_eq!(second.status(), StatusCode::OK);
    let requested_metadata = format!("vod_persistent_connection:{requested}");
    assert_eq!(
        redis
            .hget::<_, _, i64>(&requested_metadata, "user_id")
            .await
            .unwrap(),
        viewer + 1
    );
    assert_eq!(
        redis.hget::<_, _, i64>(&metadata, "user_id").await.unwrap(),
        viewer
    );
    assert_eq!(
        redis
            .hget::<_, _, i64>(&metadata, "request_count")
            .await
            .unwrap(),
        1
    );
    drop(second);
    assert!(engine.shutdown().await);
    clean(&mut redis, &retained, profile).await;
    clean(&mut redis, &requested, profile).await;
    origin.abort();
}
