//! Native body/proxy contracts with fake authorization resolution only.
//! HTTP, Redis, quota leases and body cancellation are actual implementations.
//! Source-bound Store/Auth/Docker decode remains a separate required gate.
use axum::{
    body::Body,
    extract::{Path, State},
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use dispatcharr_contracts::{BoxFuture, PlaybackRequest};
use dispatcharr_coordination::{Coordinator, LeaseConfig};
use dispatcharr_store::{ChannelIdentity, StoreError, User};
use dispatcharr_timeshift_core::ProviderCredentials;
use dispatcharr_timeshift_http::*;
use redis::AsyncCommands;
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicI32, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
struct Resolver {
    uuid: uuid::Uuid,
    profile: i64,
    origin: String,
    limit: AtomicI32,
    mode: Arc<AtomicUsize>,
}
impl Resolver {
    fn user(&self, id: i64) -> User {
        User {
            id,
            username: format!("fixture-{id}"),
            password: "!".into(),
            is_active: true,
            user_level: 1,
            stream_limit: self.limit.load(Ordering::SeqCst),
            custom_properties: json!({}),
        }
    }
    fn channel(&self) -> ChannelIdentity {
        ChannelIdentity {
            id: 123,
            uuid: self.uuid,
            name: "Fixture archive".into(),
            effective_name: "Fixture archive".into(),
        }
    }
}
impl SessionResolver for Resolver {
    fn authenticate<'a>(
        &'a self,
        _: &'a PlaybackRequest,
    ) -> BoxFuture<'a, Result<User, StoreError>> {
        Box::pin(async move { Ok(self.user(self.profile)) })
    }
    fn enabled<'a>(&'a self, _: &'a User) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async { Ok(true) })
    }
    fn channel<'a>(
        &'a self,
        _: &'a User,
        uuid: uuid::Uuid,
    ) -> BoxFuture<'a, Result<Option<SessionChannel>, StoreError>> {
        Box::pin(async move {
            Ok((uuid == self.uuid).then(|| SessionChannel {
                identity: self.channel(),
                is_catchup: true,
                has_catchup_streams: true,
            }))
        })
    }
    fn bound_channel(
        &self,
        id: i64,
        uuid: uuid::Uuid,
    ) -> BoxFuture<'_, Result<Option<ChannelIdentity>, StoreError>> {
        Box::pin(async move { Ok((id == 123 && uuid == self.uuid).then(|| self.channel())) })
    }
    fn user(&self, id: i64) -> BoxFuture<'_, Result<Option<User>, StoreError>> {
        Box::pin(async move { Ok(Some(self.user(id))) })
    }
}
impl ArchiveResolver for Resolver {
    fn authenticate<'a>(
        &'a self,
        request: &'a PlaybackRequest,
    ) -> BoxFuture<'a, Result<Option<User>, StoreError>> {
        Box::pin(async move {
            match request.header("authorization") {
                Some("Bearer other") => Ok(Some(self.user(99))),
                Some("Bearer bad") => Err(StoreError::Unauthorized("Invalid token")),
                _ => Ok(None),
            }
        })
    }
    fn network<'a>(&'a self, _: &'a PlaybackRequest) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async { Ok(true) })
    }
    fn source(&self, binding: SessionBinding) -> BoxFuture<'_, Result<CatchupSource, StoreError>> {
        Box::pin(async move {
            let paths = match self.mode.load(Ordering::SeqCst) {
                1 => vec!["403"; 7],
                2 => vec!["404"; 7],
                3 => vec!["500"; 7],
                4 => vec!["416"; 7],
                5 => vec!["held"],
                _ => vec!["html", "media"],
            };
            Ok(CatchupSource {
                user: binding.user,
                channel: binding.channel,
                timestamp: binding.start,
                duration_window: 35,
                redirect: false,
                limit_policy: dispatcharr_contracts::UserLimitPolicy {
                    terminate_on_limit_exceeded: false,
                    ..Default::default()
                },
                candidates: vec![CatchupCandidate {
                    stream_id: 321,
                    account_id: self.profile,
                    profile_id: self.profile,
                    max_streams: 1,
                    provider_stream_id: "fixture".into(),
                    provider_timestamp: "2026-11-09:11-00".into(),
                    provider_timezone: None,
                    credentials: ProviderCredentials {
                        server_url: self.origin.clone(),
                        username: "fixture".into(),
                        password: "fixture".into(),
                    },
                    urls: paths
                        .into_iter()
                        .map(|path| format!("{}/{path}", self.origin))
                        .collect(),
                    user_agent: "fixture-agent".into(),
                }],
            })
        })
    }
}
fn payload() -> Vec<u8> {
    let mut bytes = vec![0u8; 188 * 10000];
    for (index, packet) in bytes.chunks_mut(188).enumerate() {
        packet[0] = 0x47;
        packet[1] = (index % 256) as u8;
    }
    bytes
}
async fn origin(
    State(hits): State<Arc<AtomicUsize>>,
    Path(path): Path<String>,
    request: Request<Body>,
) -> Response {
    hits.fetch_add(1, Ordering::SeqCst);
    assert_eq!(request.headers()["accept-encoding"], "identity");
    match path.as_str() {
        "html" => (
            [("content-type", "text/html")],
            "<html>fixture rejection</html>",
        )
            .into_response(),
        "403" => StatusCode::FORBIDDEN.into_response(),
        "404" => StatusCode::NOT_FOUND.into_response(),
        "500" => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        "416" => (
            StatusCode::RANGE_NOT_SATISFIABLE,
            [("content-range", "bytes */1880000")],
        )
            .into_response(),
        _ => {
            let bytes = payload();
            if let Some(range) = request
                .headers()
                .get("range")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("bytes="))
            {
                let (start, end) = range.split_once('-').unwrap();
                let start = start.parse::<usize>().unwrap();
                let end = end.parse::<usize>().unwrap().min(bytes.len() - 1);
                if path == "held" {
                    let prefix =
                        bytes::Bytes::copy_from_slice(&bytes[start..=(start + 65535).min(end)]);
                    let body = Body::from_stream(futures_util::stream::unfold(
                        Some(prefix),
                        |first| async move {
                            if let Some(prefix) = first {
                                Some((Ok::<_, std::io::Error>(prefix), None))
                            } else {
                                std::future::pending().await
                            }
                        },
                    ));
                    return (
                        StatusCode::PARTIAL_CONTENT,
                        [
                            ("content-type", "video/mp2t"),
                            (
                                "content-range",
                                &format!("bytes {start}-{end}/{}", bytes.len()),
                            ),
                            ("content-length", &(end - start + 1).to_string()),
                        ],
                        body,
                    )
                        .into_response();
                }
                (
                    StatusCode::PARTIAL_CONTENT,
                    [
                        ("content-type", "video/mp2t"),
                        (
                            "content-range",
                            &format!("bytes {start}-{end}/{}", bytes.len()),
                        ),
                    ],
                    bytes[start..=end].to_vec(),
                )
                    .into_response()
            } else {
                let prefix = bytes::Bytes::copy_from_slice(&bytes[..65536]);
                let body = Body::from_stream(futures_util::stream::unfold(
                    Some(prefix),
                    |first| async move {
                        if let Some(prefix) = first {
                            Some((Ok::<_, std::io::Error>(prefix), None))
                        } else {
                            std::future::pending().await
                        }
                    },
                ));
                (
                    [
                        ("content-type", "video/mp2t"),
                        ("content-length", "1880000"),
                    ],
                    body,
                )
                    .into_response()
            }
        }
    }
}
struct Fixture {
    engine: CatchupMedia,
    sessions: CatchupSessions,
    resolver: Arc<Resolver>,
    base: String,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    redis: redis::aio::MultiplexedConnection,
    hits: Arc<AtomicUsize>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
impl Fixture {
    async fn mint(&self) -> CatchupSession {
        self.sessions
            .create(
                &self.resolver.user(self.resolver.profile),
                self.resolver.uuid,
                "2026-11-09T11:00:00Z".into(),
                Some(30),
            )
            .await
            .unwrap()
    }
    async fn cleanup(&mut self) {
        assert!(self.engine.shutdown().await);
        let _: () = self
            .redis
            .del(&[
                format!("profile_connections:{}", self.resolver.profile),
                format!(":1:timeshift:format_idx:{}", self.resolver.profile),
            ])
            .await
            .unwrap();
    }
}
async fn fixture() -> Fixture {
    let url = std::env::var("REDIS_TEST_URL").expect("disposable Redis required");
    let hits = Arc::new(AtomicUsize::new(0));
    let router = Router::new()
        .route("/{path}", get(origin))
        .with_state(hits.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("http://{}", listener.local_addr().unwrap());
    let origin_task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let profile = (uuid::Uuid::new_v4().as_u128() % 1_000_000_000_000) as i64 + 1000;
    let resolver = Arc::new(Resolver {
        uuid: uuid::Uuid::new_v4(),
        profile,
        origin: upstream,
        limit: AtomicI32::new(0),
        mode: Arc::new(AtomicUsize::new(0)),
    });
    let sessions = CatchupSessions::with_resolver(resolver.clone(), &url, SessionConfig::default())
        .await
        .unwrap();
    let coordinator = Coordinator::connect(
        &url,
        LeaseConfig {
            namespace: format!("test:archive-http:{}", uuid::Uuid::new_v4()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let config = CatchupMediaConfig {
        pool: ArchivePoolConfig {
            max_sessions: 8,
            startup_idle: Duration::from_millis(200),
            disconnect_grace: Duration::from_millis(200),
            shutdown_timeout: Duration::from_secs(2),
            ..Default::default()
        },
        ..Default::default()
    };
    let engine = CatchupMedia::with_resolver(
        resolver.clone(),
        sessions.clone(),
        coordinator,
        &url,
        config,
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = engine.clone().router();
    let media_task = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let redis = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    Fixture {
        engine,
        sessions,
        resolver,
        base,
        tasks: vec![origin_task, media_task],
        redis,
        hits,
    }
}
#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback HTTP"]
async fn busy_tail_probe_preserves_active_quota_metadata_and_shutdown_cancels_held_probe_body() {
    let mut fixture = fixture().await;
    fixture.resolver.mode.store(5, Ordering::SeqCst);
    let session = fixture.mint().await;
    let route = session.json()["playback_url"].as_str().unwrap().to_owned();
    let client = reqwest::Client::new();
    let first = client
        .get(format!("{}{}", fixture.base, route))
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 200);
    let pool = format!("timeshift:pool:{}", session.session_id);
    let counter = format!("profile_connections:{}", fixture.resolver.profile);
    let before: std::collections::BTreeMap<String, String> =
        fixture.redis.hgetall(&pool).await.unwrap();
    let probe = client
        .get(format!("{}{}", fixture.base, route))
        .header("Range", "bytes=100-355")
        .send()
        .await
        .unwrap();
    assert_eq!(probe.status(), 206);
    assert_eq!(probe.bytes().await.unwrap().as_ref(), &payload()[100..=355]);
    let after: std::collections::BTreeMap<String, String> =
        fixture.redis.hgetall(&pool).await.unwrap();
    assert_eq!(before, after);
    assert_eq!(fixture.redis.get::<_, i64>(&counter).await.unwrap(), 1);
    let held = client
        .get(format!("{}{}", fixture.base, route))
        .header("Range", "bytes=100-")
        .send()
        .await
        .unwrap();
    assert_eq!(held.status(), 206);
    assert_eq!(fixture.redis.get::<_, i64>(&counter).await.unwrap(), 1);
    assert!(fixture.engine.shutdown().await);
    assert_eq!(fixture.redis.get::<_, i64>(&counter).await.unwrap(), 0);
    assert!(!fixture.redis.exists::<_, bool>(&pool).await.unwrap());
    let hits = fixture.hits.load(Ordering::SeqCst);
    let after = client
        .get(format!("{}{}", fixture.base, route))
        .header("Range", "bytes=100-355")
        .send()
        .await
        .unwrap();
    assert_eq!(after.status(), 503);
    assert_eq!(fixture.hits.load(Ordering::SeqCst), hits);
    drop(held);
    drop(first);
    fixture.cleanup().await;
}
#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback HTTP"]
async fn native_capability_partial_bytes_match_without_token_and_wrong_owner_does_not_open_provider(
) {
    let mut fixture = fixture().await;
    let session = fixture.mint().await;
    let route = session.json()["playback_url"].as_str().unwrap().to_owned();
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}{}", fixture.base, route))
        .header("range", "bytes=7-100")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 206);
    assert_eq!(response.headers()["content-range"], "bytes 7-100/1880000");
    assert_eq!(
        response.bytes().await.unwrap().as_ref(),
        &payload()[7..=100]
    );
    let hits = fixture.hits.load(Ordering::SeqCst);
    let response = client
        .get(format!("{}{}", fixture.base, route))
        .bearer_auth("other")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);
    assert_eq!(response.text().await.unwrap(), "Access denied");
    assert_eq!(fixture.hits.load(Ordering::SeqCst), hits);
    fixture
        .sessions
        .delete(&session.session_id, fixture.resolver.profile)
        .await;
    let response = client
        .get(format!("{}{}", fixture.base, route))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);
    fixture.cleanup().await;
}
#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback HTTP"]
async fn provider_errors_and_terminal_416_release_profile_and_retain_api_session() {
    let mut fixture = fixture().await;
    let client = reqwest::Client::new();
    for (mode, status, body) in [
        (1, 403, "Provider denied access"),
        (2, 404, "Catch-up not available yet"),
        (3, 400, "Provider error"),
        (4, 416, ""),
    ] {
        fixture.resolver.mode.store(mode, Ordering::SeqCst);
        let session = fixture.mint().await;
        let route = session.json()["playback_url"].as_str().unwrap().to_owned();
        let response = client
            .get(format!("{}{}", fixture.base, route))
            .header("range", "bytes=0-255")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        if status == 416 {
            assert_eq!(response.headers()["content-range"], "bytes */1880000");
        }
        assert_eq!(response.text().await.unwrap(), body);
        assert_eq!(
            fixture
                .redis
                .get::<_, i64>(format!("profile_connections:{}", fixture.resolver.profile))
                .await
                .unwrap(),
            0
        );
        assert!(fixture
            .redis
            .exists::<_, bool>(format!("timeshift:session:{}", session.session_id))
            .await
            .unwrap());
        fixture
            .sessions
            .delete(&session.session_id, fixture.resolver.profile)
            .await;
    }
    fixture.cleanup().await;
}
#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback HTTP"]
async fn positive_user_limit_refuses_existing_live_then_shutdown_releases_unpolled_native_body() {
    let mut fixture = fixture().await;
    let client = reqwest::Client::new();
    fixture.resolver.limit.store(1, Ordering::SeqCst);
    let live_key = format!(
        "live:channel:{}:clients:{}",
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4()
    );
    let _: () = fixture
        .redis
        .hset_multiple(
            &live_key,
            &[
                ("user_id", fixture.resolver.profile.to_string()),
                ("connected_at", "1".into()),
            ],
        )
        .await
        .unwrap();
    let session = fixture.mint().await;
    let route = session.json()["playback_url"].as_str().unwrap().to_owned();
    let response = client
        .get(format!("{}{}", fixture.base, route))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);
    assert_eq!(response.text().await.unwrap(), "Stream limit exceeded");
    assert_eq!(fixture.hits.load(Ordering::SeqCst), 0);
    let _: () = fixture.redis.del(live_key).await.unwrap();
    let response = client
        .get(format!("{}{}", fixture.base, route))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        fixture
            .redis
            .get::<_, i64>(format!("profile_connections:{}", fixture.resolver.profile))
            .await
            .unwrap(),
        1
    );
    assert!(fixture.engine.shutdown().await);
    assert_eq!(
        fixture
            .redis
            .get::<_, i64>(format!("profile_connections:{}", fixture.resolver.profile))
            .await
            .unwrap(),
        0
    );
    drop(response);
    fixture
        .sessions
        .delete(&session.session_id, fixture.resolver.profile)
        .await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and loopback HTTP"]
async fn remaining_two_native_playback_error_cases_match_original_wire_capture() {
    let mut fixture = fixture().await;
    let original: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../scripts/rust-timeshift-contracts/python-session-goldens.json"
    ))
    .unwrap();
    let client = reqwest::Client::new();
    for (name, method) in [
        ("native-playback-missing-auth", reqwest::Method::GET),
        ("native-playback-head", reqwest::Method::HEAD),
    ] {
        let expected = original["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["case"] == name)
            .unwrap();
        let response = client
            .request(
                method,
                format!("{}/proxy/catchup/{}", fixture.base, fixture.resolver.uuid),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status().as_u16(),
            expected["status"].as_u64().unwrap() as u16
        );
        let mut actual_allow: Vec<_> = response.headers()["allow"]
            .to_str()
            .unwrap()
            .split(',')
            .map(str::trim)
            .collect();
        actual_allow.sort_unstable();
        let mut expected_allow: Vec<_> = expected["allow"]
            .as_str()
            .unwrap()
            .split(',')
            .map(str::trim)
            .collect();
        expected_allow.sort_unstable();
        assert_eq!(actual_allow, expected_allow);
        let body = response.bytes().await.unwrap();
        let actual = if body.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&body).unwrap()
        };
        assert_eq!(actual, expected["body"]);
    }
    assert_eq!(fixture.hits.load(Ordering::SeqCst), 0);
    fixture.cleanup().await;
}
