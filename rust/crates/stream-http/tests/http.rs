use axum::{
    body::Body, extract::State, http::StatusCode, response::Response, routing::get, Router,
};
use bytes::Bytes;
use dispatcharr_contracts::{
    BoxFuture, PlaybackCandidate, PlaybackError, PlaybackRequest, PlaybackResolver, PlaybackSource,
};
use dispatcharr_stream_http::{EngineConfig, StreamEngine};
use http_body_util::BodyExt;
use std::{
    convert::Infallible,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{net::TcpListener, task::JoinHandle, time};
use tower::ServiceExt;

#[derive(Default)]
struct Counts {
    connections: AtomicUsize,
    active: AtomicUsize,
}

struct Active(Arc<Counts>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn origin(State(counts): State<Arc<Counts>>) -> Response<Body> {
    packet_origin(counts, 100)
}

fn packet_origin(counts: Arc<Counts>, pid: u16) -> Response<Body> {
    counts.connections.fetch_add(1, Ordering::SeqCst);
    counts.active.fetch_add(1, Ordering::SeqCst);
    let guard = Active(counts);
    let stream = async_stream::stream! {
        let _guard = guard;
        let mut counter = 0u8;
        loop {
            let mut packet = vec![counter; 188];
            packet[..4].copy_from_slice(&[0x47, (pid >> 8) as u8, pid as u8, 0x10 | (counter & 15)]);
            yield Ok::<Bytes, Infallible>(Bytes::from(packet));
            counter = counter.wrapping_add(1);
            time::sleep(Duration::from_millis(1)).await;
        }
    };
    Response::builder()
        .header("Content-Type", "video/mp2t")
        .body(Body::from_stream(stream))
        .unwrap()
}

async fn start_origin() -> (String, Arc<Counts>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let counts = Arc::new(Counts::default());
    let app = Router::new()
        .route("/live", get(origin))
        .with_state(counts.clone());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{address}/live"), counts, task)
}

fn config() -> EngineConfig {
    EngineConfig {
        max_channels: 2,
        max_viewers_per_channel: 16,
        retained_bytes_per_channel: 188 * 256,
        chunk_bytes: 188 * 2,
        retention: Duration::from_secs(1),
        flush_interval: Duration::from_millis(5),
        connect_timeout: Duration::from_millis(200),
        read_timeout: Duration::from_millis(200),
        startup_timeout: Duration::from_secs(1),
        retry_delay: Duration::from_millis(10),
        max_retries: 0,
        ..EngineConfig::default()
    }
}

#[tokio::test]
async fn full_chunk_startup_waits_for_four_chunks_then_positions_at_live_head() {
    let (sender, receiver) = tokio::sync::mpsc::channel::<Bytes>(8);
    let receiver = Arc::new(std::sync::Mutex::new(Some(receiver)));
    let app = Router::new().route(
        "/live",
        get(move || {
            let mut receiver = receiver.lock().unwrap().take().unwrap();
            async move {
                Body::from_stream(async_stream::stream! {
                    while let Some(bytes) = receiver.recv().await {
                        yield Ok::<_, Infallible>(bytes);
                    }
                })
            }
        }),
    );
    let (base, origin) = serve(app).await;
    let mut limits = config();
    limits.initial_buffer_chunks = 4;
    limits.source_initialization = true;
    limits.flush_partial_chunks = false;
    limits.read_timeout = Duration::from_secs(1);
    let engine = StreamEngine::new(limits).unwrap();
    let running = engine.clone();
    let request = tokio::spawn(async move {
        running
            .play(source(format!("{base}/live"), "initial-buffer"))
            .await
    });
    let packet = |value: u8| {
        let mut bytes = vec![value; 188];
        bytes[0] = 0x47;
        bytes
    };
    for _ in 0..3 {
        sender
            .send(Bytes::from([packet(1), packet(1)].concat()))
            .await
            .unwrap();
    }
    sender.send(Bytes::from(packet(2))).await.unwrap();
    time::sleep(Duration::from_millis(30)).await;
    assert!(
        !request.is_finished(),
        "three complete chunks plus a partial chunk must stay initializing"
    );
    sender.send(Bytes::from(packet(2))).await.unwrap();
    let mut body = time::timeout(Duration::from_secs(1), request)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_body();
    assert!(
        time::timeout(Duration::from_millis(20), body.frame())
            .await
            .is_err(),
        "live-head positioning must skip the four initialization chunks"
    );
    sender
        .send(Bytes::from([packet(9), packet(9)].concat()))
        .await
        .unwrap();
    let bytes = time::timeout(Duration::from_secs(1), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert_eq!(bytes.len(), 376);
    assert_eq!(bytes[4], 9);
    drop(bytes);
    drop(body);
    drop(sender);
    assert!(engine.shutdown_wait(Duration::from_secs(2)).await);
    origin.abort();
}

#[tokio::test]
async fn source_mode_retains_partial_packets_and_full_chunk_accumulation_across_same_url_retry() {
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = requests.clone();
    let mut old = vec![1u8; 188];
    old[0] = 0x47;
    let mut new = vec![2u8; 188];
    new[0] = 0x47;
    let expected = [old.clone(), new.clone()].concat();
    let app = Router::new().route(
        "/live",
        get(move || {
            let attempt = counted.fetch_add(1, Ordering::SeqCst);
            let bytes = if attempt == 0 {
                old[..94].to_vec()
            } else {
                [old[94..].to_vec(), new.clone()].concat()
            };
            async move {
                Body::from_stream(async_stream::stream! {
                    yield Ok::<_,Infallible>(Bytes::from(bytes));
                    if attempt>0 {std::future::pending::<()>().await;}
                })
            }
        }),
    );
    let (base, origin) = serve(app).await;
    let mut limits = config();
    limits.source_initialization = true;
    limits.flush_partial_chunks = false;
    limits.max_retries = 1;
    let engine = StreamEngine::new(limits).unwrap();
    let mut playback = source(format!("{base}/live"), "retry-accumulator");
    playback.startup_behind = Duration::from_secs(10);
    let mut body = engine.play(playback).await.unwrap().into_body();
    let data = body.frame().await.unwrap().unwrap().into_data().unwrap();
    assert_eq!(data.as_ref(), expected);
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    drop(data);
    drop(body);
    assert!(engine.shutdown_wait(Duration::from_secs(1)).await);
    origin.abort();
}

fn source(url: String, channel: &str) -> PlaybackSource {
    PlaybackSource {
        channel_id: channel.into(),
        channel_name: "Fixture".into(),
        stream_id: 1,
        profile_id: 1,
        provider_profile_id: None,
        provider_max_streams: 0,
        url,
        user_agent: "Dispatcharr parity fixture".into(),
        headers: vec![],
        metadata: vec![],
        output_format: Default::default(),
        shutdown_delay: Duration::ZERO,
        startup_behind: Duration::ZERO,
        alternates: Vec::new(),
        viewer: None,
    }
}

async fn wait_for(mut condition: impl FnMut() -> bool) {
    time::timeout(Duration::from_secs(2), async {
        while !condition() {
            time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("condition timeout");
}

#[tokio::test]
async fn concurrent_viewers_share_upstream_and_last_drop_cleans_it() {
    let (url, counts, origin) = start_origin().await;
    let engine = StreamEngine::new(config()).unwrap();
    let (first, second) = tokio::join!(
        engine.play(source(url.clone(), "one")),
        engine.play(source(url, "one"))
    );
    let mut first = first.unwrap().into_body();
    let mut second = second.unwrap().into_body();
    assert_eq!(counts.connections.load(Ordering::SeqCst), 1);
    let a = first.frame().await.unwrap().unwrap().into_data().unwrap();
    let b = second.frame().await.unwrap().unwrap().into_data().unwrap();
    assert_eq!(a, b);
    assert_eq!(a.len() % 188, 0);
    assert_eq!(a[0], 0x47);
    drop((a, b, first, second));
    wait_for(|| engine.status().is_empty() && counts.active.load(Ordering::SeqCst) == 0).await;
    origin.abort();
}

#[tokio::test]
async fn outstanding_http_frame_keeps_viewer_and_channel_capacity_reserved() {
    let (url, counts, origin) = start_origin().await;
    let mut limits = config();
    limits.max_channels = 1;
    limits.max_viewers_per_channel = 1;
    let engine = StreamEngine::new(limits).unwrap();
    let mut body = engine
        .play(source(url.clone(), "one"))
        .await
        .unwrap()
        .into_body();
    let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
    drop(body);
    assert_eq!(engine.status()[0].viewer_count, 1);
    assert_eq!(
        engine
            .play(source(url.clone(), "one"))
            .await
            .unwrap_err()
            .status,
        503
    );
    engine.stop("one");
    assert_eq!(
        engine
            .play(source(url.clone(), "two"))
            .await
            .unwrap_err()
            .status,
        503
    );
    drop(frame);
    wait_for(|| counts.active.load(Ordering::SeqCst) == 0).await;
    let response = engine.play(source(url, "two")).await.unwrap();
    drop(response);
    engine.shutdown();
    origin.abort();
}

#[tokio::test]
async fn slow_reader_does_not_block_fast_reader_or_increase_retention_limit() {
    let (url, counts, origin) = start_origin().await;
    let mut limits = config();
    limits.retained_bytes_per_channel = 188 * 4;
    let max_bytes = limits.retained_bytes_per_channel;
    let engine = StreamEngine::new(limits).unwrap();
    let mut slow = engine
        .play(source(url.clone(), "one"))
        .await
        .unwrap()
        .into_body();
    let retained = slow.frame().await.unwrap().unwrap().into_data().unwrap();
    let mut fast = engine.play(source(url, "one")).await.unwrap().into_body();
    for _ in 0..30 {
        let data = time::timeout(Duration::from_secs(1), fast.frame())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        assert_eq!(data[0], 0x47);
        assert!(engine.status()[0].retained_bytes <= max_bytes);
    }
    assert_eq!(counts.connections.load(Ordering::SeqCst), 1);
    assert_eq!(retained[0], 0x47);
    drop((slow, retained, fast));
    wait_for(|| engine.status().is_empty()).await;
    origin.abort();
}

struct Resolver {
    source: PlaybackSource,
    resolutions: AtomicUsize,
}
impl PlaybackResolver for Resolver {
    fn resolve(
        &self,
        request: PlaybackRequest,
    ) -> BoxFuture<'_, Result<PlaybackSource, PlaybackError>> {
        self.resolutions.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if request.header("authorization") != Some("Bearer viewer") {
                Err(PlaybackError::new(403, "Forbidden"))
            } else {
                Ok(self.source.clone())
            }
        })
    }
    fn authorize_admin(
        &self,
        request: PlaybackRequest,
    ) -> BoxFuture<'_, Result<(), PlaybackError>> {
        Box::pin(async move {
            if request.header("authorization") == Some("Bearer admin") {
                Ok(())
            } else {
                Err(PlaybackError::new(403, "Forbidden"))
            }
        })
    }
}

#[tokio::test]
async fn router_authorizes_every_viewer_and_admin_operation() {
    let (url, counts, origin) = start_origin().await;
    let engine = StreamEngine::new(config()).unwrap();
    let resolver = Arc::new(Resolver {
        source: source(url, "one"),
        resolutions: AtomicUsize::new(0),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = engine.router(resolver.clone());
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let client = reqwest::Client::new();
    let base = format!("http://{address}");
    assert_eq!(
        client
            .get(format!("{base}/proxy/ts/stream/one"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(counts.connections.load(Ordering::SeqCst), 0);
    let first = client
        .get(format!("{base}/proxy/ts/stream/one"))
        .bearer_auth("viewer")
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.headers()["content-type"], "video/mp2t");
    assert_eq!(
        client
            .get(format!("{base}/proxy/ts/stream/one"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(resolver.resolutions.load(Ordering::SeqCst), 3);
    assert_eq!(
        client
            .get(format!("{base}/proxy/ts/status"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .post(format!("{base}/proxy/ts/stop/one"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .post(format!("{base}/proxy/ts/stop/one"))
            .bearer_auth("admin")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    drop(first);
    engine.shutdown();
    server.abort();
    origin.abort();
}

#[tokio::test]
async fn unsafe_or_unsupported_sources_fail_before_opening_connections() {
    let engine = StreamEngine::new(config()).unwrap();
    let mut limited = source("http://127.0.0.1:9/live".into(), "one");
    limited.provider_max_streams = 1;
    assert_eq!(engine.play(limited).await.unwrap_err().status, 503);
    assert_eq!(
        engine
            .play(source("file:///etc/passwd".into(), "one"))
            .await
            .unwrap_err()
            .status,
        502
    );
    assert!(engine.status().is_empty());
}

#[tokio::test]
async fn upstream_status_failure_is_reported_without_leaking_registry_capacity() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = Router::new().route("/failed", get(|| async { StatusCode::SERVICE_UNAVAILABLE }));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let engine = StreamEngine::new(config()).unwrap();
    let result = engine
        .play(source(format!("http://{address}/failed"), "bad"))
        .await;
    assert_eq!(result.unwrap_err().status, 502);
    wait_for(|| engine.status().is_empty()).await;
    let (url, counts, origin) = start_origin().await;
    let response = engine.play(source(url, "good")).await.unwrap();
    drop(response);
    wait_for(|| engine.status().is_empty() && counts.active.load(Ordering::SeqCst) == 0).await;
    server.abort();
    origin.abort();
}

#[tokio::test]
async fn upstream_retry_recovers_once_without_duplicate_active_inputs() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let counts = Arc::new(Counts::default());
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_route = attempts.clone();
    let origin_counts = counts.clone();
    let router = Router::new().route(
        "/retry",
        get(move || {
            let attempts = attempts_for_route.clone();
            let counts = origin_counts.clone();
            async move {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    Response::builder().status(503).body(Body::empty()).unwrap()
                } else {
                    origin(State(counts)).await
                }
            }
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut limits = config();
    limits.max_retries = 1;
    let engine = StreamEngine::new(limits).unwrap();
    let mut body = engine
        .play(source(format!("http://{address}/retry"), "one"))
        .await
        .unwrap()
        .into_body();
    let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(engine.status()[0].reconnects, 1);
    assert_eq!(counts.active.load(Ordering::SeqCst), 1);
    assert_eq!(frame[0], 0x47);
    assert_eq!(frame[5] & 0x80, 0x80);
    drop((frame, body));
    wait_for(|| engine.status().is_empty() && counts.active.load(Ordering::SeqCst) == 0).await;
    server.abort();
}

#[tokio::test]
async fn stalled_initialization_has_bounded_timeout_and_cleanup() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = Router::new().route(
        "/stall",
        get(|| async {
            time::sleep(Duration::from_secs(5)).await;
            Response::builder().body(Body::empty()).unwrap()
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut limits = config();
    limits.read_timeout = Duration::from_secs(5);
    limits.startup_timeout = Duration::from_millis(30);
    let engine = StreamEngine::new(limits).unwrap();
    let began = time::Instant::now();
    let failure = engine
        .play(source(format!("http://{address}/stall"), "one"))
        .await
        .unwrap_err();
    assert_eq!(failure.status, 504);
    assert!(began.elapsed() < Duration::from_secs(1));
    wait_for(|| engine.status().is_empty()).await;
    engine.shutdown();
    assert_eq!(
        engine
            .play(source(format!("http://{address}/stall"), "two"))
            .await
            .unwrap_err()
            .status,
        503
    );
    server.abort();
}

#[tokio::test]
async fn malformed_or_duplicate_authentication_never_downgrades_to_anonymous() {
    let engine = StreamEngine::new(config()).unwrap();
    let resolver = Arc::new(Resolver {
        source: source("http://127.0.0.1:9/live".into(), "one"),
        resolutions: AtomicUsize::new(0),
    });
    for header in ["authorization", "x-api-key"] {
        for duplicate in [false, true] {
            let mut request = axum::http::Request::builder()
                .uri("/proxy/ts/stream/one")
                .body(Body::empty())
                .unwrap();
            if duplicate {
                request
                    .headers_mut()
                    .append(header, "Bearer viewer".parse().unwrap());
                request
                    .headers_mut()
                    .append(header, "Bearer other".parse().unwrap());
            } else {
                request.headers_mut().insert(
                    header,
                    axum::http::HeaderValue::from_bytes(b"Bearer \xff").unwrap(),
                );
            }
            let response = engine
                .router(resolver.clone())
                .oneshot(request)
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
    }
    assert_eq!(resolver.resolutions.load(Ordering::SeqCst), 0);
    assert!(engine.status().is_empty());
}

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn source_metadata_is_promoted_before_native_response_and_cannot_regress_on_timer() {
    use dispatcharr_clients::{ClientConfig, ClientStore};
    use dispatcharr_coordination::{Coordinator, LeaseConfig};
    use dispatcharr_input_store::InputStore;
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let coordinator = Coordinator::connect(&redis_url, LeaseConfig::default())
        .await
        .unwrap();
    let clients = ClientStore::connect(&redis_url, ClientConfig::default())
        .await
        .unwrap();
    let inputs = InputStore::connect(&redis_url, Default::default())
        .await
        .unwrap();
    let mut limits = config();
    limits.source_initialization = true;
    limits.flush_partial_chunks = false;
    limits.initial_buffer_chunks = 4;
    let engine = StreamEngine::new_with_shared_inputs(
        limits,
        Some(coordinator),
        Some(clients.clone()),
        Some(inputs),
    )
    .unwrap();
    let (url, _, origin) = start_origin().await;
    let mut source = limited_source(url);
    source.startup_behind = Duration::from_secs(10);
    let channel = source.channel_id.clone();
    let body = engine.play(source).await.unwrap().into_body();
    let (metadata, index) = clients.channel_snapshot(&channel).await.unwrap().unwrap();
    assert_eq!(metadata.get("state").map(String::as_str), Some("active"));
    assert!(
        metadata
            .get("connection_ready_time")
            .unwrap()
            .parse::<f64>()
            .unwrap()
            > 0.0
    );
    assert_eq!(
        metadata.get("clients_at_activation").map(String::as_str),
        Some("1")
    );
    assert!(index >= 4);
    time::sleep(Duration::from_millis(1100)).await;
    let (metadata, _) = clients.channel_snapshot(&channel).await.unwrap().unwrap();
    assert_eq!(metadata.get("state").map(String::as_str), Some("active"));
    drop(body);
    assert!(engine.shutdown_wait(Duration::from_secs(2)).await);
    assert!(clients.channel_snapshot(&channel).await.unwrap().is_none());
    clients.shutdown().await;
    origin.abort();
}

#[tokio::test]
async fn native_source_rejects_partial_content_without_a_range_request() {
    let (base, origin) = serve(Router::new().route(
        "/partial",
        get(|| async {
            Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header("Content-Type", "video/mp2t")
                .body(Body::from(vec![0x47; 188 * 8]))
                .unwrap()
        }),
    ))
    .await;
    let engine = StreamEngine::new(config()).unwrap();
    assert_eq!(
        engine
            .play(source(format!("{base}/partial"), "unexpected206"))
            .await
            .unwrap_err()
            .status,
        502
    );
    assert!(engine.status().is_empty());
    origin.abort();
}

#[tokio::test]
async fn native_http_redirects_allow_fifteen_hops_and_reject_thirty_one() {
    use axum::extract::Path;
    let counts = Arc::new(Counts::default());
    let app = Router::new()
        .route(
            "/redirect/{remaining}",
            get(
                |Path(remaining): Path<u8>, State(counts): State<Arc<Counts>>| async move {
                    if remaining == 0 {
                        packet_origin(counts, 100)
                    } else {
                        Response::builder()
                            .status(StatusCode::FOUND)
                            .header("Location", format!("/redirect/{}", remaining - 1))
                            .body(Body::empty())
                            .unwrap()
                    }
                },
            ),
        )
        .with_state(counts.clone());
    let (base, origin) = serve(app).await;
    let engine = StreamEngine::new(config()).unwrap();
    let response = engine
        .play(source(format!("{base}/redirect/15"), "redirect15"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(counts.connections.load(Ordering::SeqCst), 1);
    drop(response);
    assert!(engine.shutdown_wait(Duration::from_secs(1)).await);
    let engine = StreamEngine::new(config()).unwrap();
    assert_eq!(
        engine
            .play(source(format!("{base}/redirect/31"), "redirect31"))
            .await
            .unwrap_err()
            .status,
        502
    );
    assert_eq!(counts.connections.load(Ordering::SeqCst), 1);
    origin.abort();
}

async fn redis_coordinator() -> (
    dispatcharr_coordination::Coordinator,
    redis::aio::MultiplexedConnection,
) {
    let url =
        std::env::var("REDIS_TEST_URL").expect("REDIS_TEST_URL must point to disposable Redis");
    let coordinator = dispatcharr_coordination::Coordinator::connect(
        &url,
        dispatcharr_coordination::LeaseConfig {
            namespace: format!("test:http:coord:{}", uuid_for_test()),
            ttl: Duration::from_millis(300),
            heartbeat_interval: Some(Duration::from_millis(30)),
            reap_interval: Some(Duration::from_millis(50)),
            command_timeout: Duration::from_secs(1),
            max_inflight_commands: 16,
            reap_batch: 128,
        },
    )
    .await
    .unwrap();
    let connection = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    (coordinator, connection)
}

fn uuid_for_test() -> String {
    use std::sync::atomic::AtomicU64;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

fn limited_source(url: String) -> PlaybackSource {
    let mut source = source(url, &uuid_for_test());
    source.provider_profile_id = Some(
        (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            % 1_000_000_000_000_000) as i64
            + 1,
    );
    source.provider_max_streams = 1;
    source
}

async fn wait_profile_zero(redis: &mut redis::aio::MultiplexedConnection, key: &str) {
    use redis::AsyncCommands;
    time::timeout(Duration::from_secs(2), async {
        loop {
            if redis.get::<_, i64>(key).await.unwrap() == 0 {
                break;
            }
            time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("profile reservation not released");
}

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn shutdown_drains_upstream_and_provider_even_while_http_frame_is_held() {
    let (coordinator, mut redis) = redis_coordinator().await;
    let (url, counts, origin) = start_origin().await;
    let source = limited_source(url);
    let profile_key = format!(
        "profile_connections:{}",
        source.provider_profile_id.unwrap()
    );
    let engine = StreamEngine::new_with_coordinator(config(), Some(coordinator)).unwrap();
    let mut body = engine.play(source.clone()).await.unwrap().into_body();
    let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
    let held = frame.clone();
    assert!(!held.is_empty());
    assert!(engine.shutdown_wait(Duration::from_secs(2)).await);
    wait_profile_zero(&mut redis, &profile_key).await;
    wait_for(|| counts.active.load(Ordering::SeqCst) == 0).await;
    assert!(engine.status().is_empty());
    assert_eq!(engine.play(source).await.unwrap_err().status, 503);
    // The downstream frame remains valid after the upstream and provider slot
    // have drained; dropping it must not be required to finish process cleanup.
    assert_eq!(held[0], 0x47);
    drop(frame);
    drop(held);
    drop(body);
    assert!(engine.shutdown_wait(Duration::from_millis(100)).await);
    origin.abort();
}

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn native_cold_failure_waits_for_ghost_and_returns_exact_error_packet_but_capacity_stays_json(
) {
    use dispatcharr_clients::{ClientConfig, ClientStore};
    use dispatcharr_coordination::{Coordinator, LeaseConfig};
    use redis::AsyncCommands;
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let coordinator = Coordinator::connect(&redis_url, LeaseConfig::default())
        .await
        .unwrap();
    let clients = ClientStore::connect(
        &redis_url,
        ClientConfig {
            ghost_timeout: Duration::from_millis(250),
            flush_interval: Duration::from_millis(20),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = requests.clone();
    let (base, origin) = serve(Router::new().route(
        "/fail",
        get(move || {
            counted.fetch_add(1, Ordering::SeqCst);
            async { StatusCode::SERVICE_UNAVAILABLE }
        }),
    ))
    .await;
    let mut limits = config();
    limits.source_initialization = true;
    limits.initial_buffer_chunks = 4;
    limits.flush_partial_chunks = false;
    let engine =
        StreamEngine::new_with_services(limits, Some(coordinator), Some(clients.clone())).unwrap();
    let mut source = limited_source(format!("{base}/fail"));
    source.shutdown_delay = Duration::from_millis(50);
    let profile_key = format!(
        "profile_connections:{}",
        source.provider_profile_id.unwrap()
    );
    let mut redis = redis::Client::open(redis_url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let running = engine.clone();
    let playback = source.clone();
    let started = time::Instant::now();
    let request = tokio::spawn(async move { running.play(playback).await });
    wait_for(|| requests.load(Ordering::SeqCst) > 0).await;
    time::sleep(Duration::from_millis(30)).await;
    assert!(
        !request.is_finished(),
        "an upstream503 is not a pre-stream JSON503"
    );
    assert_eq!(redis.get::<_, i64>(&profile_key).await.unwrap(), 1);
    let response = time::timeout(Duration::from_secs(2), request)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(200));
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "video/mp2t");
    assert_eq!(redis.get::<_, i64>(&profile_key).await.unwrap(), 1);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let mut expected = [0u8; 188];
    expected[..4].copy_from_slice(&[0x47, 0x1f, 0xff, 0]);
    let message = b"Error: Client disconnected";
    expected[4..4 + message.len()].copy_from_slice(message);
    assert_eq!(bytes.as_ref(), expected);
    drop(bytes);
    wait_profile_zero(&mut redis, &profile_key).await;
    let mut full = limited_source(format!("{base}/fail"));
    full.provider_profile_id = source.provider_profile_id;
    redis.set::<_, _, ()>(&profile_key, 1).await.unwrap();
    let count = requests.load(Ordering::SeqCst);
    let failure = engine.play(full).await.unwrap_err();
    assert_eq!(failure.status, 503);
    assert_eq!(requests.load(Ordering::SeqCst), count);
    assert_eq!(redis.get::<_, i64>(&profile_key).await.unwrap(), 1);
    redis.del::<_, ()>(&profile_key).await.unwrap();
    assert!(engine.shutdown_wait(Duration::from_secs(2)).await);
    clients.shutdown().await;
    origin.abort();
}

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn real_redis_http_provider_capacity_owner_fencing_and_stop_drop_release() {
    use redis::AsyncCommands;
    let (url, counts, origin) = start_origin().await;
    let (coordinator, mut redis) = redis_coordinator().await;
    let first = StreamEngine::new_with_coordinator(config(), Some(coordinator.clone())).unwrap();
    let other = StreamEngine::new_with_coordinator(config(), Some(coordinator)).unwrap();
    let source = limited_source(url);
    let profile_key = format!(
        "profile_connections:{}",
        source.provider_profile_id.unwrap()
    );
    let owner_key = format!("live:channel:{}:owner", source.channel_id);
    let a = first.play(source.clone()).await.unwrap();
    let b = first.play(source.clone()).await.unwrap();
    assert_eq!(redis.get::<_, i64>(&profile_key).await.unwrap(), 1);
    assert_eq!(counts.connections.load(Ordering::SeqCst), 1);
    assert_eq!(other.play(source.clone()).await.unwrap_err().status, 503);
    let mut competing_channel = source.clone();
    competing_channel.channel_id = uuid_for_test();
    assert_eq!(other.play(competing_channel).await.unwrap_err().status, 503);
    drop((a, b));
    wait_for(|| counts.active.load(Ordering::SeqCst) == 0).await;
    wait_profile_zero(&mut redis, &profile_key).await;
    assert!(!redis.exists::<_, bool>(&owner_key).await.unwrap());
    let response = other.play(source.clone()).await.unwrap();
    assert_eq!(redis.get::<_, i64>(&profile_key).await.unwrap(), 1);
    assert!(other.stop(&source.channel_id));
    wait_for(|| counts.active.load(Ordering::SeqCst) == 0).await;
    wait_profile_zero(&mut redis, &profile_key).await;
    drop(response);
    assert!(!redis.exists::<_, bool>(&owner_key).await.unwrap());
    first.shutdown();
    other.shutdown();
    origin.abort();
}

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn real_redis_lost_owner_stops_upstream_and_preserves_replacement_owner() {
    use redis::AsyncCommands;
    let (url, counts, origin) = start_origin().await;
    let (coordinator, mut redis) = redis_coordinator().await;
    let engine = StreamEngine::new_with_coordinator(config(), Some(coordinator)).unwrap();
    let source = limited_source(url);
    let profile_key = format!(
        "profile_connections:{}",
        source.provider_profile_id.unwrap()
    );
    let owner_key = format!("live:channel:{}:owner", source.channel_id);
    let mut body = engine.play(source).await.unwrap().into_body();
    let frame = body.frame().await.unwrap().unwrap();
    drop(frame);
    let _: () = redis
        .set_ex(&owner_key, "replacement-owner", 5)
        .await
        .unwrap();
    wait_for(|| counts.active.load(Ordering::SeqCst) == 0 && engine.status().is_empty()).await;
    wait_profile_zero(&mut redis, &profile_key).await;
    assert_eq!(
        redis.get::<_, String>(&owner_key).await.unwrap(),
        "replacement-owner"
    );
    assert!(body.frame().await.is_none());
    engine.shutdown();
    origin.abort();
}

fn candidate(url: String, stream_id: i64) -> PlaybackCandidate {
    PlaybackCandidate {
        stream_id,
        provider_profile_id: None,
        provider_max_streams: 0,
        url,
        user_agent: "Dispatcharr parity fixture".into(),
        headers: Vec::new(),
        metadata: vec![],
    }
}

async fn serve(router: Router) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{address}"), server)
}

fn ts_packet(pid: u16, value: u8) -> Vec<u8> {
    let mut packet = vec![value; 188];
    packet[..4].copy_from_slice(&[0x47, (pid >> 8) as u8, pid as u8, 0x10]);
    packet
}

#[tokio::test]
async fn cold_primary_failure_uses_ordered_backup_and_reports_current_stream() {
    let requests = Arc::new(AtomicUsize::new(0));
    let primary_requests = requests.clone();
    let counts = Arc::new(Counts::default());
    let backup_counts = counts.clone();
    let router = Router::new()
        .route(
            "/primary",
            get(move || {
                primary_requests.fetch_add(1, Ordering::SeqCst);
                async { StatusCode::SERVICE_UNAVAILABLE }
            }),
        )
        .route(
            "/backup",
            get(move || {
                let counts = backup_counts.clone();
                async move { packet_origin(counts, 200) }
            }),
        );
    let (base, server) = serve(router).await;
    let mut source = source(format!("{base}/primary"), "one");
    source
        .alternates
        .push(candidate(format!("{base}/backup"), 2));
    let engine = StreamEngine::new(config()).unwrap();
    let mut body = engine.play(source).await.unwrap().into_body();
    let bytes = body.frame().await.unwrap().unwrap().into_data().unwrap();
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    assert_eq!(counts.connections.load(Ordering::SeqCst), 1);
    assert_eq!(engine.status()[0].stream_id, 2);
    assert_eq!(bytes[2], 200);
    assert_eq!(bytes[5] & 0x80, 0x80);
    drop((bytes, body));
    wait_for(|| engine.status().is_empty()).await;
    server.abort();
}

#[tokio::test]
async fn midstream_failover_discards_partial_old_packet_and_marks_new_pid() {
    let counts = Arc::new(Counts::default());
    let backup_counts = counts.clone();
    let mut primary = [
        ts_packet(100, 0x55),
        ts_packet(100, 0x66),
        ts_packet(100, 0x77),
    ]
    .concat();
    primary.extend_from_slice(&[0x99; 17]);
    let router = Router::new()
        .route(
            "/primary",
            get(move || {
                let bytes = primary.clone();
                async move { Response::builder().body(Body::from(bytes)).unwrap() }
            }),
        )
        .route(
            "/backup",
            get(move || {
                let counts = backup_counts.clone();
                async move { packet_origin(counts, 200) }
            }),
        );
    let (base, server) = serve(router).await;
    let mut source = source(format!("{base}/primary"), "one");
    source
        .alternates
        .push(candidate(format!("{base}/backup"), 2));
    let engine = StreamEngine::new(config()).unwrap();
    let mut body = engine.play(source).await.unwrap().into_body();
    time::timeout(Duration::from_secs(1), async {
        loop {
            let bytes = body.frame().await.unwrap().unwrap().into_data().unwrap();
            assert_eq!(bytes.len() % 188, 0);
            for packet in bytes.chunks_exact(188) {
                assert_eq!(packet[0], 0x47);
                assert!(matches!(packet[2], 100 | 200));
                if packet[2] == 200 {
                    assert_eq!(packet[5] & 0x80, 0x80);
                    return;
                }
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(engine.status()[0].stream_id, 2);
    drop(body);
    wait_for(|| engine.status().is_empty()).await;
    server.abort();
}

#[tokio::test]
async fn exhausted_cold_sources_never_wrap_or_busy_spin() {
    let primary = Arc::new(AtomicUsize::new(0));
    let backup = Arc::new(AtomicUsize::new(0));
    let primary_requests = primary.clone();
    let backup_requests = backup.clone();
    let router = Router::new()
        .route(
            "/primary",
            get(move || {
                primary_requests.fetch_add(1, Ordering::SeqCst);
                async { StatusCode::SERVICE_UNAVAILABLE }
            }),
        )
        .route(
            "/backup",
            get(move || {
                backup_requests.fetch_add(1, Ordering::SeqCst);
                async { StatusCode::SERVICE_UNAVAILABLE }
            }),
        );
    let (base, server) = serve(router).await;
    let mut source = source(format!("{base}/primary"), "one");
    source
        .alternates
        .push(candidate(format!("{base}/backup"), 2));
    let mut limits = config();
    limits.max_retries = 1;
    limits.rotation_cooldown = Duration::from_millis(20);
    limits.max_stream_switches = 100;
    let engine = StreamEngine::new(limits).unwrap();
    let began = time::Instant::now();
    assert_eq!(engine.play(source).await.unwrap_err().status, 502);
    assert_eq!(primary.load(Ordering::SeqCst), 2);
    assert_eq!(backup.load(Ordering::SeqCst), 2);
    assert!(began.elapsed() >= Duration::from_millis(20));
    assert!(began.elapsed() < Duration::from_secs(1));
    assert!(engine.status().is_empty());
    server.abort();
}

#[tokio::test]
async fn post_media_exhaustion_waits_bounded_rotation_cooldowns() {
    let primary = Arc::new(AtomicUsize::new(0));
    let backup = Arc::new(AtomicUsize::new(0));
    let primary_requests = primary.clone();
    let backup_requests = backup.clone();
    let router = Router::new()
        .route(
            "/primary",
            get(move || {
                let first = primary_requests.fetch_add(1, Ordering::SeqCst) == 0;
                async move {
                    if first {
                        Response::builder()
                            .body(Body::from([ts_packet(100, 1), ts_packet(100, 2)].concat()))
                            .unwrap()
                    } else {
                        Response::builder().status(503).body(Body::empty()).unwrap()
                    }
                }
            }),
        )
        .route(
            "/backup",
            get(move || {
                backup_requests.fetch_add(1, Ordering::SeqCst);
                async { StatusCode::SERVICE_UNAVAILABLE }
            }),
        );
    let (base, server) = serve(router).await;
    let mut source = source(format!("{base}/primary"), "one");
    source
        .alternates
        .push(candidate(format!("{base}/backup"), 2));
    let mut limits = config();
    limits.rotation_cooldown = Duration::from_millis(80);
    limits.max_stream_switches = 2;
    let engine = StreamEngine::new(limits).unwrap();
    let began = time::Instant::now();
    let response = engine.play(source).await.unwrap();
    wait_for(|| engine.status().is_empty()).await;
    assert!(began.elapsed() >= Duration::from_millis(150));
    assert_eq!(primary.load(Ordering::SeqCst), 2);
    assert_eq!(backup.load(Ordering::SeqCst), 2);
    drop(response);
    server.abort();
}

#[tokio::test]
async fn stable_backup_rearms_earlier_source_without_waiting_for_wrap() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let primary_attempts = attempts.clone();
    let counts = Arc::new(Counts::default());
    let primary_counts = counts.clone();
    let router = Router::new()
        .route(
            "/primary",
            get(move || {
                let first = primary_attempts.fetch_add(1, Ordering::SeqCst) == 0;
                let counts = primary_counts.clone();
                async move {
                    if first {
                        Response::builder().status(503).body(Body::empty()).unwrap()
                    } else {
                        packet_origin(counts, 100)
                    }
                }
            }),
        )
        .route(
            "/backup",
            get(|| async {
                let stream = async_stream::stream! {
                    for _ in 0..8 {
                        yield Ok::<_, Infallible>(Bytes::from(ts_packet(200, 0x33)));
                        time::sleep(Duration::from_millis(20)).await;
                    }
                };
                Response::builder().body(Body::from_stream(stream)).unwrap()
            }),
        );
    let (base, server) = serve(router).await;
    let mut source = source(format!("{base}/primary"), "one");
    source
        .alternates
        .push(candidate(format!("{base}/backup"), 2));
    let mut limits = config();
    limits.stable_connection_threshold = Duration::from_millis(50);
    limits.rotation_cooldown = Duration::from_secs(2);
    let engine = StreamEngine::new(limits).unwrap();
    let response = engine.play(source).await.unwrap();
    wait_for(|| attempts.load(Ordering::SeqCst) == 2 && engine.status()[0].stream_id == 1).await;
    assert_eq!(counts.connections.load(Ordering::SeqCst), 1);
    drop(response);
    wait_for(|| engine.status().is_empty()).await;
    server.abort();
}

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn real_redis_full_primary_profile_falls_back_without_stealing_external_slot() {
    use redis::AsyncCommands;
    let primary_requests = Arc::new(AtomicUsize::new(0));
    let first_requests = primary_requests.clone();
    let counts = Arc::new(Counts::default());
    let backup_counts = counts.clone();
    let router = Router::new()
        .route(
            "/primary",
            get(move || {
                first_requests.fetch_add(1, Ordering::SeqCst);
                async { StatusCode::INTERNAL_SERVER_ERROR }
            }),
        )
        .route(
            "/backup",
            get(move || {
                let counts = backup_counts.clone();
                async move { packet_origin(counts, 200) }
            }),
        );
    let (base, server) = serve(router).await;
    let (coordinator, mut redis) = redis_coordinator().await;
    let mut source = limited_source(format!("{base}/primary"));
    let primary_profile = source.provider_profile_id.unwrap();
    let backup_profile = primary_profile + 1;
    let primary_key = format!("profile_connections:{primary_profile}");
    let backup_key = format!("profile_connections:{backup_profile}");
    let _: () = redis.set(&primary_key, 1).await.unwrap();
    let mut backup = candidate(format!("{base}/backup"), 2);
    backup.provider_profile_id = Some(backup_profile);
    backup.provider_max_streams = 1;
    source.alternates.push(backup);
    let engine = StreamEngine::new_with_coordinator(config(), Some(coordinator)).unwrap();
    let response = engine.play(source).await.unwrap();
    assert_eq!(primary_requests.load(Ordering::SeqCst), 0);
    assert_eq!(engine.status()[0].stream_id, 2);
    assert_eq!(engine.status()[0].provider_profile_id, Some(backup_profile));
    assert_eq!(redis.get::<_, i64>(&primary_key).await.unwrap(), 1);
    assert_eq!(redis.get::<_, i64>(&backup_key).await.unwrap(), 1);
    drop(response);
    wait_profile_zero(&mut redis, &backup_key).await;
    assert_eq!(redis.get::<_, i64>(&primary_key).await.unwrap(), 1);
    engine.shutdown();
    server.abort();
}

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn real_redis_midstream_handoff_releases_old_slot_before_backup_get_and_keeps_owner() {
    use redis::AsyncCommands;
    let (coordinator, mut redis) = redis_coordinator().await;
    let first_source = limited_source("http://placeholder".into());
    let primary_profile = first_source.provider_profile_id.unwrap();
    let backup_profile = primary_profile + 1;
    let primary_key = format!("profile_connections:{primary_profile}");
    let backup_key = format!("profile_connections:{backup_profile}");
    let owner_key = format!("live:channel:{}:owner", first_source.channel_id);
    let release = Arc::new(tokio::sync::Notify::new());
    let primary_release = release.clone();
    let correct_order = Arc::new(AtomicUsize::new(0));
    let order_probe = correct_order.clone();
    let counts = Arc::new(Counts::default());
    let backup_counts = counts.clone();
    let probe_redis = redis.clone();
    let probe_key = primary_key.clone();
    let router = Router::new().route("/primary", get(move || {
        let release = primary_release.clone();
        async move {
            let stream = async_stream::stream! {
                yield Ok::<_, Infallible>(Bytes::from([ts_packet(100, 1), ts_packet(100, 2)].concat()));
                release.notified().await;
                yield Ok::<_, Infallible>(Bytes::from_static(&[0x99; 17]));
            };
            Response::builder().body(Body::from_stream(stream)).unwrap()
        }
    })).route("/backup", get(move || {
        let counts = backup_counts.clone();
        let key = probe_key.clone();
        let mut redis = probe_redis.clone();
        let order = order_probe.clone();
        async move {
            if redis.get::<_, i64>(key).await.unwrap() == 0 {
                order.fetch_add(1, Ordering::SeqCst);
                packet_origin(counts, 200)
            } else { Response::builder().status(503).body(Body::empty()).unwrap() }
        }
    }));
    let (base, server) = serve(router).await;
    let mut source = first_source;
    source.url = format!("{base}/primary");
    let mut backup = candidate(format!("{base}/backup"), 2);
    backup.provider_profile_id = Some(backup_profile);
    backup.provider_max_streams = 1;
    source.alternates.push(backup);
    let engine = StreamEngine::new_with_coordinator(config(), Some(coordinator)).unwrap();
    let mut body = engine.play(source).await.unwrap().into_body();
    let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
    assert_eq!(frame[2], 100);
    let owner = redis.get::<_, String>(&owner_key).await.unwrap();
    assert_eq!(redis.get::<_, i64>(&primary_key).await.unwrap(), 1);
    drop(frame);
    release.notify_one();
    let frame = time::timeout(Duration::from_secs(1), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert_eq!(frame[2], 200);
    assert_eq!(frame[5] & 0x80, 0x80);
    assert_eq!(correct_order.load(Ordering::SeqCst), 1);
    assert_eq!(redis.get::<_, i64>(&primary_key).await.unwrap(), 0);
    assert_eq!(redis.get::<_, i64>(&backup_key).await.unwrap(), 1);
    assert_eq!(redis.get::<_, String>(&owner_key).await.unwrap(), owner);
    drop((frame, body));
    wait_profile_zero(&mut redis, &backup_key).await;
    assert_eq!(redis.get::<_, i64>(&primary_key).await.unwrap(), 0);
    engine.shutdown();
    server.abort();
}

#[tokio::test]
#[ignore = "requires disposable Redis; execute run-redis.py --http"]
async fn viewer_limit_metadata_stop_and_frame_lifetime_are_enforced_over_http() {
    use dispatcharr_clients::{ClientConfig, ClientStore};
    use dispatcharr_contracts::{PlaybackViewer, UserLimitPolicy};
    use redis::AsyncCommands;
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let clients = ClientStore::connect(
        &redis_url,
        ClientConfig {
            flush_interval: Duration::from_millis(25),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let (url, counts, origin) = start_origin().await;
    let engine = StreamEngine::new_with_services(config(), None, Some(clients.clone())).unwrap();
    let mut first = source(url.clone(), &uuid::Uuid::new_v4().to_string());
    let channel = first.channel_id.clone();
    first.viewer = Some(PlaybackViewer {
        user_id: Some((uuid::Uuid::new_v4().as_u128() % 1_000_000_000) as i64),
        username: Some("fixture".into()),
        ip_address: "127.0.0.1".parse().unwrap(),
        user_agent: "bounded-http-fixture".into(),
        stream_limit: 1,
        limit_policy: UserLimitPolicy {
            terminate_on_limit_exceeded: false,
            ..Default::default()
        },
    });
    let mut second = first.clone();
    second.channel_id = uuid::Uuid::new_v4().to_string();
    let mut body = engine.play(first).await.unwrap().into_body();
    let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
    assert!(!frame.is_empty());
    let failure = engine.play(second).await.unwrap_err();
    assert_eq!(failure.status, 429);
    assert_eq!(counts.connections.load(Ordering::SeqCst), 1);
    let mut redis = redis::Client::open(redis_url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let clients_key = format!("live:channel:{channel}:clients");
    let ids: Vec<String> = redis.smembers(&clients_key).await.unwrap();
    assert_eq!(ids.len(), 1);
    let metadata = format!("live:channel:{channel}:clients:{}", ids[0]);
    time::timeout(Duration::from_secs(3), async {
        loop {
            let bytes: Option<u64> = redis.hget(&metadata, "bytes_sent").await.unwrap();
            let published: Option<u64> = redis
                .get(format!("live:channel:{channel}:input:buffer:index"))
                .await
                .unwrap();
            if bytes.unwrap_or(0) > 0 && published.unwrap_or(0) > 0 {
                break;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        redis
            .hget::<_, _, String>(&metadata, "output_format")
            .await
            .unwrap(),
        "mpegts"
    );
    let projected = engine
        .legacy_status(&channel, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(projected["client_count"], 1);
    assert_eq!(projected["clients"][0]["output_format"], "mpegts");
    assert!(projected["buffer_index"].as_u64().unwrap() > 0);
    assert!(projected["total_bytes"].as_u64().unwrap() > 0);
    assert!(projected.get("viewer_count").is_none());
    let detailed = engine.legacy_status(&channel, true).await.unwrap().unwrap();
    assert_eq!(detailed["clients"][0]["user_agent"], "bounded-http-fixture");
    assert!(detailed["clients"][0]["bytes_sent"].as_u64().unwrap() > 0);
    drop(body);
    assert!(
        redis.exists::<_, bool>(&metadata).await.unwrap(),
        "held body frame retains client lease"
    );
    assert!(engine.stop_client(&channel, &ids[0]).await.unwrap());
    assert!(
        !redis.exists::<_, bool>(&metadata).await.unwrap(),
        "explicit stop releases accounting even if bytes remain held"
    );
    drop(frame);
    engine.shutdown();
    clients.shutdown().await;
    origin.abort();
}

#[tokio::test]
#[ignore = "requires disposable Redis; execute run-redis.py --http"]
async fn separate_workers_follow_shared_input_without_extra_upstream_or_provider_slot() {
    shared_workers(false).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis; execute run-redis.py --http"]
async fn stop_through_follower_cancels_owner_and_all_accounting_with_held_body_frame() {
    shared_workers(true).await;
}

async fn shared_workers(remote_stop: bool) {
    shared_workers_mode(remote_stop, false).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis; execute run-redis.py --http"]
async fn shared_source_followers_wait_for_committed_ready_metadata_before_media() {
    shared_workers_mode(false, true).await;
}

async fn shared_workers_mode(remote_stop: bool, source_mode: bool) {
    use dispatcharr_clients::{ClientConfig, ClientStore};
    use dispatcharr_coordination::{Coordinator, LeaseConfig};
    use dispatcharr_input_store::{InputConfig, InputStore};
    use redis::AsyncCommands;
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let coordinator = Coordinator::connect(&redis_url, LeaseConfig::default())
        .await
        .unwrap();
    let first_clients = ClientStore::connect(&redis_url, ClientConfig::default())
        .await
        .unwrap();
    let second_clients = ClientStore::connect(&redis_url, ClientConfig::default())
        .await
        .unwrap();
    let inputs = InputStore::connect(&redis_url, InputConfig::default())
        .await
        .unwrap();
    let mut limits = config();
    if source_mode {
        limits.source_initialization = true;
        limits.flush_partial_chunks = false;
        limits.initial_buffer_chunks = 4;
    }
    let first = StreamEngine::new_with_shared_inputs(
        limits.clone(),
        Some(coordinator.clone()),
        Some(first_clients.clone()),
        Some(inputs.clone()),
    )
    .unwrap();
    let second = StreamEngine::new_with_shared_inputs(
        limits,
        Some(coordinator),
        Some(second_clients.clone()),
        Some(inputs.clone()),
    )
    .unwrap();
    let (url, counts, origin) = start_origin().await;
    let channel = uuid::Uuid::new_v4().to_string();
    let profile = (uuid::Uuid::new_v4().as_u128() % 1_000_000_000) as i64 + 100;
    let mut source = source(url, &channel);
    source.provider_profile_id = Some(profile);
    source.provider_max_streams = 1;
    source.startup_behind = Duration::from_millis(25);
    let mut owner = first.play(source.clone()).await.unwrap().into_body();
    let bytes = owner.frame().await.unwrap().unwrap().into_data().unwrap();
    assert!(!bytes.is_empty());
    drop(bytes);
    let mut follower = second.play(source).await.unwrap().into_body();
    let bytes = follower
        .frame()
        .await
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert_eq!(bytes.len() % 188, 0);
    assert_eq!(bytes[0], 0x47);
    drop(bytes);
    assert_eq!(counts.connections.load(Ordering::SeqCst), 1);
    let mut redis = redis::Client::open(redis_url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let key = format!("profile_connections:{profile}");
    assert_eq!(redis.get::<_, u64>(&key).await.unwrap(), 1);
    let metadata = second
        .legacy_status(&channel, false)
        .await
        .unwrap()
        .unwrap();
    if source_mode {
        assert_eq!(metadata["state"], "active");
    }
    assert_eq!(metadata["client_count"], 2);
    drop(owner);
    // The owner must keep ingesting for viewers on another worker, even after
    // its final local HTTP reader disappears.
    time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(counts.active.load(Ordering::SeqCst), 1);
    for _ in 0..4 {
        let bytes = follower
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        assert_eq!(bytes[0], 0x47);
    }
    assert_eq!(counts.connections.load(Ordering::SeqCst), 1);
    let held = if remote_stop {
        let held = follower
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        assert!(second.stop_global(&channel).await.unwrap());
        assert_eq!(
            redis
                .scard::<_, usize>(format!("live:channel:{channel}:clients"))
                .await
                .unwrap(),
            0
        );
        Some(held)
    } else {
        None
    };
    if !remote_stop {
        drop(follower);
    } else {
        assert!(!held.as_ref().unwrap().is_empty());
        drop(follower);
    }
    time::timeout(Duration::from_secs(4), async {
        loop {
            if counts.active.load(Ordering::SeqCst) == 0
                && redis
                    .get::<_, Option<u64>>(&key)
                    .await
                    .unwrap()
                    .unwrap_or(0)
                    == 0
            {
                break;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(inputs.snapshot(&channel).await.unwrap().owner.is_none());
    if remote_stop {
        assert!(!redis
            .exists::<_, bool>(format!("live:channel:{channel}:stopping"))
            .await
            .unwrap());
    }
    drop(held);
    assert!(!redis
        .exists::<_, bool>(format!("live:channel:{channel}:input:buffer:index"))
        .await
        .unwrap());
    first.shutdown();
    second.shutdown();
    first_clients.shutdown().await;
    second_clients.shutdown().await;
    origin.abort();
}

#[tokio::test]
async fn head_authenticates_without_resolving_or_starting_an_origin() {
    let engine = StreamEngine::new(config()).unwrap();
    let resolver = Arc::new(Resolver {
        source: source("http://127.0.0.1:9/live".into(), "head"),
        resolutions: AtomicUsize::new(0),
    });
    for (path, auth, expected) in [
        ("/proxy/ts/stream/head", None, 405),
        ("/proxy/ts/stream/head", Some("Bearer supplied"), 501),
        ("/proxy/ts/status", None, 403),
        ("/proxy/ts/status/head", Some("Bearer admin"), 405),
    ] {
        let mut request = axum::http::Request::builder().method("HEAD").uri(path);
        if let Some(auth) = auth {
            request = request.header("authorization", auth);
        }
        assert_eq!(
            engine
                .router(resolver.clone())
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap()
                .status()
                .as_u16(),
            expected
        );
    }
    assert_eq!(resolver.resolutions.load(Ordering::SeqCst), 0);
    assert!(engine.status().is_empty());
}
