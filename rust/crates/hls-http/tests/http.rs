//! Live Redis/HTTP regression gates; original Dispatcharr author credit retained.
use axum::{body::Body, http::Request, routing::get as route_get, Router};
use bytes::Bytes;
use dispatcharr_clients::{ClientConfig, ClientStore};
use dispatcharr_contracts::{
    BoxFuture, OutputFormat, PlaybackError, PlaybackRequest, PlaybackResolver, PlaybackSource,
    PlaybackViewer, UserLimitPolicy,
};
use dispatcharr_hls_http::{GatewayConfig, HlsGateway};
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
use tokio::{net::TcpListener, time};
use tower::ServiceExt;
struct Resolver;
impl PlaybackResolver for Resolver {
    fn resolve(&self, _: PlaybackRequest) -> BoxFuture<'_, Result<PlaybackSource, PlaybackError>> {
        Box::pin(async { Err(PlaybackError::new(404, "Not found")) })
    }
    fn authorize_admin(&self, _: PlaybackRequest) -> BoxFuture<'_, Result<(), PlaybackError>> {
        Box::pin(async { Err(PlaybackError::new(403, "Forbidden")) })
    }
    fn authorize_hls_owner(
        &self,
        request: PlaybackRequest,
        owner: Option<i64>,
    ) -> BoxFuture<'_, Result<(), PlaybackError>> {
        Box::pin(async move {
            if request
                .header("x-owner")
                .and_then(|text| text.parse::<i64>().ok())
                == owner
                && owner.is_some()
            {
                Ok(())
            } else {
                Err(PlaybackError::new(404, "Session not found"))
            }
        })
    }
}
fn source(channel: String, url: String) -> PlaybackSource {
    PlaybackSource {
        channel_id: channel,
        channel_name: "HLS fixture".into(),
        stream_id: 1,
        profile_id: 1,
        provider_profile_id: None,
        provider_max_streams: 0,
        url,
        user_agent: "Dispatcharr HLS tests".into(),
        headers: vec![],
        metadata: vec![],
        output_format: OutputFormat::Hls,
        shutdown_delay: Duration::ZERO,
        startup_behind: Duration::from_secs(2),
        alternates: vec![],
        viewer: Some(PlaybackViewer {
            user_id: Some(7),
            username: Some("upstream-credit-test".into()),
            ip_address: "127.0.0.1".parse().unwrap(),
            user_agent: "test".into(),
            stream_limit: 0,
            limit_policy: UserLimitPolicy::default(),
        }),
    }
}
fn packet(pid: u16, payload: &[u8], pusi: bool) -> [u8; 188] {
    let mut packet = [0xff; 188];
    packet[..4].copy_from_slice(&[
        0x47,
        ((pid >> 8) as u8) | if pusi { 0x40 } else { 0 },
        pid as u8,
        0x10,
    ]);
    packet[4..4 + payload.len()].copy_from_slice(payload);
    packet
}
fn clip() -> Bytes {
    if let Ok(path) = std::env::var("HLS_TEST_FIXTURE") {
        return Bytes::from(std::fs::read(path).expect("real TS fixture"));
    }
    let pat = packet(
        0,
        &[0, 0, 0xb0, 13, 0, 1, 0xc1, 0, 0, 0, 1, 0xf0, 0, 0, 0, 0, 0],
        true,
    );
    let pmt = packet(
        4096,
        &[
            0, 2, 0xb0, 18, 0, 1, 0xc1, 0, 0, 0xe1, 0, 0xf0, 0, 0x1b, 0xe1, 0, 0xf0, 0, 0, 0, 0, 0,
        ],
        true,
    );
    let mut data = Vec::from(pat);
    data.extend(pmt);
    for second in 0..20 {
        let pts = (second * 90_000) as u64;
        let payload = [
            0,
            0,
            1,
            0xe0,
            0,
            0,
            0x80,
            0x80,
            5,
            0x21 | (((pts >> 30) & 7) << 1) as u8,
            (pts >> 22) as u8,
            1 | (((pts >> 15) & 0x7f) << 1) as u8,
            (pts >> 7) as u8,
            1 | ((pts & 0x7f) << 1) as u8,
            0,
            0,
            0,
            1,
            0x67,
            1,
            0,
            0,
            0,
            1,
            0x68,
            1,
            0,
            0,
            0,
            1,
            0x65,
            1,
        ];
        data.extend(packet(256, &payload, true));
    }
    Bytes::from(data)
}
async fn setup(
    config: GatewayConfig,
    send_media: bool,
) -> (
    HlsGateway,
    StreamEngine,
    ClientStore,
    String,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    let url = std::env::var("REDIS_TEST_URL").expect("real Redis required");
    let counts = Arc::new(AtomicUsize::new(0));
    let active = counts.clone();
    let app=Router::new().route("/ts",route_get(move || {let active=active.clone();async move {
        active.fetch_add(1,Ordering::SeqCst);let data=clip();
        Body::from_stream(async_stream::stream! {if send_media {yield Ok::<_,Infallible>(data);} loop {time::sleep(Duration::from_millis(20)).await;yield Ok(Bytes::from_static(&[0x47;188]));}})
    }}));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}/ts", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let clients = ClientStore::connect(
        &url,
        ClientConfig {
            flush_interval: Duration::from_millis(100),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let engine = StreamEngine::new(EngineConfig {
        chunk_bytes: 188,
        flush_interval: Duration::from_millis(5),
        ..Default::default()
    })
    .unwrap();
    let gateway =
        HlsGateway::new(engine.clone(), clients.clone(), Arc::new(Resolver), config).unwrap();
    (gateway, engine, clients, origin, counts, server)
}
fn channel() -> String {
    format!(
        "hls-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
async fn mint(gateway: &HlsGateway, channel: &str, url: &str) -> String {
    let response = gateway
        .mint(source(channel.to_owned(), url.to_owned()))
        .await
        .unwrap();
    assert_eq!(response.status(), 302);
    response.headers()["X-Dispatcharr-Session-Token"]
        .to_str()
        .unwrap()
        .to_owned()
}
async fn get(gateway: &HlsGateway, path: String) -> axum::response::Response {
    gateway
        .router()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}
async fn until<F, Fut>(mut predicate: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = time::Instant::now() + Duration::from_secs(5);
    loop {
        if predicate().await {
            return;
        }
        assert!(time::Instant::now() < deadline, "condition timed out");
        time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
#[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
async fn shared_manager_token_scope_and_exact_owner_delete() {
    let (gateway, engine, clients, url, counts, server) =
        setup(GatewayConfig::default(), true).await;
    let channel = channel();
    let one = mint(&gateway, &channel, &url).await;
    let two = mint(&gateway, &channel, &url).await;
    let response = get(&gateway, format!("/proxy/hls/{one}/index.m3u8")).await;
    assert_eq!(response.status(), 200);
    let bytes = time::timeout(Duration::from_secs(3), response.into_body().collect())
        .await
        .unwrap()
        .unwrap()
        .to_bytes();
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(text.starts_with("#EXTM3U\n"));
    assert_eq!(text.matches("#EXTM3U").count(), 1);
    assert!(text.contains("#EXTINF:"));
    assert_eq!(
        counts.load(Ordering::SeqCst),
        1,
        "all token viewers share one native upstream"
    );
    if let Ok(directory) = std::env::var("HLS_TEST_CAPTURE") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join("index.m3u8"), text).unwrap();
        for uri in text.lines().filter(|line| line.ends_with(".ts")) {
            let response = get(&gateway, format!("/proxy/hls/{two}/{uri}")).await;
            assert_eq!(response.status(), 200);
            let data = response.into_body().collect().await.unwrap().to_bytes();
            std::fs::write(directory.join(uri), data).unwrap();
        }
    }
    let seq = text.lines().find(|line| line.ends_with(".ts")).unwrap();
    let segment = get(&gateway, format!("/proxy/hls/{two}/{seq}")).await;
    assert_eq!(segment.status(), 200);
    let media = segment.into_body().collect().await.unwrap().to_bytes();
    assert!(media.len() >= 188);
    assert_eq!(media[0], 0x47);
    let missing = get(&gateway, format!("/proxy/hls/{one}/999999.ts")).await;
    assert_eq!(missing.status(), 404);
    let request = |owner: &str| {
        Request::builder()
            .method("DELETE")
            .uri(format!("/api/proxy/hls/sessions/{one}/"))
            .header("x-owner", owner)
            .body(Body::empty())
            .unwrap()
    };
    assert_eq!(
        gateway
            .router()
            .oneshot(request("8"))
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(
        gateway
            .router()
            .oneshot(request("7"))
            .await
            .unwrap()
            .status(),
        204
    );
    assert_eq!(
        get(&gateway, format!("/proxy/hls/{one}/index.m3u8"))
            .await
            .status(),
        410
    );
    assert_eq!(
        get(&gateway, format!("/proxy/hls/{two}/index.m3u8"))
            .await
            .status(),
        200
    );
    gateway.shutdown().await;
    engine.shutdown();
    clients.shutdown().await;
    server.abort();
}

#[tokio::test]
#[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
async fn cold_disconnect_reconnect_grace_request_bound_and_idle_cleanup() {
    let config = GatewayConfig {
        max_requests_per_session: 1,
        session_idle_timeout: Duration::from_millis(650),
        reconnect_grace: Duration::from_millis(100),
        ..Default::default()
    };
    let (gateway, engine, clients, url, _, server) = setup(config, false).await;
    let channel = channel();
    let token = mint(&gateway, &channel, &url).await;
    let cold = get(&gateway, format!("/proxy/hls/{token}/index.m3u8")).await;
    assert_eq!(cold.status(), 200);
    let limited = get(&gateway, format!("/proxy/hls/{token}/index.m3u8")).await;
    assert_eq!(limited.status(), 429);
    drop(cold); // Even an unpolled response must release the Redis held-request count.
    time::sleep(Duration::from_millis(30)).await;
    let cold = get(&gateway, format!("/proxy/hls/{token}/index.m3u8")).await;
    assert_eq!(cold.status(), 200);
    time::sleep(Duration::from_millis(150)).await;
    assert!(
        clients.get_hls_session(&token).await.unwrap().is_some(),
        "reconnected request inhibits abandoned claim"
    );
    drop(cold);
    until(|| async { clients.get_hls_session(&token).await.unwrap().is_none() }).await;
    assert_eq!(
        get(&gateway, format!("/proxy/hls/{token}/index.m3u8"))
            .await
            .status(),
        410
    );
    gateway.shutdown().await;
    engine.shutdown();
    clients.shutdown().await;
    server.abort();
}

#[tokio::test]
#[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
async fn cold_comments_touch_stop_and_auth_header_fail_closed() {
    let (gateway, engine, clients, url, _, server) = setup(GatewayConfig::default(), false).await;
    let channel = channel();
    let token = mint(&gateway, &channel, &url).await;
    let response = get(&gateway, format!("/proxy/hls/{token}/index.m3u8")).await;
    let mut body = response.into_body();
    let header = body.frame().await.unwrap().unwrap().into_data().unwrap();
    assert_eq!(header.as_ref(), b"#EXTM3U\n");
    drop(header);
    let comment = time::timeout(Duration::from_secs(1), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert!(comment.starts_with(b"# dispatcharr: waiting "));
    drop(comment);
    let invalid = Request::builder()
        .method("DELETE")
        .uri(format!("/api/proxy/hls/sessions/{token}/"))
        .header(
            "authorization",
            axum::http::HeaderValue::from_bytes(&[255]).unwrap(),
        )
        .header("x-owner", "7")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        gateway.router().oneshot(invalid).await.unwrap().status(),
        401
    );
    let info = clients.get_hls_session(&token).await.unwrap().unwrap();
    clients
        .stop_client(&info.channel_id, &info.client_id)
        .await
        .unwrap();
    assert_eq!(
        get(&gateway, format!("/proxy/hls/{token}/index.m3u8"))
            .await
            .status(),
        410
    );
    drop(body);
    gateway.shutdown().await;
    engine.shutdown();
    clients.shutdown().await;
    server.abort();
}

#[tokio::test]
#[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
async fn response_frame_pins_channel_admission_after_session_stop() {
    let (gateway, engine, clients, url, _, server) = setup(
        GatewayConfig {
            max_managers: 1,
            max_sessions: 2,
            ..Default::default()
        },
        true,
    )
    .await;
    let first = channel();
    let token = mint(&gateway, &first, &url).await;
    let response = get(&gateway, format!("/proxy/hls/{token}/index.m3u8")).await;
    let playlist = time::timeout(Duration::from_secs(2), response.into_body().collect())
        .await
        .unwrap()
        .unwrap()
        .to_bytes();
    let seq = std::str::from_utf8(&playlist)
        .unwrap()
        .lines()
        .find(|line| line.ends_with(".ts"))
        .unwrap()
        .to_owned();
    drop(playlist);
    let response = get(&gateway, format!("/proxy/hls/{token}/{seq}")).await;
    let mut body = response.into_body();
    let retained = body.frame().await.unwrap().unwrap().into_data().unwrap();
    drop(body);
    let info = clients.get_hls_session(&token).await.unwrap().unwrap();
    clients.stop_client(&first, &info.client_id).await.unwrap();
    until(|| async {
        get(&gateway, format!("/proxy/hls/{token}/index.m3u8"))
            .await
            .status()
            == 410
    })
    .await;
    time::sleep(Duration::from_millis(50)).await;
    let next = channel();
    let failure = gateway
        .mint(source(next.clone(), url.clone()))
        .await
        .unwrap_err();
    assert_eq!(
        failure.status, 503,
        "old Bytes retains manager admission after stop"
    );
    let retained_clone = retained.clone();
    drop(retained);
    assert_eq!(
        gateway
            .mint(source(next.clone(), url.clone()))
            .await
            .unwrap_err()
            .status,
        503,
        "last clone retains admission"
    );
    drop(retained_clone);
    time::sleep(Duration::from_millis(50)).await;
    let next_token = mint(&gateway, &next, &url).await;
    assert_eq!(
        get(&gateway, format!("/proxy/hls/{next_token}/index.m3u8"))
            .await
            .status(),
        200
    );
    gateway.shutdown().await;
    engine.shutdown();
    clients.shutdown().await;
    server.abort();
}

#[tokio::test]
#[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
async fn idle_sessions_and_shutdown_remove_shared_descriptor_without_reviving_tokens() {
    let (gateway, engine, clients, url, _, server) = setup(
        GatewayConfig {
            session_idle_timeout: Duration::from_millis(150),
            ..Default::default()
        },
        true,
    )
    .await;
    let first = channel();
    let token = mint(&gateway, &first, &url).await;
    let response = get(&gateway, format!("/proxy/hls/{token}/index.m3u8")).await;
    time::timeout(Duration::from_secs(2), response.into_body().collect())
        .await
        .unwrap()
        .unwrap();
    until(|| async { clients.get_hls_session(&token).await.unwrap().is_none() }).await;
    assert_eq!(
        get(&gateway, format!("/proxy/hls/{token}/index.m3u8"))
            .await
            .status(),
        410
    );
    gateway.shutdown().await;
    let redis = redis::Client::open(std::env::var("REDIS_TEST_URL").unwrap()).unwrap();
    let mut connection = redis.get_multiplexed_async_connection().await.unwrap();
    let remaining: i64 = redis::cmd("EXISTS")
        .arg(format!("live:channel:{first}:output:hls:owner"))
        .arg(format!("live:channel:{first}:output:hls:playlist"))
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(remaining, 0);
    assert_eq!(
        gateway
            .mint(source(channel(), url.clone()))
            .await
            .unwrap_err()
            .status,
        503
    );
    engine.shutdown();
    clients.shutdown().await;
    server.abort();
}
