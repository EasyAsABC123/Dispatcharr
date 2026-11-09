//! HLS shutdown stays globally bounded when real Redis stalls.
//! Full original Dispatcharr author credit remains in ATTRIBUTION.md.
use axum::{body::Body, routing::get, Router};
use bytes::Bytes;
use dispatcharr_clients::{ClientConfig, ClientStore};
use dispatcharr_contracts::{
    BoxFuture, OutputFormat, PlaybackError, PlaybackRequest, PlaybackResolver, PlaybackSource,
    PlaybackViewer, UserLimitPolicy,
};
use dispatcharr_hls_http::HlsGateway;
use dispatcharr_stream_http::{EngineConfig, StreamEngine};
use std::{convert::Infallible, sync::Arc, time::Duration};
struct Resolver;
impl PlaybackResolver for Resolver {
    fn resolve(&self, _: PlaybackRequest) -> BoxFuture<'_, Result<PlaybackSource, PlaybackError>> {
        Box::pin(async { Err(PlaybackError::new(404, "Not found")) })
    }
    fn authorize_admin(&self, _: PlaybackRequest) -> BoxFuture<'_, Result<(), PlaybackError>> {
        Box::pin(async { Ok(()) })
    }
}
#[tokio::test]
#[ignore = "requires isolated real Redis"]
async fn shutdown_deadline_covers_all_sessions_and_managers_when_redis_stalls() {
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let clients = ClientStore::connect(
        &redis_url,
        ClientConfig {
            ttl: Duration::from_secs(3600),
            flush_interval: Duration::from_secs(300),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let app=Router::new().route("/ts",get(||async {Body::from_stream(async_stream::stream! {loop {yield Ok::<_,Infallible>(Bytes::from_static(&[0x47;188]));tokio::time::sleep(Duration::from_millis(10)).await;}})}));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/ts", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let engine = StreamEngine::new(EngineConfig {
        chunk_bytes: 188,
        ..Default::default()
    })
    .unwrap();
    let gateway = HlsGateway::new(
        engine.clone(),
        clients.clone(),
        Arc::new(Resolver),
        Default::default(),
    )
    .unwrap();
    let channel = format!("shutdown-{}", std::process::id());
    for _ in 0..40 {
        gateway
            .mint(PlaybackSource {
                channel_id: channel.clone(),
                channel_name: "shutdown".into(),
                stream_id: 1,
                profile_id: 1,
                provider_profile_id: None,
                provider_max_streams: 0,
                url: url.clone(),
                user_agent: "test".into(),
                headers: vec![],
                metadata: vec![],
                output_format: OutputFormat::Hls,
                shutdown_delay: Duration::ZERO,
                startup_behind: Duration::ZERO,
                alternates: vec![],
                viewer: Some(PlaybackViewer {
                    user_id: None,
                    username: None,
                    ip_address: "127.0.0.1".parse().unwrap(),
                    user_agent: "test".into(),
                    stream_limit: 0,
                    limit_policy: UserLimitPolicy::default(),
                }),
            })
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut connection = redis::Client::open(redis_url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(6000)
        .arg("ALL")
        .query_async(&mut connection)
        .await
        .unwrap();
    let began = tokio::time::Instant::now();
    tokio::time::timeout(Duration::from_millis(5600), gateway.shutdown())
        .await
        .expect("global five-second shutdown budget must cover every client and manager");
    assert!(began.elapsed() <= Duration::from_millis(5500));
    engine.shutdown();
    clients.shutdown().await;
    server.abort();
}
