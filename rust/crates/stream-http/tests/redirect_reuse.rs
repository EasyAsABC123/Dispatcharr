//! Actual provider alias adoption and Source single-winner metadata lifecycle.
//! Raw TS payload is a transport control, not Source/codec acceptance.
use axum::{body::Body, routing::get, Router};
use bytes::Bytes;
use dispatcharr_clients::{ClientConfig, ClientStore};
use dispatcharr_contracts::{
    BoxFuture, PlaybackCandidate, PlaybackError, PlaybackRedirectPlan, PlaybackSource,
    PlaybackViewer, RedirectAliasResolver, RedirectAssignmentPlan, UserLimitPolicy,
};
use dispatcharr_coordination::{Coordinator, LeaseConfig};
use dispatcharr_input_store::{InputConfig, InputStore};
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
use tokio::{net::TcpListener, sync::Notify, time};
struct ActualCandidates(Vec<PlaybackCandidate>);
impl RedirectAliasResolver for ActualCandidates {
    fn candidates(&self) -> BoxFuture<'_, Result<Vec<PlaybackCandidate>, PlaybackError>> {
        Box::pin(async { Ok(self.0.clone()) })
    }
    fn resolve(
        &self,
        stream: i64,
        profile: i64,
    ) -> BoxFuture<'_, Result<Option<PlaybackCandidate>, PlaybackError>> {
        Box::pin(async move {
            Ok(self
                .0
                .iter()
                .find(|candidate| {
                    stream == candidate.stream_id && Some(profile) == candidate.provider_profile_id
                })
                .cloned())
        })
    }
}
#[tokio::test]
#[ignore = "requires real isolated Redis"]
async fn actual_redirect_finally_consumes_shared_slot_without_metadata_tick_resurrection_or_raw_retirement(
) {
    let url = std::env::var("REDIS_TEST_URL").expect("isolated Redis required");
    let channel = uuid::Uuid::new_v4().to_string();
    let numeric = 17;
    let profile = 99;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let gets = Arc::new(AtomicUsize::new(0));
    let head_enter = entered.clone();
    let head_release = release.clone();
    let count = gets.clone();
    let app=Router::new().route("/live",get(move||{let count=count.clone();async move{
  count.fetch_add(1,Ordering::SeqCst);
  Body::from_stream(async_stream::stream!{loop{let mut data=[0u8;188];data[..4].copy_from_slice(&[0x47,0,100,0x10]);yield Ok::<_,Infallible>(Bytes::copy_from_slice(&data));time::sleep(Duration::from_millis(2)).await;}})
 }}).head(move||{let enter=head_enter.clone();let release=head_release.clone();async move{enter.notify_one();release.notified().await;axum::http::StatusCode::OK}}));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let origin = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let candidate = PlaybackCandidate {
        input_command: None,
        stream_id: 11,
        provider_profile_id: Some(profile),
        provider_max_streams: 1,
        credential_pool: None,
        url: format!("http://{address}/live"),
        user_agent: "owned redirect control".into(),
        headers: vec![],
        metadata: vec![("channel_id".into(), numeric.to_string())],
    };
    let coordinator = Coordinator::connect(&url, LeaseConfig::default())
        .await
        .unwrap();
    let clients = ClientStore::connect(&url, ClientConfig::default())
        .await
        .unwrap();
    let inputs = InputStore::connect(&url, InputConfig::default())
        .await
        .unwrap();
    let engine = StreamEngine::new_with_shared_inputs(
        EngineConfig {
            max_channels: 4,
            chunk_bytes: 188 * 2,
            read_timeout: Duration::from_secs(10),
            ..Default::default()
        },
        Some(coordinator),
        Some(clients.clone()),
        Some(inputs),
    )
    .unwrap();
    let worker = engine.clone();
    let first_channel = channel.clone();
    let selected = candidate.clone();
    let first = tokio::spawn(async move {
        worker
            .redirect(PlaybackRedirectPlan {
                channel_id: first_channel,
                candidates: vec![],
                viewer: PlaybackViewer {
                    user_id: None,
                    username: None,
                    ip_address: "127.0.0.1".parse().unwrap(),
                    user_agent: "owned redirect control".into(),
                    stream_limit: 0,
                    limit_policy: UserLimitPolicy::default(),
                },
                capacity_only: false,
                assignment: Some(RedirectAssignmentPlan {
                    numeric_channel_id: numeric,
                    has_assigned_streams: true,
                    resolver: Arc::new(ActualCandidates(vec![selected])),
                }),
                validate_urls: true,
                selection_error: None,
            })
            .await
    });
    time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    let source = PlaybackSource {
        output_profile: None,
        input_command: None,
        channel_id: channel.clone(),
        channel_name: "owned inverse control".into(),
        stream_id: candidate.stream_id,
        profile_id: 1,
        provider_profile_id: candidate.provider_profile_id,
        provider_max_streams: 1,
        credential_pool: None,
        url: candidate.url,
        user_agent: candidate.user_agent,
        headers: vec![],
        metadata: candidate.metadata,
        output_format: Default::default(),
        shutdown_delay: Duration::ZERO,
        startup_behind: Duration::ZERO,
        alternates: vec![],
        viewer: None,
    };
    let mut raw = engine.play(source).await.unwrap().into_body();
    assert!(!time::timeout(Duration::from_secs(3), raw.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap()
        .is_empty());
    let mut conn = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let before: i64 = redis::cmd("GET")
        .arg("profile_connections:99")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(before, 1);
    assert_eq!(gets.load(Ordering::SeqCst), 1);
    release.notify_one();
    assert_eq!(first.await.unwrap().unwrap().status(), 302);
    for _ in 0..3 {
        time::sleep(Duration::from_millis(1100)).await;
        let observed: (i64, Option<String>) = redis::pipe()
            .get("profile_connections:99")
            .hget(format!("live:channel:{channel}:metadata"), "m3u_profile")
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(
            observed,
            (0, None),
            "real metadata worker must not resurrect a consumed Source slot token"
        );
        assert!(!raw
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap()
            .is_empty());
    }
    assert_eq!(gets.load(Ordering::SeqCst), 1);
    drop(raw);
    assert!(engine.shutdown_wait(Duration::from_secs(10)).await);
    clients.shutdown().await;
    origin.abort();
    let _ = origin.await;
}

#[tokio::test]
#[ignore = "requires real isolated Redis"]
async fn actual_redirect_failing_primary_uses_owned_single_slot_for_same_profile_backup() {
    use axum::http::StatusCode;
    use redis::AsyncCommands;

    let url = std::env::var("REDIS_TEST_URL").expect("isolated Redis required");
    let channel = uuid::Uuid::new_v4().to_string();
    let numeric = (uuid::Uuid::new_v4().as_u128() & 0x3fff_ffff) as i64 + 100;
    let profile = numeric + 1;
    let stream = numeric + 2;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let head_entered = entered.clone();
    let head_release = release.clone();
    let app = Router::new()
        .route(
            "/primary",
            get(|| async { StatusCode::BAD_GATEWAY })
                .head(|| async { StatusCode::METHOD_NOT_ALLOWED }),
        )
        .route(
            "/backup",
            get(|| async { StatusCode::NOT_FOUND }).head(move || {
                let entered = head_entered.clone();
                let release = head_release.clone();
                async move {
                    entered.notify_one();
                    release.notified().await;
                    StatusCode::NO_CONTENT
                }
            }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let origin = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let coordinator = Coordinator::connect(&url, LeaseConfig::default())
        .await
        .unwrap();
    let clients = ClientStore::connect(&url, ClientConfig::default())
        .await
        .unwrap();
    let engine = StreamEngine::new_with_services(
        EngineConfig::default(),
        Some(coordinator),
        Some(clients.clone()),
    )
    .unwrap();
    let first = PlaybackCandidate {
        input_command: None,
        stream_id: stream,
        provider_profile_id: Some(profile),
        provider_max_streams: 1,
        credential_pool: None,
        url: format!("http://{address}/primary"),
        user_agent: "owned redirect fixture".into(),
        headers: vec![],
        metadata: vec![],
    };
    let backup_url = format!("http://{address}/backup");
    let backup = PlaybackCandidate {
        stream_id: stream + 1,
        url: backup_url.clone(),
        ..first.clone()
    };
    let plan = PlaybackRedirectPlan {
        channel_id: channel.clone(),
        candidates: vec![],
        viewer: PlaybackViewer {
            user_id: None,
            username: None,
            ip_address: "127.0.0.1".parse().unwrap(),
            user_agent: "owned redirect fixture".into(),
            stream_limit: 0,
            limit_policy: UserLimitPolicy::default(),
        },
        capacity_only: false,
        assignment: Some(RedirectAssignmentPlan {
            numeric_channel_id: numeric,
            has_assigned_streams: true,
            resolver: Arc::new(ActualCandidates(vec![first, backup])),
        }),
        validate_urls: true,
        selection_error: None,
    };
    let worker = engine.clone();
    let result = tokio::spawn(async move { worker.redirect(plan).await });
    time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    let mut redis = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let counter = format!("profile_connections:{profile}");
    let channel_alias = format!("channel_stream:{numeric}");
    let primary_alias = format!("stream_profile:{stream}");
    assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 1);
    assert_eq!(redis.get::<_, i64>(&channel_alias).await.unwrap(), stream);
    assert_eq!(redis.get::<_, i64>(&primary_alias).await.unwrap(), profile);
    assert!(!redis
        .exists::<_, bool>(format!("stream_profile:{}", stream + 1))
        .await
        .unwrap());
    release.notify_one();
    let response = time::timeout(Duration::from_secs(3), result)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::FOUND);
    assert_eq!(response.headers()["location"], backup_url);
    assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 0);
    assert!(!redis.exists::<_, bool>(&channel_alias).await.unwrap());
    assert!(!redis.exists::<_, bool>(&primary_alias).await.unwrap());
    assert!(!redis
        .exists::<_, bool>(format!("live:channel:{channel}:owner"))
        .await
        .unwrap());
    assert!(engine.shutdown_wait(Duration::from_secs(3)).await);
    clients.shutdown().await;
    origin.abort();
}
