//! Real HTTP/valid local media state-loss contract for opt-in recovery.
//! Original Dispatcharr authors retain full application and algorithm credit.
//! Resolver is fixture-only: actual SQL/auth and actual Redis restart follow in
//! the packaged Source/native replay. This test removes only its own keys.
use axum::{body::Body, routing::get, Router};
use dispatcharr_contracts::{BoxFuture, PlaybackRequest};
use dispatcharr_coordination::{Coordinator, LeaseConfig};
use dispatcharr_store::{StoreError, User};
use dispatcharr_vod_http::{
    ContentKind, VodCandidate, VodConfig, VodEngine, VodRequest, VodResolver, VodSource,
};
use futures_util::stream;
use redis::AsyncCommands;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{net::TcpListener, task::JoinHandle};
struct Resolver(VodSource);
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
        Box::pin(async { Ok(self.0.clone()) })
    }
    fn default_redirect(&self) -> BoxFuture<'_, Result<bool, StoreError>> {
        Box::pin(async { Ok(false) })
    }
    fn admin<'a>(&'a self, _: &'a PlaybackRequest) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async { Ok(()) })
    }
}
async fn serve(router: Router) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (format!("http://{address}"), task)
}
struct Reader {
    response: reqwest::Response,
    pending: bytes::Bytes,
}
impl Reader {
    async fn take(&mut self, size: usize) -> Vec<u8> {
        assert!(size <= 8192);
        let mut output = Vec::with_capacity(size);
        while output.len() < size {
            if self.pending.is_empty() {
                self.pending = tokio::time::timeout(Duration::from_secs(12), self.response.chunk())
                    .await
                    .expect("bounded response read")
                    .expect("body error")
                    .expect("unexpected EOF");
                // HTTP client may coalesce unread chunks. Retain its bounded
                // immutable fixture frame and slice; never duplicate/accumulate it.
                assert!(self.pending.len() <= 5 * 1024 * 1024);
            }
            let copied = (size - output.len()).min(self.pending.len());
            output.extend_from_slice(&self.pending[..copied]);
            self.pending = self.pending.slice(copied..);
        }
        output
    }
}
async fn wait(mut condition: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(12), async {
        while !condition().await {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "owned valid MP4 + actual HTTP/Redis opt-in generation recovery"]
async fn actual_http_old_and_new_bodies_survive_state_loss_and_source_close_accounting() {
    run_case(false, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "owned valid MP4 + actual HTTP current-generation additional entry"]
async fn actual_http_recovered_current_additional_entry_reuses_provider() {
    run_case(true, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "owned valid MP4 + actual HTTP public-retired generation reopen"]
async fn actual_http_public_retired_reopen_preserves_younger_body() {
    run_case(false, true).await;
}
async fn run_case(extra_entry: bool, retired_reopen: bool) {
    assert_eq!(
        std::env::var("REDIS_RESTART_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let media =
        std::fs::read(std::env::var("VOD_RECOVERY_MEDIA").expect("owned valid MP4 fixture"))
            .unwrap();
    assert!(media.len() > 1_000_000 && media.len() <= 5 * 1024 * 1024);
    assert_eq!(&media[4..8], b"ftyp");
    let media = Arc::new(media);
    let requests = Arc::new(AtomicUsize::new(0));
    let origin_data = media.clone();
    let origin_requests = requests.clone();
    let (origin, origin_task) = serve(Router::new().route(
        "/fixture.mp4",
        get(move || {
            let data = origin_data.clone();
            let count = origin_requests.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                let length = data.len();
                let body = stream::unfold((data, 0usize), |(data, position)| async move {
                    if position == data.len() {
                        return None;
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    let end = (position + 8192).min(data.len());
                    Some((
                        Ok::<_, std::io::Error>(data[position..end].to_vec()),
                        (data, end),
                    ))
                });
                axum::http::Response::builder()
                    .status(200)
                    .header("content-type", "video/mp4")
                    .header("content-length", length)
                    .body(Body::from_stream(body))
                    .unwrap()
            }
        }),
    ))
    .await;
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let namespace = format!("http-recovery:{}", uuid::Uuid::new_v4());
    let coordinator = Coordinator::connect(
        &url,
        LeaseConfig {
            namespace: namespace.clone(),
            heartbeat_interval: Some(Duration::from_secs(1)),
            ttl: Duration::from_secs(10),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let profile = (uuid::Uuid::new_v4().as_u128() % 1_000_000_000) as i64 + 1000;
    let content = uuid::Uuid::new_v4().to_string();
    let id = uuid::Uuid::new_v4().to_string();
    let resolver = Arc::new(Resolver(VodSource {
        kind: ContentKind::Movie,
        content_uuid: content.clone(),
        name: "Owned recovery movie".into(),
        user: None,
        redirect: false,
        limit_policy: Default::default(),
        candidates: vec![VodCandidate {
            relation_id: 1,
            account_id: 1,
            profile_id: profile,
            max_streams: 1,
            url: format!("{origin}/fixture.mp4"),
            user_agent: "owned-recovery".into(),
        }],
    }));
    let engine = VodEngine::with_resolver(
        resolver,
        coordinator,
        &url,
        VodConfig {
            max_sessions: 1,
            max_requests: 3,
            frame_bytes: 8192,
            idle_grace: Duration::from_secs(1),
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .with_redis_recovery(&namespace)
    .unwrap();
    let (base, app_task) = serve(engine.clone().router()).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(90))
        .build()
        .unwrap();
    let response = client
        .get(format!("{base}/proxy/vod/movie/{content}/{id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut old = Reader {
        response,
        pending: bytes::Bytes::new(),
    };
    let mut old_bytes = old.take(8192).await;
    assert_eq!(&old_bytes, &media[..8192]);
    let mut redis = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let owner = format!("{namespace}:vod:session:{id}:owner");
    let metadata = format!("vod_persistent_connection:{id}");
    let counter = format!("profile_connections:{profile}");
    let token: String = redis.get(&owner).await.unwrap();
    let ledger = format!("{namespace}:lease:{token}");
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    let _: usize = redis
        .del(&[owner.clone(), metadata.clone(), ledger, counter.clone()])
        .await
        .unwrap();
    for _ in 1..34 {
        old_bytes.extend(old.take(8192).await);
    }
    assert_eq!(old_bytes, &media[..278528]);
    assert!(!redis.exists::<_, bool>(&metadata).await.unwrap());
    let response = client
        .get(format!("{base}/proxy/vod/movie/{content}/{id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut young = Reader {
        response,
        pending: bytes::Bytes::new(),
    };
    let mut young_bytes = young.take(8192).await;
    assert_eq!(young_bytes, &media[..8192]);
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    if extra_entry {
        let response = client
            .get(format!("{base}/proxy/vod/movie/{content}/{id}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let mut third = Reader {
            response,
            pending: bytes::Bytes::new(),
        };
        assert_eq!(third.take(8192).await, &media[..8192]);
        assert_eq!(
            redis
                .hget::<_, _, i64>(&metadata, "active_streams")
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            redis
                .hget::<_, _, i64>(&metadata, "request_count")
                .await
                .unwrap(),
            2
        );
        assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 1);
        assert_eq!(requests.load(Ordering::SeqCst), 3);
        drop(third);
        wait(async || {
            redis
                .hget::<_, _, Option<i64>>(&metadata, "active_streams")
                .await
                .unwrap()
                == Some(1)
        })
        .await;
        assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 1);
    }
    assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 1);
    assert_eq!(
        redis
            .hget::<_, _, i64>(&metadata, "active_streams")
            .await
            .unwrap(),
        1
    );
    for _ in 34..101 {
        old_bytes.extend(old.take(8192).await);
    }
    assert_eq!(old_bytes, &media[..827392]);
    wait(async || {
        redis
            .hget::<_, _, Option<u64>>(&metadata, "bytes_sent")
            .await
            .unwrap()
            == Some(819200)
    })
    .await;
    assert_eq!(
        redis
            .hget::<_, _, i64>(&metadata, "active_streams")
            .await
            .unwrap(),
        1
    );
    drop(old);
    wait(async || !redis.exists::<_, bool>(&metadata).await.unwrap()).await;
    assert_eq!(
        redis
            .get::<_, Option<i64>>(&counter)
            .await
            .unwrap()
            .unwrap_or(0),
        0
    );
    for _ in 1..103 {
        young_bytes.extend(young.take(8192).await);
    }
    assert_eq!(young_bytes, &media[..843776]);
    assert_eq!(
        requests.load(Ordering::SeqCst),
        if extra_entry { 3 } else { 2 }
    );
    if retired_reopen {
        let response = client
            .get(format!("{base}/proxy/vod/movie/{content}/{id}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let mut replacement = Reader {
            response,
            pending: bytes::Bytes::new(),
        };
        assert_eq!(replacement.take(8192).await, &media[..8192]);
        assert_eq!(
            redis
                .hget::<_, _, i64>(&metadata, "active_streams")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            redis
                .hget::<_, _, i64>(&metadata, "request_count")
                .await
                .unwrap(),
            1
        );
        assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 1);
        assert_eq!(requests.load(Ordering::SeqCst), 3);
        let next = young.take(8192).await;
        assert_eq!(next, &media[843776..851968]);
        young_bytes.extend(next);
        drop(replacement);
        wait(async || !redis.exists::<_, bool>(&metadata).await.unwrap()).await;
    }
    // Retain bounded valid MP4 prefixes for independent strict first-second decode.
    std::fs::write("/evidence/old-prefix.mp4", old_bytes).unwrap();
    std::fs::write("/evidence/young-prefix.mp4", young_bytes).unwrap();
    drop(young);
    assert!(
        engine.shutdown().await,
        "all body/control/provider work must physically drain"
    );
    assert!(!redis.exists::<_, bool>(&owner).await.unwrap());
    assert!(!redis.exists::<_, bool>(&metadata).await.unwrap());
    assert_eq!(
        redis
            .get::<_, Option<i64>>(&counter)
            .await
            .unwrap()
            .unwrap_or(0),
        0
    );
    app_task.abort();
    origin_task.abort();
}
