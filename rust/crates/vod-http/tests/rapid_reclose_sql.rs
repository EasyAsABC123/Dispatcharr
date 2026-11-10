//! Original VOD independent-timer event multiplicity and fenced cleanup.
//! Full original application credit remains with Dispatcharr contributors.
use axum::body::Body;
use dispatcharr_contracts::{BoxFuture, PlaybackRequest, UpdateEventError, UpdateEventSink};
use dispatcharr_coordination::{Coordinator, LeaseConfig};
use dispatcharr_store::{AuthConfig, AuthService, NotificationCache, Store, StoreError, User};
use dispatcharr_vod_http::{
    SqlVodResolver, VodConfig, VodEngine, VodRequest, VodResolver, VodSource,
};
use http_body_util::BodyExt;
use redis::AsyncCommands;
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Sink(Mutex<Vec<Value>>);
impl UpdateEventSink for Sink {
    fn send<'a>(
        &'a self,
        group: &'a str,
        value: Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            assert_eq!(group, "updates");
            self.0.lock().unwrap().push(value);
            Ok(())
        })
    }
}
struct Resolver {
    sql: SqlVodResolver,
    origin: String,
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
        request: &'a VodRequest,
        user: Option<User>,
    ) -> BoxFuture<'a, Result<VodSource, StoreError>> {
        Box::pin(async move {
            let mut source = self.sql.resolve(request, user).await?;
            for row in &mut source.candidates {
                row.url = format!("{}/vod.mp4", self.origin);
            }
            Ok(source)
        })
    }
    fn default_redirect(&self) -> BoxFuture<'_, Result<bool, StoreError>> {
        Box::pin(async { Ok(false) })
    }
}
fn request(id: &str) -> VodRequest {
    let uuid = "a4d78cd1-bf9f-4a63-a746-f40566825cf9";
    VodRequest {
        resolved_client_ip: None,
        kind: "movie".into(),
        content_id: uuid.into(),
        session_id: Some(id.into()),
        profile_id: None,
        playback: PlaybackRequest {
            channel_id: uuid.into(),
            query: vec![],
            headers: vec![("user-agent".into(), "fixture-rapid-reclose".into())],
            peer_ip: "192.0.2.25".parse().unwrap(),
        },
    }
}
async fn first(body: &mut Body) {
    let frame = tokio::time::timeout(Duration::from_secs(3), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!frame.data_ref().unwrap().is_empty());
}
async fn wait(predicate: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(4), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
struct Fixture {
    store: Store,
    engine: VodEngine,
    sink: Arc<Sink>,
    redis: redis::aio::MultiplexedConnection,
    profile: i64,
}
impl Fixture {
    async fn new(config: VodConfig) -> Self {
        assert_eq!(
            std::env::var("VOD_DISPOSABLE_FIXTURE").as_deref(),
            Ok("playback-fixture-only")
        );
        let database = std::env::var("DATABASE_TEST_URL").unwrap();
        let url = std::env::var("REDIS_TEST_URL").unwrap();
        assert!(
            database.contains("playback-fixture-only@") && url.contains("playback-fixture-only@")
        );
        let store = Store::connect(&database, 8).await.unwrap();
        let auth = AuthService::new(
            store.clone(),
            AuthConfig::new("playback-fixture-only".into()).unwrap(),
        )
        .with_notification_cache(NotificationCache::connect(&url).await.unwrap());
        let resolver = Arc::new(Resolver {
            sql: SqlVodResolver::new(store.clone(), auth.clone()),
            origin: std::env::var("VOD_ORIGIN_TEST_URL").unwrap(),
        });
        let profile:i64=sqlx::query_scalar("SELECT p.id::bigint FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id WHERE a.name='Rust VOD fixture XC' AND p.is_default").fetch_one(store.pool()).await.unwrap();
        let coordinator = Coordinator::connect(
            &url,
            LeaseConfig {
                namespace: format!("rust:vod-rapid:{}", uuid::Uuid::new_v4()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let sink = Arc::new(Sink::default());
        let engine = VodEngine::with_resolver(resolver, coordinator, &url, config)
            .await
            .unwrap()
            .with_events(store.clone(), auth, &url, sink.clone())
            .await
            .unwrap();
        let redis = redis::Client::open(url)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        Self {
            store,
            engine,
            sink,
            redis,
            profile,
        }
    }
    async fn inactive(&mut self, id: &str) {
        tokio::time::timeout(Duration::from_millis(600), async {
            loop {
                let active: Option<i64> = self
                    .redis
                    .hget(format!("vod_persistent_connection:{id}"), "active_streams")
                    .await
                    .unwrap();
                if active == Some(0) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn counter(&mut self) -> i64 {
        self.redis
            .get::<_, Option<i64>>(format!("profile_connections:{}", self.profile))
            .await
            .unwrap()
            .unwrap_or(0)
    }
}

#[tokio::test]
#[ignore = "requires disposable original rapid-reclose oracle and actual PG/Redis/HTTP"]
async fn original_rapid_reclose_keeps_both_stops_and_audits() {
    let oracle: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_RAPID_RECLOSE_ORACLE").unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(oracle["within_original_grace"], true);
    let mut fixture = Fixture::new(VodConfig::default()).await;
    let before: i64 =
        sqlx::query_scalar("SELECT COALESCE(max(id),0)::bigint FROM core_systemevent")
            .fetch_one(fixture.store.pool())
            .await
            .unwrap();
    let id = "fixture-vod-rapid-reclose";
    let response = fixture.engine.respond(request(id)).await;
    assert_eq!(response.status(), 200);
    let mut body = response.into_body();
    first(&mut body).await;
    wait(|| fixture.sink.0.lock().unwrap().len() >= 2).await;
    let began = tokio::time::Instant::now();
    drop(body);
    fixture.inactive(id).await;
    let response = fixture.engine.respond(request(id)).await;
    assert_eq!(response.status(), 200);
    let mut body = response.into_body();
    first(&mut body).await;
    drop(body);
    fixture.inactive(id).await;
    assert!(began.elapsed() < Duration::from_secs(1));
    wait(|| fixture.sink.0.lock().unwrap().len() >= 6).await;
    assert!(fixture.engine.shutdown().await);
    let events = fixture.sink.0.lock().unwrap().clone();
    let kinds = events
        .iter()
        .map(|event| event["data"]["type"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(kinds.len(), 6);
    assert_eq!(kinds[0], "vod_started");
    assert_eq!(kinds[1], "vod_stats");
    // Original independent timer threads permit different interleavings of the
    // two stop/stats pairs. Compare multiplicity and transition payloads, not a
    // single scheduler-dependent order between the independent timers.
    for kind in ["vod_started", "vod_stopped", "vod_stats"] {
        assert_eq!(
            kinds.iter().filter(|value| **value == kind).count(),
            oracle["event_types"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|value| value.as_str() == Some(kind))
                .count()
        );
    }
    for event in &events {
        let kind = event["data"]["type"].as_str().unwrap();
        if kind != "vod_stats" {
            let expected = oracle["events"]
                .as_array()
                .unwrap()
                .iter()
                .find(|value| value["data"]["type"] == kind)
                .unwrap();
            assert_eq!(event, expected);
        } else {
            let stats: Value =
                serde_json::from_str(event["data"]["stats"].as_str().unwrap()).unwrap();
            assert!(stats["timestamp"].as_f64().unwrap().is_finite());
            assert!(stats["total_connections"].as_u64().unwrap() <= 1);
        }
    }
    let audit=sqlx::query_as::<_,(String,Value)>("SELECT event_type,details FROM core_systemevent WHERE id>$1 AND event_type IN('vod_start','vod_stop') ORDER BY id").bind(before).fetch_all(fixture.store.pool()).await.unwrap();
    assert_eq!(audit.len(), oracle["audit"].as_array().unwrap().len());
    for ((kind, details), expected) in audit.iter().zip(oracle["audit"].as_array().unwrap()) {
        assert_eq!(kind, expected["event_type"].as_str().unwrap());
        assert_eq!(details, &expected["details"]);
    }
    assert_eq!(fixture.counter().await, 0);
    assert!(!fixture
        .redis
        .exists::<_, bool>(format!("vod_persistent_connection:{id}"))
        .await
        .unwrap());
    assert_eq!(
        fixture.engine.event_diagnostics(),
        json!({"pending":0,"failed":0})
    );
    println!(
        "{}",
        json!({"rapid_reclose_native_event_types":kinds,"audit_rows":audit.len(),"physical_cleanup":true,"independent_timer_interleaving_normalized":true})
    );
}

#[tokio::test]
#[ignore = "requires disposable actual PG/Redis/HTTP"]
async fn closing_timers_retain_request_admission_until_physical_drain() {
    let mut fixture = Fixture::new(VodConfig {
        max_requests: 2,
        ..Default::default()
    })
    .await;
    // Same-content/IP/agent reconnects deliberately reuse the idle session,
    // including when another proposed ID is supplied. Exercise two closing
    // requests on that actual shared session rather than polling a nonexistent
    // second metadata key.
    for _ in 0..2 {
        let id = "fixture-rapid-admission";
        let response = fixture.engine.respond(request(id)).await;
        assert_eq!(response.status(), 200);
        let mut body = response.into_body();
        first(&mut body).await;
        drop(body);
        fixture.inactive(id).await;
    }
    let response = fixture
        .engine
        .respond(request("fixture-rapid-admission-overflow"))
        .await;
    assert_eq!(response.status(), 503);
    drop(response);
    assert!(fixture.engine.shutdown().await);
    assert_eq!(fixture.counter().await, 0);
    assert_eq!(
        fixture.engine.event_diagnostics(),
        json!({"pending":0,"failed":0})
    );
}

#[tokio::test]
#[ignore = "requires disposable actual PG/Redis/HTTP"]
async fn duplicate_stop_timers_preserve_another_active_provider_allocation() {
    let mut fixture = Fixture::new(VodConfig::default()).await;
    let sibling_id = "fixture-rapid-active-sibling";
    let response = fixture.engine.respond(request(sibling_id)).await;
    assert_eq!(response.status(), 200);
    let mut sibling = response.into_body();
    first(&mut sibling).await;
    let id = "fixture-rapid-shared-provider";
    let response = fixture.engine.respond(request(id)).await;
    assert_eq!(response.status(), 200);
    let mut body = response.into_body();
    first(&mut body).await;
    assert_eq!(fixture.counter().await, 2);
    drop(body);
    fixture.inactive(id).await;
    let response = fixture.engine.respond(request(id)).await;
    assert_eq!(response.status(), 200);
    let mut body = response.into_body();
    first(&mut body).await;
    drop(body);
    fixture.inactive(id).await;
    wait(|| {
        fixture
            .sink
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event["data"]["type"] == "vod_stopped")
            .count()
            == 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        fixture.counter().await,
        1,
        "old timers released the active sibling's provider"
    );
    assert_eq!(
        fixture
            .redis
            .hget::<_, _, Option<i64>>(
                format!("vod_persistent_connection:{sibling_id}"),
                "active_streams"
            )
            .await
            .unwrap(),
        Some(1)
    );
    drop(sibling);
    fixture.inactive(sibling_id).await;
    assert!(fixture.engine.shutdown().await);
    assert_eq!(fixture.counter().await, 0);
}

#[tokio::test]
#[ignore = "requires disposable actual PG/Redis/HTTP"]
async fn old_timer_checks_current_owner_and_preserves_replacement_metadata() {
    let mut fixture = Fixture::new(VodConfig::default()).await;
    let sibling_id = "fixture-rapid-replacement-sibling";
    let response = fixture.engine.respond(request(sibling_id)).await;
    assert_eq!(response.status(), 200);
    let mut sibling = response.into_body();
    first(&mut sibling).await;
    let id = "fixture-rapid-replaced-owner";
    let response = fixture.engine.respond(request(id)).await;
    assert_eq!(response.status(), 200);
    let mut body = response.into_body();
    first(&mut body).await;
    drop(body);
    fixture.inactive(id).await;
    let key = format!("vod_persistent_connection:{id}");
    // Inject a new owner only in the explicitly disposable fixture. The active
    // sibling provides a real independent provider allocation to protect.
    let _: () = fixture
        .redis
        .hset_multiple(
            &key,
            &[
                ("rust_owner_token", "fixture-replacement-owner"),
                ("active_streams", "1"),
            ],
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1250)).await;
    assert_eq!(
        fixture
            .redis
            .hget::<_, _, String>(&key, "rust_owner_token")
            .await
            .unwrap(),
        "fixture-replacement-owner"
    );
    assert_eq!(
        fixture
            .redis
            .hget::<_, _, i64>(&key, "active_streams")
            .await
            .unwrap(),
        1
    );
    assert_eq!(fixture.counter().await, 1);
    assert!(!fixture
        .sink
        .0
        .lock()
        .unwrap()
        .iter()
        .any(|event| event["data"]["type"] == "vod_stopped"));
    // Test-owned fault metadata is removed by the fixture, never by old runtime
    // cleanup. Ordinary media teardown still owns the sibling's real lease.
    let _: usize = fixture.redis.del(&key).await.unwrap();
    drop(sibling);
    fixture.inactive(sibling_id).await;
    assert!(fixture.engine.shutdown().await);
    assert_eq!(fixture.counter().await, 0);
}
