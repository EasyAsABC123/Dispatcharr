//! Actual original-schema/provider/body timing and native VOD producer comparison.
//! All original Dispatcharr authors/contributors retain full application credit.
//! Entry authentication is intentionally outside this manager-body component scope.
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
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
#[derive(Default)]
struct Sink {
    messages: Mutex<Vec<Value>>,
}
impl UpdateEventSink for Sink {
    fn send<'a>(
        &'a self,
        group: &'a str,
        value: Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            assert_eq!(group, "updates");
            self.messages.lock().unwrap().push(value);
            Ok(())
        })
    }
}
struct Resolver {
    sql: SqlVodResolver,
    origin: String,
    fail: AtomicBool,
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
            for candidate in &mut source.candidates {
                candidate.url = format!(
                    "{}{}",
                    self.origin,
                    if self.fail.load(Ordering::Acquire) {
                        "/not-a-vod-fixture.mp4"
                    } else {
                        "/vod.mp4"
                    }
                );
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
            headers: vec![("user-agent".into(), format!("fixture-body-{id}"))],
            peer_ip: "192.0.2.23".parse().unwrap(),
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
fn types(messages: &[Value]) -> Vec<&str> {
    messages
        .iter()
        .map(|value| value["data"]["type"].as_str().unwrap())
        .collect()
}
async fn wait(predicate: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
async fn quiet(sink: &Sink, expected: usize, duration: Duration) {
    tokio::time::sleep(duration).await;
    assert_eq!(sink.messages.lock().unwrap().len(), expected);
}
async fn cleaned(redis: &mut redis::aio::MultiplexedConnection, id: &str, profile: i64) {
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let exists: bool = redis
                .exists(format!("vod_persistent_connection:{id}"))
                .await
                .unwrap();
            let counter: Option<i64> = redis
                .get(format!("profile_connections:{profile}"))
                .await
                .unwrap();
            if !exists && counter.unwrap_or(0) == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
fn normalize_event(value: &Value) -> Value {
    let mut value = value.clone();
    if value["data"]["type"] == "vod_stats" {
        let mut stats: Value =
            serde_json::from_str(value["data"]["stats"].as_str().unwrap()).unwrap();
        assert!(stats["timestamp"].as_f64().unwrap().is_finite());
        stats.as_object_mut().unwrap().remove("timestamp");
        // Stop snapshot races final idle metadata removal in the original.
        // Stable full fields of any remaining connection are checked separately.
        for group in stats["vod_connections"].as_array_mut().unwrap() {
            for connection in group["connections"].as_array_mut().unwrap() {
                for field in ["connected_at", "last_activity"] {
                    assert!(connection[field]
                        .as_str()
                        .unwrap()
                        .parse::<f64>()
                        .unwrap()
                        .is_finite());
                    connection.as_object_mut().unwrap().remove(field);
                }
                assert!(connection["duration"].as_i64().is_some());
                connection.as_object_mut().unwrap().remove("duration");
            }
        }
        value["data"]["stats"] = stats;
    }
    value
}
#[tokio::test]
#[ignore = "requires disposable actual Django VOD lifecycle fixture and HTTP origin"]
async fn original_new_unpolled_reused_reconnect_complete_and_failure_events_match() {
    assert_eq!(
        std::env::var("VOD_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let database = std::env::var("DATABASE_TEST_URL").unwrap();
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    assert!(
        database.contains("playback-fixture-only@") && redis_url.contains("playback-fixture-only@")
    );
    let source: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_LIFECYCLE_ORACLE").unwrap()).unwrap(),
    )
    .unwrap();
    let store = Store::connect(&database, 8).await.unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(NotificationCache::connect(&redis_url).await.unwrap());
    let resolver = Arc::new(Resolver {
        sql: SqlVodResolver::new(store.clone(), auth.clone()),
        origin: std::env::var("VOD_ORIGIN_TEST_URL").unwrap(),
        fail: AtomicBool::new(false),
    });
    let profile:i64=sqlx::query_scalar("SELECT p.id::bigint FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id WHERE a.name='Rust VOD fixture XC' AND p.is_default").fetch_one(store.pool()).await.unwrap();
    let coordinator = Coordinator::connect(
        &redis_url,
        LeaseConfig {
            namespace: format!("rust:vod-events:{}", uuid::Uuid::new_v4()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let sink = Arc::new(Sink::default());
    let engine = VodEngine::with_resolver(
        resolver.clone(),
        coordinator,
        &redis_url,
        VodConfig::default(),
    )
    .await
    .unwrap()
    .with_events(store.clone(), auth, &redis_url, sink.clone())
    .await
    .unwrap();
    let mut redis = redis::Client::open(redis_url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let before: i64 =
        sqlx::query_scalar("SELECT COALESCE(max(id),0)::bigint FROM core_systemevent")
            .fetch_one(store.pool())
            .await
            .unwrap();
    let mut captured = Vec::new();
    for (index, id) in [
        "fixture-vod-unpolled",
        "fixture-vod-disconnect",
        "fixture-vod-reused-unpolled",
        "fixture-vod-reconnect",
        "fixture-vod-completed",
        "fixture-vod-provider-failed",
    ]
    .iter()
    .enumerate()
    {
        let start = sink.messages.lock().unwrap().len();
        if index == 5 {
            resolver.fail.store(true, Ordering::Release);
        }
        let response = engine.respond(request(id)).await;
        if index == 5 {
            assert_eq!(
                response.status().as_u16(),
                source["cases"][index]["status"].as_u64().unwrap() as u16
            );
            drop(response);
            cleaned(&mut redis, id, profile).await;
            quiet(&sink, start, Duration::from_millis(100)).await;
        } else {
            assert_eq!(response.status(), 200);
            let mut body = response.into_body();
            if index == 0 {
                quiet(&sink, start, Duration::from_millis(200)).await;
                drop(body);
            } else {
                first(&mut body).await;
                wait(|| sink.messages.lock().unwrap().len() >= start + 2).await;
                match index {
                    1 => drop(body),
                    2 => {
                        let second = engine.respond(request(id)).await;
                        assert_eq!(second.status(), 200);
                        drop(body);
                        quiet(&sink, start + 2, Duration::from_millis(1200)).await;
                        drop(second);
                    }
                    3 => {
                        drop(body);
                        // Let the first response release provider demand before reentry.
                        tokio::time::sleep(Duration::from_millis(25)).await;
                        let second = engine.respond(request(id)).await;
                        assert_eq!(second.status(), 200);
                        let mut second = second.into_body();
                        first(&mut second).await;
                        quiet(&sink, start + 2, Duration::from_millis(1200)).await;
                        drop(second);
                    }
                    4 => {
                        let remainder = body.collect().await.unwrap().to_bytes();
                        assert!(remainder.len() > 4_000_000);
                        let reported: Option<String> = redis
                            .hget(format!("vod_persistent_connection:{id}"), "bytes_sent")
                            .await
                            .unwrap();
                        assert_eq!(
                            reported.unwrap().parse::<u64>().unwrap(),
                            source["cases"][index]["sample_after_eof"]["bytes_sent"]
                                .as_u64()
                                .unwrap(),
                            "Source consumed-byte milestone metadata"
                        );
                    }
                    _ => unreachable!(),
                }
            }
            cleaned(&mut redis, id, profile).await;
            let expected = start + if index == 0 { 0 } else { 4 };
            wait(|| sink.messages.lock().unwrap().len() >= expected).await;
        }
        let events = sink.messages.lock().unwrap()[start..].to_vec();
        assert_eq!(
            types(&events),
            source["cases"][index]["event_types"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect::<Vec<_>>()
        );
        for (position, (actual, expected)) in events
            .iter()
            .zip(source["cases"][index]["events"].as_array().unwrap())
            .enumerate()
        {
            assert!(actual["data"].get("success").is_none());
            if position == 3 {
                let stats: Value =
                    serde_json::from_str(actual["data"]["stats"].as_str().unwrap()).unwrap();
                assert_eq!(stats["total_connections"], 0);
            } else {
                assert_eq!(
                    normalize_event(actual),
                    normalize_event(expected),
                    "Source event case{index} position{position}"
                );
            }
        }
        captured.push(json!({"name":source["cases"][index]["name"],"events":events.len(),"event_types":types(&events)}));
    }
    assert!(engine.shutdown().await);
    assert_eq!(engine.event_diagnostics(), json!({"pending":0,"failed":0}));
    let rows=sqlx::query_as::<_,(String,Value)>("SELECT event_type,details FROM core_systemevent WHERE id>$1 AND event_type IN('vod_start','vod_stop') ORDER BY id").bind(before).fetch_all(store.pool()).await.unwrap();
    let expected = source["audit"].as_array().unwrap();
    assert_eq!(rows.len(), expected.len());
    for ((kind, details), expected) in rows.iter().zip(expected) {
        assert_eq!(kind, expected["event_type"].as_str().unwrap());
        assert_eq!(details, &expected["details"]);
    }
    println!(
        "{}",
        json!({"native_body_lifecycle_cases":captured,"audit_rows":rows.len(),"actual_native_transport_verified":false,"application_parity_verified":false})
    );
}

#[derive(Default)]
struct BlockedSink {
    entered: AtomicBool,
    active: Arc<AtomicUsize>,
}
struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
impl UpdateEventSink for BlockedSink {
    fn send<'a>(&'a self, _: &'a str, _: Value) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            self.entered.store(true, Ordering::Release);
            self.active.fetch_add(1, Ordering::AcqRel);
            let _active = Active(self.active.clone());
            futures_util::future::pending().await
        })
    }
}
#[tokio::test]
#[ignore = "requires disposable actual Django VOD lifecycle fixture and HTTP origin"]
async fn stalled_event_sink_shutdown_is_bounded_and_releases_provider_and_all_actor_tasks() {
    assert_eq!(
        std::env::var("VOD_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let database = std::env::var("DATABASE_TEST_URL").unwrap();
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    assert!(
        database.contains("playback-fixture-only@") && redis_url.contains("playback-fixture-only@")
    );
    let store = Store::connect(&database, 8).await.unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    );
    let resolver = Arc::new(Resolver {
        sql: SqlVodResolver::new(store.clone(), auth.clone()),
        origin: std::env::var("VOD_ORIGIN_TEST_URL").unwrap(),
        fail: AtomicBool::new(false),
    });
    let coordinator = Coordinator::connect(
        &redis_url,
        LeaseConfig {
            namespace: format!("rust:vod-events-stalled:{}", uuid::Uuid::new_v4()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let sink = Arc::new(BlockedSink::default());
    let engine = VodEngine::with_resolver(
        resolver,
        coordinator,
        &redis_url,
        VodConfig {
            shutdown_timeout: Duration::from_millis(100),
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .with_events(store.clone(), auth, &redis_url, sink.clone())
    .await
    .unwrap();
    let response = engine.respond(request("fixture-vod-stalled-sink")).await;
    assert_eq!(response.status(), 200);
    let mut body = response.into_body();
    first(&mut body).await;
    wait(|| sink.entered.load(Ordering::Acquire)).await;
    assert!(
        !tokio::time::timeout(Duration::from_millis(300), engine.shutdown())
            .await
            .unwrap(),
        "stalled publication must not falsely report drained shutdown"
    );
    wait(|| sink.active.load(Ordering::Acquire) == 0).await;
    wait(|| engine.event_diagnostics()["pending"] == 0).await;
    assert!(engine.event_diagnostics()["failed"].as_u64().unwrap() > 0);
    let exists: bool = redis::AsyncCommands::exists(
        &mut redis::Client::open(redis_url)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap(),
        "vod_persistent_connection:fixture-vod-stalled-sink",
    )
    .await
    .unwrap();
    assert!(!exists);
    drop(body);
    assert!(engine.shutdown().await);
}

#[tokio::test]
#[ignore = "requires disposable original VOD progress-boundary fixture"]
async fn final_partial_hundredth_source_chunk_records_actual_consumed_bytes() {
    assert_eq!(
        std::env::var("VOD_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let database = std::env::var("DATABASE_TEST_URL").unwrap();
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    assert!(
        database.contains("playback-fixture-only@") && redis_url.contains("playback-fixture-only@")
    );
    let source: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_PROGRESS_ORACLE").unwrap()).unwrap(),
    )
    .unwrap();
    let store = Store::connect(&database, 8).await.unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    );
    let origin = std::env::var("VOD_ORIGIN_TEST_URL").unwrap();
    let resolver = Arc::new(Resolver {
        sql: SqlVodResolver::new(store, auth),
        origin: origin.clone(),
        fail: AtomicBool::new(false),
    });
    let coordinator = Coordinator::connect(
        &redis_url,
        LeaseConfig {
            namespace: format!("rust:vod-progress:{}", uuid::Uuid::new_v4()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let engine = VodEngine::with_resolver(resolver, coordinator, &redis_url, VodConfig::default())
        .await
        .unwrap();
    let mut redis = redis::Client::open(redis_url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let client = reqwest::Client::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = engine.clone().router();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    for case in source["cases"].as_array().unwrap() {
        let sid = case["session_id"].as_str().unwrap();
        let response = client
            .get(format!(
                "{base}/proxy/vod/movie/a4d78cd1-bf9f-4a63-a746-f40566825cf9/{sid}"
            ))
            .header("range", case["range"].as_str().unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status().as_u16(),
            case["status"].as_u64().unwrap() as u16
        );
        let body = response.bytes().await.unwrap();
        assert_eq!(body.len(), case["bytes"].as_u64().unwrap() as usize);
        let expected = client
            .get(format!("{origin}/vod.mp4"))
            .header("range", case["range"].as_str().unwrap())
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(body, expected);
        let recorded: Option<String> = redis
            .hget(format!("vod_persistent_connection:{sid}"), "bytes_sent")
            .await
            .unwrap();
        assert_eq!(
            recorded.unwrap().parse::<u64>().unwrap(),
            case["bytes_sent"].as_u64().unwrap(),
            "Source byteprogress {}",
            case["case"]
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            while redis
                .exists::<_, bool>(format!("vod_persistent_connection:{sid}"))
                .await
                .unwrap()
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    assert!(engine.shutdown().await);
    server.abort();
    println!(
        "{}",
        json!({"actual_source_final_partial_progress_cases":3,"body_bytes_match_origin":true})
    );
}
