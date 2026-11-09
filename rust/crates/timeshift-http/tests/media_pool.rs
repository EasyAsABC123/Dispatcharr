//! Queued quiet-window pool checks. These require real disposable Redis.
//! Original application credit belongs to upstream Dispatcharr contributors.
use dispatcharr_coordination::{Coordinator, LeaseConfig};
use dispatcharr_timeshift_http::{ArchivePool, ArchivePoolConfig, ArchivePoolRequest};
use redis::AsyncCommands;
use std::{collections::BTreeMap, time::Duration};
async fn setup() -> (ArchivePool, redis::aio::MultiplexedConnection, i64) {
    let url = std::env::var("REDIS_TEST_URL").expect("explicit disposable Redis required");
    let connection = redis::Client::open(url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let coordinator = Coordinator::connect(
        &url,
        LeaseConfig {
            namespace: format!("test:archive-pool:{}", uuid::Uuid::new_v4()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let config = ArchivePoolConfig {
        max_sessions: 4,
        disconnect_grace: Duration::from_millis(200),
        startup_idle: Duration::from_millis(300),
        shutdown_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let pool = ArchivePool::connect(&url, coordinator, config)
        .await
        .unwrap();
    let profile = (uuid::Uuid::new_v4().as_u128() % 1_000_000_000_000) as i64 + 1000;
    (pool, connection, profile)
}
fn fixture_user(sid: &str) -> i64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sid.hash(&mut hasher);
    (hasher.finish() % 1_000_000_000_000) as i64 + 1000
}
fn request(sid: &str, profile: i64) -> ArchivePoolRequest {
    ArchivePoolRequest {
        session_id: sid.into(),
        channel_id: 123,
        virtual_channel_id: "123_2026-11-09T11-00-00Z_fixture".into(),
        provider_profile_id: profile,
        provider_max_streams: 1,
        stream_limit: 0,
        limit_policy: Default::default(),
        pool_fields: BTreeMap::from([
            ("user_id".into(), fixture_user(sid).to_string()),
            ("profile_id".into(), profile.to_string()),
            ("media_id".into(), "123_2026-11-09T11-00-00Z".into()),
        ]),
        client_fields: BTreeMap::from([
            ("user_id".into(), fixture_user(sid).to_string()),
            ("username".into(), "fixture-viewer".into()),
            ("ip_address".into(), "127.0.0.1".into()),
        ]),
        channel_fields: BTreeMap::from([
            ("channel_id".into(), "123".into()),
            ("channel_name".into(), "fixture-archive".into()),
        ]),
    }
}
async fn wait(mut predicate: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !predicate().await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("archive lifecycle timeout");
}
fn profile_key(profile: i64) -> String {
    format!("profile_connections:{profile}")
}
fn pool_key(sid: &str) -> String {
    format!("timeshift:pool:{sid}")
}
fn api_key(sid: &str) -> String {
    format!("timeshift:session:{sid}")
}
fn client_key(sid: &str) -> String {
    format!("timeshift:channel:123_{sid}:clients:{sid}")
}
fn metadata_key(sid: &str) -> String {
    format!("timeshift:channel:123_{sid}:metadata")
}
#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn startup_release_has_no_live_phantom_preserves_api_and_busy_reentry_does_not_reserve() {
    let (pool, mut redis, profile) = setup().await;
    let sid = uuid::Uuid::new_v4().to_string();
    let _: () = redis
        .hset(api_key(&sid), "user_id", fixture_user(&sid))
        .await
        .unwrap();
    let ticket = pool.open(request(&sid, profile)).await.unwrap();
    assert_eq!(redis.get::<_, i64>(profile_key(profile)).await.unwrap(), 1);
    assert!(!redis
        .exists::<_, bool>(format!("live:channel:{sid}:owner"))
        .await
        .unwrap());
    assert!(pool.open(request(&sid, profile)).await.is_err());
    assert!(pool
        .open(request(&uuid::Uuid::new_v4().to_string(), profile))
        .await
        .is_err());
    assert_eq!(redis.get::<_, i64>(profile_key(profile)).await.unwrap(), 1);
    ticket
        .start(
            "http://fixture.invalid/archive.ts",
            Some(15_001_836),
            Some("bytes=0-255"),
        )
        .await
        .unwrap();
    assert_eq!(
        redis
            .hget::<_, _, String>(pool_key(&sid), "serving_range")
            .await
            .unwrap(),
        "start"
    );
    ticket.forwarded(256);
    drop(ticket);
    wait(async || {
        redis
            .get::<_, i64>(profile_key(profile))
            .await
            .unwrap_or(-1)
            == 0
    })
    .await;
    wait(async || !redis.exists::<_, bool>(pool_key(&sid)).await.unwrap()).await;
    assert!(redis.exists::<_, bool>(api_key(&sid)).await.unwrap());
    assert!(!redis.exists::<_, bool>(client_key(&sid)).await.unwrap());
    assert!(pool.shutdown().await);
    let _: () = redis
        .del(&[api_key(&sid), profile_key(profile)])
        .await
        .unwrap();
}
#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn established_playback_finishes_byte_telemetry_then_removes_native_session_after_grace() {
    let (pool, mut redis, profile) = setup().await;
    let sid = uuid::Uuid::new_v4().to_string();
    let _: () = redis
        .hset(api_key(&sid), "user_id", fixture_user(&sid))
        .await
        .unwrap();
    let ticket = pool.open(request(&sid, profile)).await.unwrap();
    ticket
        .start(
            "http://fixture.invalid/archive.ts",
            Some(15_001_836),
            Some("bytes=0-1879999"),
        )
        .await
        .unwrap();
    ticket.forwarded(100);
    assert!(ticket.heartbeat().await.unwrap());
    ticket.forwarded(1_879_900);
    tokio::time::sleep(Duration::from_millis(2050)).await;
    drop(ticket);
    wait(async || {
        redis
            .hget::<_, _, String>(pool_key(&sid), "busy")
            .await
            .ok()
            .as_deref()
            == Some("0")
    })
    .await;
    assert_eq!(
        redis
            .hget::<_, _, u64>(metadata_key(&sid), "total_bytes")
            .await
            .unwrap(),
        1_880_000
    );
    assert_eq!(redis.get::<_, i64>(profile_key(profile)).await.unwrap(), 0);
    wait(async || !redis.exists::<_, bool>(api_key(&sid)).await.unwrap()).await;
    assert!(!redis.exists::<_, bool>(client_key(&sid)).await.unwrap());
    assert!(pool.shutdown().await);
    let _: () = redis.del(profile_key(profile)).await.unwrap();
}
#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn shutdown_signals_supervised_held_ticket_and_fenced_cleanup_preserves_successor_hash() {
    let (pool, mut redis, profile) = setup().await;
    let sid = uuid::Uuid::new_v4().to_string();
    let ticket = pool.open(request(&sid, profile)).await.unwrap();
    ticket
        .start("http://fixture.invalid/archive.ts", None, None)
        .await
        .unwrap();
    let mut stopped = ticket.stopped();
    let body = tokio::spawn(async move {
        stopped.wait_for(|value| *value).await.unwrap();
        drop(ticket);
    });
    assert!(pool.shutdown().await);
    body.await.unwrap();
    assert_eq!(redis.get::<_, i64>(profile_key(profile)).await.unwrap(), 0);
    assert!(!redis.exists::<_, bool>(pool_key(&sid)).await.unwrap());
    let _: () = redis.del(profile_key(profile)).await.unwrap();
    let (pool, mut redis, profile) = setup().await;
    let sid = uuid::Uuid::new_v4().to_string();
    let ticket = pool.open(request(&sid, profile)).await.unwrap();
    ticket
        .start("http://fixture.invalid/archive.ts", None, None)
        .await
        .unwrap();
    let _: () = redis
        .hset(pool_key(&sid), "rust_owner_token", "successor")
        .await
        .unwrap();
    drop(ticket);
    wait(async || {
        redis
            .get::<_, i64>(profile_key(profile))
            .await
            .unwrap_or(-1)
            == 0
    })
    .await;
    assert!(!pool.shutdown().await);
    assert_eq!(
        redis
            .hget::<_, _, String>(pool_key(&sid), "rust_owner_token")
            .await
            .unwrap(),
        "successor"
    );
    assert!(redis.exists::<_, bool>(client_key(&sid)).await.unwrap());
    let _: () = redis
        .del(&[
            pool_key(&sid),
            client_key(&sid),
            metadata_key(&sid),
            format!("timeshift:channel:123_{sid}:clients"),
            format!("timeshift:stream_gen:123_2026-11-09T11-00-00Z_fixture:{sid}"),
            profile_key(profile),
        ])
        .await
        .unwrap();
}
#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn missing_pool_hash_does_not_hide_owned_stats_from_shutdown_cleanup() {
    let (pool, mut redis, profile) = setup().await;
    let sid = uuid::Uuid::new_v4().to_string();
    let ticket = pool.open(request(&sid, profile)).await.unwrap();
    ticket
        .start("http://fixture.invalid/archive.ts", None, None)
        .await
        .unwrap();
    let _: () = redis.del(pool_key(&sid)).await.unwrap();
    drop(ticket);
    assert!(!pool.shutdown().await); // Eviction is recorded as a lifecycle fault.
    assert!(!redis.exists::<_, bool>(client_key(&sid)).await.unwrap());
    assert!(!redis.exists::<_, bool>(metadata_key(&sid)).await.unwrap());
    assert_eq!(redis.get::<_, i64>(profile_key(profile)).await.unwrap(), 0);
    let _: () = redis.del(profile_key(profile)).await.unwrap();
}

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn user_preemption_precedes_full_provider_and_failed_setup_rolls_back_pending_user_record() {
    let (pool, mut redis, profile) = setup().await;
    let sid = uuid::Uuid::new_v4().to_string();
    let user = fixture_user(&sid);
    let live_channel = uuid::Uuid::new_v4().to_string();
    let live_client = uuid::Uuid::new_v4().to_string();
    let live_key = format!("live:channel:{live_channel}:clients:{live_client}");
    let live_stop = format!("live:channel:{live_channel}:client:{live_client}:stop");
    let _: () = redis
        .hset_multiple(
            &live_key,
            &[("user_id", user.to_string()), ("connected_at", "1".into())],
        )
        .await
        .unwrap();
    let _: () = redis.set(profile_key(profile), 1).await.unwrap();
    let mut req = request(&sid, profile);
    req.stream_limit = 1;
    assert!(pool.open(req).await.is_err());
    assert_eq!(redis.get::<_, String>(&live_stop).await.unwrap(), "true");
    assert_eq!(
        redis
            .hget::<_, _, String>(&live_key, "rust_preempted")
            .await
            .unwrap(),
        "1"
    );
    assert_eq!(redis.get::<_, i64>(profile_key(profile)).await.unwrap(), 1);
    assert!(!redis.exists::<_, bool>(client_key(&sid)).await.unwrap());
    assert!(!redis.exists::<_, bool>(pool_key(&sid)).await.unwrap());
    assert!(pool.shutdown().await);
    let _: () = redis
        .del(&[live_key, live_stop, profile_key(profile)])
        .await
        .unwrap();
}
