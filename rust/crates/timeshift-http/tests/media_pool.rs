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
            ("rust_duration_secs".into(), "2100".into()),
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
async fn idle_reopen_preserves_connection_times_cumulative_bytes_and_cancels_old_cleanup() {
    let (pool, mut redis, profile) = setup().await;
    let sid = uuid::Uuid::new_v4().to_string();
    let first = pool.open(request(&sid, profile)).await.unwrap();
    first
        .start(
            "http://fixture.invalid/cached.ts",
            Some(15_001_836),
            Some("bytes=0-255"),
        )
        .await
        .unwrap();
    let connected: String = redis.hget(client_key(&sid), "connected_at").await.unwrap();
    let initialized: String = redis.hget(metadata_key(&sid), "init_time").await.unwrap();
    first.forwarded(256);
    drop(first);
    let second = pool.open(request(&sid, profile)).await.unwrap();
    assert_eq!(
        second.cached_final_url().await.unwrap().as_deref(),
        Some("http://fixture.invalid/cached.ts")
    );
    assert_eq!(redis.get::<_, i64>(profile_key(profile)).await.unwrap(), 1);
    second
        .start(
            "http://fixture.invalid/cached.ts",
            Some(15_001_836),
            Some("bytes=0-1879999"),
        )
        .await
        .unwrap();
    second.forwarded(1_880_000);
    assert!(second.heartbeat().await.unwrap());
    // Cross the previous startup-idle deadline while its successor is live.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(second.alive().await.unwrap());
    assert_eq!(
        redis
            .hget::<_, _, String>(client_key(&sid), "connected_at")
            .await
            .unwrap(),
        connected
    );
    assert_eq!(
        redis
            .hget::<_, _, String>(metadata_key(&sid), "init_time")
            .await
            .unwrap(),
        initialized
    );
    assert_eq!(
        redis
            .hget::<_, _, u64>(metadata_key(&sid), "total_bytes")
            .await
            .unwrap(),
        1_880_256
    );
    drop(second);
    let third = pool.open(request(&sid, profile)).await.unwrap();
    third
        .start(
            "http://fixture.invalid/cached.ts",
            Some(15_001_836),
            Some("bytes=1880000-1880255"),
        )
        .await
        .unwrap();
    assert!(
        (redis
            .hget::<_, _, f64>(client_key(&sid), "playback_base_secs")
            .await
            .unwrap()
            - 263.16778826271667)
            .abs()
            < 1e-10
    );
    third.forwarded(256);
    drop(third);
    wait(async || {
        redis
            .hget::<_, _, u64>(metadata_key(&sid), "total_bytes")
            .await
            .unwrap_or(0)
            == 1_880_512
    })
    .await;
    let anchor: String = redis
        .hget(client_key(&sid), "position_anchor_at")
        .await
        .unwrap();
    let _: () = redis.hset(client_key(&sid), "paused", "1").await.unwrap();
    let eof = pool.open(request(&sid, profile)).await.unwrap();
    eof.start(
        "http://fixture.invalid/cached.ts",
        Some(15_001_836),
        Some("bytes=14999900-15000155"),
    )
    .await
    .unwrap();
    assert_eq!(
        redis
            .hget::<_, _, String>(client_key(&sid), "position_anchor_at")
            .await
            .unwrap(),
        anchor
    );
    assert_eq!(
        redis
            .hget::<_, _, String>(client_key(&sid), "paused")
            .await
            .unwrap(),
        "1"
    );
    assert!(
        (redis
            .hget::<_, _, f64>(client_key(&sid), "playback_base_secs")
            .await
            .unwrap()
            - 263.16778826271667)
            .abs()
            < 1e-9
    );
    drop(eof);
    wait(async || !redis.exists::<_, bool>(pool_key(&sid)).await.unwrap()).await;
    assert_eq!(redis.get::<_, i64>(profile_key(profile)).await.unwrap(), 0);
    assert!(pool.shutdown().await);
    let _: () = redis.del(profile_key(profile)).await.unwrap();
}
#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn idle_reopen_preserves_source_same_session_exemption_under_new_positive_limit() {
    let (pool, mut redis, profile) = setup().await;
    let sid = uuid::Uuid::new_v4().to_string();
    let first = pool.open(request(&sid, profile)).await.unwrap();
    first
        .start("http://fixture.invalid/archive.ts", None, None)
        .await
        .unwrap();
    drop(first);
    let live = format!(
        "live:channel:{}:clients:{}",
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4()
    );
    let _: () = redis
        .hset_multiple(
            &live,
            &[
                ("user_id", fixture_user(&sid).to_string()),
                ("connected_at", "1".into()),
            ],
        )
        .await
        .unwrap();
    let mut updated = request(&sid, profile);
    updated.stream_limit = 1;
    updated.limit_policy.terminate_on_limit_exceeded = false;
    // Original check_user_stream_limit exempts this already registered SID,
    // including when another protocol is now counted at the positive limit.
    let reopened = pool.open(updated).await.unwrap();
    assert_eq!(redis.get::<_, i64>(profile_key(profile)).await.unwrap(), 1);
    assert!(redis.exists::<_, bool>(&live).await.unwrap());
    drop(reopened);
    wait(async || pool.is_idle()).await;
    assert_eq!(redis.get::<_, i64>(profile_key(profile)).await.unwrap(), 0);
    assert!(pool.shutdown().await);
    let _: () = redis.del(&[live, profile_key(profile)]).await.unwrap();
}
#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn busy_seek_drains_previous_body_retains_quota_and_supersedes_old_generation() {
    let (pool, mut redis, profile) = setup().await;
    let sid = uuid::Uuid::new_v4().to_string();
    let first = pool.open(request(&sid, profile)).await.unwrap();
    first
        .start(
            "http://fixture.invalid/cached.ts",
            Some(15_001_836),
            Some("bytes=0-"),
        )
        .await
        .unwrap();
    first.forwarded(400);
    let mut stopped = first.stopped();
    let old = tokio::spawn(async move {
        stopped.wait_for(|value| *value).await.unwrap();
        // Simulate the old supervised body draining an admitted mutation.
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop(first);
    });
    let cloned = pool.clone();
    let req = request(&sid, profile);
    let replacement = tokio::spawn(async move {
        cloned
            .open_playback(req, Some("bytes=1880000-1880255"))
            .await
    });
    let mut observations = 0;
    while !replacement.is_finished() {
        assert_eq!(redis.get::<_, i64>(profile_key(profile)).await.unwrap(), 1);
        observations += 1;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(observations >= 2);
    old.await.unwrap();
    let second = replacement.await.unwrap().unwrap();
    assert_eq!(redis.get::<_, i64>(profile_key(profile)).await.unwrap(), 1);
    assert_eq!(
        redis
            .hget::<_, _, String>(pool_key(&sid), "busy")
            .await
            .unwrap(),
        "1"
    );
    second
        .start(
            "http://fixture.invalid/cached.ts",
            Some(15_001_836),
            Some("bytes=1880000-1880255"),
        )
        .await
        .unwrap();
    let generation = format!("timeshift:stream_gen:123_2026-11-09T11-00-00Z_fixture:{sid}");
    assert_eq!(redis.get::<_, u64>(&generation).await.unwrap(), 2);
    let stop = format!("timeshift:channel:123_2026-11-09T11-00-00Z_fixture:client:{sid}:stop");
    for (value, alive) in [
        ("1", true),
        ("2", false),
        ("reuse", false),
        ("admin", false),
        ("hop", false),
        ("limit", false),
        ("true", true),
        ("unknown", true),
    ] {
        let _: () = redis.set(&stop, value).await.unwrap();
        assert_eq!(
            second.alive().await.unwrap(),
            alive,
            "source generation stop {value}"
        );
        assert_eq!(
            second.heartbeat().await.unwrap(),
            alive,
            "source generation heartbeat {value}"
        );
    }
    let _: () = redis.del(&stop).await.unwrap();
    assert!(second.alive().await.unwrap());
    second.forwarded(200);
    assert!(second.heartbeat().await.unwrap());
    assert_eq!(
        redis
            .hget::<_, _, u64>(metadata_key(&sid), "total_bytes")
            .await
            .unwrap(),
        600
    );
    drop(second);
    wait(async || pool.is_idle()).await;
    assert_eq!(redis.get::<_, i64>(profile_key(profile)).await.unwrap(), 0);
    assert!(!redis.exists::<_, bool>(&generation).await.unwrap());
    assert!(pool.shutdown().await);
    let _: () = redis.del(profile_key(profile)).await.unwrap();
}
#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn cancelled_busy_setup_still_drains_and_releases_the_transferred_reservation() {
    let (pool, mut redis, profile) = setup().await;
    let sid = uuid::Uuid::new_v4().to_string();
    let first = pool.open(request(&sid, profile)).await.unwrap();
    first
        .start(
            "http://fixture.invalid/archive.ts",
            Some(15_001_836),
            Some("bytes=0-"),
        )
        .await
        .unwrap();
    let mut stopped = first.stopped();
    let cloned = pool.clone();
    let req = request(&sid, profile);
    let call = tokio::spawn(async move {
        cloned
            .open_playback(req, Some("bytes=1880000-1880255"))
            .await
    });
    stopped.wait_for(|value| *value).await.unwrap();
    call.abort();
    let _ = call.await;
    assert_eq!(redis.get::<_, i64>(profile_key(profile)).await.unwrap(), 1);
    drop(first);
    wait(async || pool.is_idle()).await;
    assert_eq!(redis.get::<_, i64>(profile_key(profile)).await.unwrap(), 0);
    assert!(!redis.exists::<_, bool>(pool_key(&sid)).await.unwrap());
    assert!(!redis.exists::<_, bool>(client_key(&sid)).await.unwrap());
    assert!(pool.shutdown().await);
    let _: () = redis.del(profile_key(profile)).await.unwrap();
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

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn admin_stop_finalizer_retains_source_grace_only_hash_then_idle_fingerprint() {
    let (pool, mut redis, profile) = setup().await;
    let sid = uuid::Uuid::new_v4().to_string();
    let ticket = pool.open(request(&sid, profile)).await.unwrap();
    ticket
        .start("http://fixture.invalid/archive.ts", Some(15_001_836), None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2050)).await;
    let stop = format!("timeshift:channel:123_2026-11-09T11-00-00Z_fixture:client:{sid}:stop");
    let grace = format!("timeshift:grace:123_{sid}:{sid}");
    // The Source admin view deletes statistics/API before generator finally.
    let _: () = redis
        .del(&[
            client_key(&sid),
            metadata_key(&sid),
            api_key(&sid),
            format!("timeshift:channel:123_{sid}:clients"),
        ])
        .await
        .unwrap();
    let _: () = redis.set_ex(&stop, "admin", 60).await.unwrap();
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
    assert!(redis
        .hexists::<_, _, bool>(client_key(&sid), "stats_grace_deadline")
        .await
        .unwrap());
    assert!(redis
        .hexists::<_, _, bool>(client_key(&sid), "stats_grace_disconnected_at")
        .await
        .unwrap());
    assert!(!redis
        .hexists::<_, _, bool>(client_key(&sid), "programme_start")
        .await
        .unwrap());
    assert_eq!(redis.ttl::<_, i64>(&grace).await.unwrap(), 2);
    wait(async || pool.is_idle()).await;
    assert!(!redis.exists::<_, bool>(client_key(&sid)).await.unwrap());
    assert!(!redis.exists::<_, bool>(&grace).await.unwrap());
    assert!(redis.exists::<_, bool>(pool_key(&sid)).await.unwrap());
    assert!((1..=60).contains(&redis.ttl::<_, i64>(pool_key(&sid)).await.unwrap()));
    assert_eq!(redis.get::<_, String>(&stop).await.unwrap(), "admin");
    assert_eq!(redis.get::<_, i64>(profile_key(profile)).await.unwrap(), 0);
    assert!(pool.shutdown().await);
    let _: () = redis
        .del(&[pool_key(&sid), stop, profile_key(profile)])
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn established_reopen_cancels_grace_fields_and_old_cleanup_cannot_remove_successor() {
    let (pool, mut redis, profile) = setup().await;
    let sid = uuid::Uuid::new_v4().to_string();
    let ticket = pool.open(request(&sid, profile)).await.unwrap();
    ticket
        .start("http://fixture.invalid/archive.ts", None, None)
        .await
        .unwrap();
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
    let grace = format!("timeshift:grace:123_{sid}:{sid}");
    assert!(redis.exists::<_, bool>(&grace).await.unwrap());
    let successor = pool.open(request(&sid, profile)).await.unwrap();
    successor
        .start("http://fixture.invalid/archive.ts", None, None)
        .await
        .unwrap();
    assert!(!redis.exists::<_, bool>(&grace).await.unwrap());
    assert!(!redis
        .hexists::<_, _, bool>(client_key(&sid), "stats_grace_deadline")
        .await
        .unwrap());
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        redis
            .hget::<_, _, String>(pool_key(&sid), "busy")
            .await
            .unwrap(),
        "1"
    );
    assert_eq!(redis.get::<_, i64>(profile_key(profile)).await.unwrap(), 1);
    drop(successor);
    assert!(pool.shutdown().await);
    let _: () = redis.del(profile_key(profile)).await.unwrap();
}
