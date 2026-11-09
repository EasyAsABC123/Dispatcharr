//! Real Redis contracts. Run explicitly with the disposable-container runner.
use dispatcharr_clients::{ClientConfig, ClientLease, ClientStore, Error};
use dispatcharr_contracts::{PlaybackViewer, UserLimitPolicy};
use redis::AsyncCommands;
use std::{net::IpAddr, time::Duration};

fn user() -> PlaybackViewer {
    PlaybackViewer {
        user_id: Some((uuid::Uuid::new_v4().as_u128() % 1_000_000_000) as i64 + 1000),
        username: Some("fixture".into()),
        ip_address: "127.0.0.1".parse::<IpAddr>().unwrap(),
        user_agent: "fixture-client".into(),
        stream_limit: 1,
        limit_policy: UserLimitPolicy::default(),
    }
}

fn channel() -> String {
    uuid::Uuid::new_v4().to_string()
}
fn url() -> String {
    std::env::var("REDIS_TEST_URL").expect("REDIS_TEST_URL must point to a disposable Redis")
}
fn config() -> ClientConfig {
    ClientConfig {
        worker_id: format!("rust-test-{}", uuid::Uuid::new_v4()),
        flush_interval: Duration::from_millis(25),
        ttl: Duration::from_secs(5),
        ghost_timeout: Duration::from_secs(2),
        ..ClientConfig::default()
    }
}
async fn store() -> ClientStore {
    ClientStore::connect(&url(), config()).await.unwrap()
}
async fn connection() -> redis::aio::MultiplexedConnection {
    redis::Client::open(url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}
async fn wait(mut check: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if check().await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Redis lifecycle deadline");
}
fn key(channel: &str, lease: &ClientLease) -> String {
    format!("live:channel:{channel}:clients:{}", lease.id())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Redis; execute run-redis.py --clients"]
async fn atomic_limit_across_replicas_admits_exactly_one() {
    let first = store().await;
    let second = store().await;
    let mut user = user();
    user.limit_policy.terminate_on_limit_exceeded = false;
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..32 {
        let store = if index % 2 == 0 {
            first.clone()
        } else {
            second.clone()
        };
        let user = user.clone();
        tasks.spawn(async move { store.register(&channel(), &user, "ts").await });
    }
    let mut admitted = Vec::new();
    let mut rejected = 0;
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            Ok(lease) => admitted.push(lease),
            Err(Error::UserLimit) => rejected += 1,
            Err(error) => panic!("{error:?}"),
        }
    }
    assert_eq!(admitted.len(), 1);
    assert_eq!(rejected, 31);
    for lease in admitted {
        lease.release().await.unwrap();
    }
    first.shutdown().await;
    second.shutdown().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis; execute run-redis.py --clients"]
async fn legacy_metadata_and_batched_stats_and_admin_stop_match_keys() {
    let store = store().await;
    let mut user = user();
    user.stream_limit = 0;
    let channel = channel();
    let lease = store.register(&channel, &user, "ts").await.unwrap();
    let mut connection = connection().await;
    let metadata = key(&channel, &lease);
    let values: Vec<String> = connection
        .hget(
            &metadata,
            &["user_id", "ip_address", "output_format", "worker_id"],
        )
        .await
        .unwrap();
    assert_eq!(values[0], user.user_id.unwrap().to_string());
    assert_eq!(values[1], "127.0.0.1");
    assert_eq!(values[2], "ts");
    assert!(values[3].starts_with("rust-test-"));
    let included: bool = connection
        .sismember(format!("live:channel:{channel}:clients"), lease.id())
        .await
        .unwrap();
    assert!(included);
    lease.record_bytes(188);
    wait(async || {
        connection
            .hget::<_, _, Option<u64>>(&metadata, "bytes_sent")
            .await
            .unwrap()
            == Some(188)
    })
    .await;
    let stats: Vec<Option<String>> = connection
        .hget(
            &metadata,
            &[
                "chunks_sent",
                "current_rate_KBps",
                "avg_rate_KBps",
                "stats_updated_at",
                "current_rate",
            ],
        )
        .await
        .unwrap();
    assert_eq!(stats[0].as_deref(), Some("1"));
    assert!(stats[1].as_ref().unwrap().parse::<f64>().unwrap() >= 0.0);
    assert!(stats[2].as_ref().unwrap().parse::<f64>().unwrap() > 0.0);
    assert!(stats[3].as_ref().unwrap().parse::<f64>().unwrap() > 0.0);
    assert!(stats[4].is_none());
    let mut stopped = lease.stopped();
    assert!(store.stop_client(&channel, lease.id()).await.unwrap());
    tokio::time::timeout(Duration::from_secs(2), stopped.wait_for(|value| *value))
        .await
        .unwrap()
        .unwrap();
    lease.release().await.unwrap();
    assert!(!connection.exists::<_, bool>(&metadata).await.unwrap());
    assert!(!connection
        .sismember::<_, _, bool>(format!("live:channel:{channel}:clients"), lease.id())
        .await
        .unwrap());
    store.shutdown().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis; execute run-redis.py --clients"]
async fn unique_channel_reconnect_then_oldest_channel_preemption_is_atomic() {
    let store = store().await;
    let mut user = user();
    user.limit_policy.ignore_same_channel_connections = true;
    let first_channel = channel();
    let second_channel = channel();
    let first = store.register(&first_channel, &user, "ts").await.unwrap();
    let sibling = store.register(&first_channel, &user, "ts").await.unwrap();
    let replacement = store.register(&second_channel, &user, "ts").await.unwrap();
    let mut first_stop = first.stopped();
    let mut sibling_stop = sibling.stopped();
    tokio::time::timeout(Duration::from_secs(2), first_stop.wait_for(|value| *value))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        sibling_stop.wait_for(|value| *value),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!*replacement.stopped().borrow());
    for lease in [first, sibling, replacement] {
        lease.release().await.unwrap();
    }
    store.shutdown().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis; execute run-redis.py --clients"]
async fn existing_python_live_vod_and_timeshift_count_against_limit() {
    let store = store().await;
    let mut user = user();
    user.limit_policy.terminate_on_limit_exceeded = false;
    let mut connection = connection().await;
    for prefix in ["live", "timeshift", "vod"] {
        let media = channel();
        let client = uuid::Uuid::new_v4().simple().to_string();
        let metadata = if prefix == "vod" {
            format!("vod_persistent_connection:{client}")
        } else {
            format!("{prefix}:channel:{media}:clients:{client}")
        };
        let _: () = connection
            .hset_multiple(
                &metadata,
                &[
                    ("user_id", user.user_id.unwrap().to_string()),
                    ("connected_at", "1".into()),
                    ("created_at", "1".into()),
                    ("content_uuid", media),
                ],
            )
            .await
            .unwrap();
        assert!(
            matches!(
                store.register(&channel(), &user, "ts").await,
                Err(Error::UserLimit)
            ),
            "{prefix} connection was not counted"
        );
        let _: () = connection.del(metadata).await.unwrap();
    }
    store.shutdown().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis; execute run-redis.py --clients"]
async fn full_byte_owner_lifetime_does_not_release_metadata_early() {
    let store = store().await;
    let mut user = user();
    user.stream_limit = 0;
    let channel = channel();
    let lease = store.register(&channel, &user, "ts").await.unwrap();
    let held_frame = lease.clone();
    let metadata = key(&channel, &lease);
    drop(lease);
    let mut connection = connection().await;
    assert!(connection.exists::<_, bool>(&metadata).await.unwrap());
    drop(held_frame);
    wait(async || !connection.exists::<_, bool>(&metadata).await.unwrap()).await;
    store.shutdown().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis; execute run-redis.py --clients"]
async fn missing_metadata_and_stalled_clients_cancel_and_shutdown_removes_owned_data() {
    let store = store().await;
    let mut user = user();
    user.stream_limit = 0;
    let channel = channel();
    let first = store.register(&channel, &user, "ts").await.unwrap();
    let second = store.register(&channel, &user, "ts").await.unwrap();
    let mut connection = connection().await;
    let _: () = connection
        .hset(key(&channel, &first), "last_active", "0")
        .await
        .unwrap();
    let mut stopped = first.stopped();
    tokio::time::timeout(Duration::from_secs(2), stopped.wait_for(|value| *value))
        .await
        .unwrap()
        .unwrap();
    store.shutdown().await;
    assert!(*second.stopped().borrow());
    assert!(!connection
        .exists::<_, bool>(key(&channel, &second))
        .await
        .unwrap());
    assert!(matches!(
        store.register(&channel, &user, "ts").await,
        Err(Error::Stopped)
    ));
}

#[tokio::test]
#[ignore = "requires disposable Redis; execute run-redis.py --clients"]
async fn hls_capability_remint_touch_and_release_are_atomic() {
    let store = store().await;
    let mut user = user();
    user.stream_limit = 0;
    let channel = channel();
    let lease = store.register(&channel, &user, "hls").await.unwrap();
    let first = "a".repeat(32);
    let second = "b".repeat(32);
    lease.bind_hls_token(&first, user.user_id).await.unwrap();
    let info = store.touch_hls_session(&first).await.unwrap().unwrap();
    assert_eq!(info.channel_id, channel);
    assert_eq!(info.client_id, lease.id());
    assert_eq!(info.user_id, user.user_id);
    lease.bind_hls_token(&second, user.user_id).await.unwrap();
    assert!(store.touch_hls_session(&first).await.unwrap().is_none());
    assert!(store.get_hls_session(&second).await.unwrap().is_some());
    let mut connection = connection().await;
    let bytes: Option<String> = connection
        .hget(key(&channel, &lease), "bytes_sent")
        .await
        .unwrap();
    assert!(
        bytes.is_none(),
        "HLS must not invent native TS byte telemetry"
    );
    lease.release().await.unwrap();
    assert!(store.get_hls_session(&second).await.unwrap().is_none());
    store.shutdown().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis; execute run-redis.py --clients"]
async fn hls_cold_reconnect_and_published_descriptor_prevent_abandonment() {
    let store = store().await;
    let mut user = user();
    user.stream_limit = 0;
    let channel = channel();
    let lease = store.register(&channel, &user, "hls").await.unwrap();
    let token = "c".repeat(32);
    lease.bind_hls_token(&token, user.user_id).await.unwrap();
    assert_eq!(store.enter_hls_cold_start(&token).await.unwrap(), Some(1));
    let playlist = format!("live:channel:{channel}:output:hls:playlist");
    assert!(store
        .claim_abandoned_hls(&token, &playlist, f64::MAX)
        .await
        .unwrap()
        .is_none());
    assert_eq!(store.exit_hls_cold_start(&token).await.unwrap(), Some(0));
    assert!(store
        .claim_output_owner(&channel, "hls", "worker-a", Duration::from_secs(5))
        .await
        .unwrap());
    assert!(store
        .publish_output_playlist(
            &channel,
            "hls",
            "worker-a",
            &serde_json::json!({"ready":true}),
            Duration::from_secs(60)
        )
        .await
        .unwrap());
    assert!(store
        .claim_abandoned_hls(&token, &playlist, f64::MAX)
        .await
        .unwrap()
        .is_none());
    assert!(store
        .release_output_owner(&channel, "hls", "worker-a")
        .await
        .unwrap());
    let claimed = store
        .claim_abandoned_hls(&token, &playlist, f64::MAX)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed, (channel.clone(), lease.id().to_owned()));
    assert!(store.get_hls_session(&token).await.unwrap().is_none());
    lease.release().await.unwrap();
    store.shutdown().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis; execute run-redis.py --clients"]
async fn output_and_channel_metadata_are_fenced_against_stale_publish_and_release() {
    let store = store().await;
    let channel = channel();
    let mut connection = connection().await;
    assert!(store
        .claim_output_owner(&channel, "hls", "old", Duration::from_secs(5))
        .await
        .unwrap());
    assert!(!store
        .claim_output_owner(&channel, "hls", "new", Duration::from_secs(5))
        .await
        .unwrap());
    let owner = format!("live:channel:{channel}:output:hls:owner");
    let _: () = connection.set(&owner, "new").await.unwrap();
    assert!(!store
        .publish_output_playlist(
            &channel,
            "hls",
            "old",
            &serde_json::json!({"old":true}),
            Duration::from_secs(5)
        )
        .await
        .unwrap());
    assert!(!store
        .release_output_owner(&channel, "hls", "old")
        .await
        .unwrap());
    assert!(store
        .publish_output_playlist(
            &channel,
            "hls",
            "new",
            &serde_json::json!({"ready":true}),
            Duration::from_secs(5)
        )
        .await
        .unwrap());
    let channel_owner = format!("live:channel:{channel}:owner");
    let _: () = connection.set(&channel_owner, "new-owner").await.unwrap();
    let fields = [("state".to_owned(), "active".to_owned())]
        .into_iter()
        .collect();
    assert!(matches!(
        store
            .publish_channel(&channel, "old-owner", "old-fence", &fields, 1)
            .await,
        Err(Error::Stopped)
    ));
    store
        .publish_channel(&channel, "new-owner", "new-fence", &fields, 2)
        .await
        .unwrap();
    store.remove_channel(&channel, "old-fence").await.unwrap();
    assert_eq!(
        connection
            .hget::<_, _, String>(format!("live:channel:{channel}:metadata"), "state")
            .await
            .unwrap(),
        "active"
    );
    store.remove_channel(&channel, "new-fence").await.unwrap();
    assert!(store
        .release_output_owner(&channel, "hls", "new")
        .await
        .unwrap());
    let _: () = connection.del(channel_owner).await.unwrap();
    store.shutdown().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis"]
async fn hls_readonly_inspection_validates_binding_without_activity_or_ttl_refresh() {
    let store = ClientStore::connect(
        &url(),
        ClientConfig {
            flush_interval: Duration::from_secs(4),
            ..config()
        },
    )
    .await
    .unwrap();
    let channel = channel();
    let viewer = user();
    let lease = store.register(&channel, &viewer, "hls").await.unwrap();
    let token = "Readonly_HLS_Token_0123456789012";
    assert_eq!(token.len(), 32);
    lease.bind_hls_token(token, viewer.user_id).await.unwrap();
    let mut conn = connection().await;
    let capability = format!("live:hls:session:{token}");
    let row = key(&channel, &lease);
    let _: () = conn.hset(&row, "last_active", "123.5").await.unwrap();
    let _: bool = conn.pexpire(&capability, 500).await.unwrap();
    let ttl_before: i64 = conn.pttl(&capability).await.unwrap();
    let result = store.inspect_hls_session(token).await.unwrap().unwrap();
    assert_eq!(result.metadata["last_active"], "123.5");
    assert_eq!(result.client_id, lease.id());
    let ttl_after: i64 = conn.pttl(&capability).await.unwrap();
    assert!(
        ttl_after <= ttl_before,
        "read-only inspection must not refresh token TTL"
    );
    let _: () = conn.hset(&row, "hls_token", "different").await.unwrap();
    assert!(store.inspect_hls_session(token).await.unwrap().is_none());
    let _: () = conn.hset(&row, "hls_token", token).await.unwrap();
    let _: () = conn
        .set(format!("live:channel:{channel}:stopping"), "1")
        .await
        .unwrap();
    assert!(store.inspect_hls_session(token).await.unwrap().is_none());
    lease.release().await.unwrap();
    store.shutdown().await;
}
