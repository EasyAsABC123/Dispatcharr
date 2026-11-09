//! Original Dispatcharr input key contracts against real Redis.
use dispatcharr_input_store::{Error, InputConfig, InputStore};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
fn url() -> String {
    std::env::var("REDIS_TEST_URL").expect("isolated Redis required")
}
fn channel() -> String {
    format!(
        "input-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
fn packet(byte: u8) -> [u8; 188] {
    let mut data = [byte; 188];
    data[0] = 0x47;
    data
}
#[tokio::test]
#[ignore = "requires real Redis"]
async fn status_samples_real_shared_chunks_and_reports_expired_latest_key() {
    let store = InputStore::connect(&url(), Default::default())
        .await
        .unwrap();
    let channel = channel();
    owner(&channel, "status-owner").await;
    for i in 1..=6 {
        store
            .put(&channel, "status-owner", &packet(i), f64::from(i), false)
            .await
            .unwrap();
    }
    let mut conn = connection().await;
    let _: () = redis::cmd("DEL")
        .arg(format!("live:channel:{channel}:input:buffer:chunk:4"))
        .query_async(&mut conn)
        .await
        .unwrap();
    let stats = store.buffer_diagnostics(&channel, 6).await.unwrap();
    assert_eq!(stats["chunks"], 6);
    assert_eq!(stats["keys_found"], serde_json::json!([2, 3, 5, 6]));
    assert_eq!(stats["keys_missing"], serde_json::json!([4]));
    assert_eq!(stats["total_sample_bytes"], 752);
    assert_eq!(stats["avg_chunk_size"], 188.0);
    assert_eq!(stats["is_ts_aligned"], true);
    assert_eq!(stats["diagnostics"]["first_chunk"]["first_byte"], 71);
    assert!(stats["latest_chunk_ttl"].as_i64().unwrap() > 0);
    for i in 1..=6 {
        let _: () = redis::cmd("DEL")
            .arg(format!("live:channel:{channel}:input:buffer:chunk:{i}"))
            .query_async(&mut conn)
            .await
            .unwrap();
    }
    let empty = store.buffer_diagnostics(&channel, 6).await.unwrap();
    assert_eq!(empty["latest_chunk_ttl"], -2);
    assert_eq!(
        empty["diagnostics"]["all_buffer_keys"],
        serde_json::json!([])
    );
    assert_eq!(empty["diagnostics"]["total_buffer_keys"], 0);
}
async fn connection() -> redis::aio::MultiplexedConnection {
    redis::Client::open(url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}
async fn owner(channel: &str, owner: &str) {
    let mut conn = connection().await;
    let _: () = redis::cmd("SET")
        .arg(format!("live:channel:{channel}:owner"))
        .arg(owner)
        .query_async(&mut conn)
        .await
        .unwrap();
}
#[tokio::test]
#[ignore = "requires real Redis"]
async fn exact_legacy_keys_shared_bytes_timestamps_discontinuities_and_age_selection() {
    let store = InputStore::connect(&url(), Default::default())
        .await
        .unwrap();
    let reader = InputStore::connect(&url(), Default::default())
        .await
        .unwrap();
    let channel = channel();
    owner(&channel, "first").await;
    assert_eq!(
        store
            .put(&channel, "first", &packet(1), 100.0, false)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .put(&channel, "first", &packet(2), 101.0, true)
            .await
            .unwrap(),
        2
    );
    let snapshot = reader.snapshot(&channel).await.unwrap();
    assert_eq!(snapshot.owner.as_deref(), Some("first"));
    assert_eq!(snapshot.index, 2);
    assert_eq!(snapshot.chunks.len(), 2);
    assert!(!snapshot.chunks[0].discontinuity);
    assert!(snapshot.chunks[1].discontinuity);
    assert_eq!(snapshot.cursor_for_age(1.0, 101.5), Some(0));
    assert_eq!(snapshot.cursor_for_age(100.0, 101.5), Some(0));
    assert_eq!(snapshot.cursor_for_age(0.0, 101.5), Some(1));
    drop(snapshot);
    let bytes = reader.get(&channel, 2).await.unwrap().unwrap();
    assert_eq!(bytes.as_ref(), &packet(2));
    assert_eq!(bytes.owner, "first");
    assert_eq!(bytes.received_at, 101.0);
    assert!(bytes.discontinuity);
    let mut conn = connection().await;
    let native: Vec<u8> = redis::cmd("GET")
        .arg(format!("live:channel:{channel}:input:buffer:chunk:2"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(native, packet(2));
    assert!(store.cleanup(&channel, "first").await.unwrap());
    let current: String = redis::cmd("GET")
        .arg(format!("live:channel:{channel}:owner"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(current, "first");
}
#[tokio::test]
#[ignore = "requires real Redis"]
async fn owner_change_hides_stale_media_before_first_put_and_fences_cleanup() {
    let store = InputStore::connect(&url(), Default::default())
        .await
        .unwrap();
    let channel = channel();
    owner(&channel, "old").await;
    store
        .put(&channel, "old", &packet(1), 10.0, false)
        .await
        .unwrap();
    let mut conn = connection().await;
    let counter = format!("profile_connections:input-test-{}", channel);
    let _: () = redis::cmd("SET")
        .arg(&counter)
        .arg(7)
        .query_async(&mut conn)
        .await
        .unwrap();
    owner(&channel, "new").await;
    assert!(store.get(&channel, 1).await.unwrap().is_none());
    let snapshot = store.snapshot(&channel).await.unwrap();
    assert_eq!(snapshot.owner.as_deref(), Some("new"));
    assert_eq!(snapshot.index, 0);
    assert!(snapshot.chunks.is_empty());
    drop(snapshot);
    assert_eq!(
        store.put(&channel, "old", &packet(3), 11.0, false).await,
        Err(Error::LostOwnership)
    );
    assert!(!store.cleanup(&channel, "old").await.unwrap());
    assert_eq!(
        store
            .put(&channel, "new", &packet(2), 12.0, false)
            .await
            .unwrap(),
        2
    );
    assert!(store.get(&channel, 1).await.unwrap().is_none());
    assert!(store.get(&channel, 2).await.unwrap().unwrap().discontinuity);
    assert!(!store.cleanup(&channel, "old").await.unwrap());
    assert!(store.cleanup(&channel, "new").await.unwrap());
    let actual: String = redis::cmd("GET")
        .arg(format!("live:channel:{channel}:owner"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(actual, "new");
    let profile: i64 = redis::cmd("GET")
        .arg(&counter)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(profile, 7);
    let _: () = redis::cmd("DEL")
        .arg(counter)
        .query_async(&mut conn)
        .await
        .unwrap();
}
#[tokio::test]
#[ignore = "requires real Redis"]
async fn bounded_retention_evicts_oldest_and_ttl_does_not_drop_coordinator_owner() {
    let store = InputStore::connect(
        &url(),
        InputConfig {
            max_chunk_bytes: 188,
            max_channel_bytes: 376,
            max_chunks: 2,
            chunk_ttl: Duration::from_millis(120),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let channel = channel();
    owner(&channel, "owner").await;
    for seq in 1..=3 {
        assert_eq!(
            store
                .put(&channel, "owner", &packet(seq), seq as f64, seq == 1)
                .await
                .unwrap(),
            seq as u64
        );
    }
    let snapshot = store.snapshot(&channel).await.unwrap();
    assert_eq!(
        snapshot
            .chunks
            .iter()
            .map(|stamp| stamp.sequence)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    drop(snapshot);
    assert!(store.get(&channel, 1).await.unwrap().is_none());
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(store.get(&channel, 3).await.unwrap().is_none());
    assert_eq!(
        store.snapshot(&channel).await.unwrap().owner.as_deref(),
        Some("owner")
    );
    assert_eq!(
        store
            .put(&channel, "owner", &packet(4), 4.0, false)
            .await
            .unwrap(),
        4
    );
    assert!(
        store.get(&channel, 4).await.unwrap().unwrap().discontinuity,
        "expired buffer fence starts a new era"
    );
    store.cleanup(&channel, "owner").await.unwrap();
}
#[tokio::test]
#[ignore = "requires real Redis"]
async fn cancelled_queued_binary_read_holds_budget_until_actual_response() {
    let store = InputStore::connect(
        &url(),
        InputConfig {
            max_chunk_bytes: 3008,
            max_channel_bytes: 6016,
            max_chunks: 2,
            max_read_bytes: 3008,
            command_timeout: Duration::from_millis(40),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let channel = channel();
    owner(&channel, "owner").await;
    store
        .put(&channel, "owner", &packet(1), 10.0, false)
        .await
        .unwrap();
    let snapshot = store.snapshot(&channel).await.unwrap();
    let entries = snapshot.chunks.clone();
    drop(snapshot);
    assert!(
        matches!(store.get(&channel, 1).await, Err(Error::Capacity)),
        "cloned metadata entries retain their admission"
    );
    drop(entries);
    let mut conn = connection().await;
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(200)
        .arg("ALL")
        .query_async(&mut conn)
        .await
        .unwrap();
    let reader = store.clone();
    let target = channel.clone();
    let task = tokio::spawn(async move { reader.get(&target, 1).await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    task.abort();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(matches!(store.get(&channel, 1).await, Err(Error::Capacity)));
    tokio::time::sleep(Duration::from_millis(240)).await;
    let held = store.get(&channel, 1).await.unwrap().unwrap();
    let held = std::sync::Arc::new(held);
    let cloned = held.clone();
    drop(held);
    assert!(matches!(store.get(&channel, 1).await, Err(Error::Capacity)));
    drop(cloned);
    assert!(store.get(&channel, 1).await.unwrap().is_some());
    store.cleanup(&channel, "owner").await.unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis and original source checkout"]
async fn age_cursor_matches_actual_upstream_lua_including_equal_scores_and_oldest_fallback() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .join("apps/proxy/live_proxy/input/buffer.py");
    let original =
        std::fs::read_to_string(path).expect("actual upstream StreamBuffer source required");
    let (_, tail) = original
        .split_once("_FIND_CHUNK_BY_TIME_LUA = \"\"\"")
        .unwrap();
    let (lua, _) = tail.split_once("\"\"\"").unwrap();
    let script = redis::Script::new(lua);
    let store = InputStore::connect(&url(), Default::default())
        .await
        .unwrap();
    let channel = channel();
    owner(&channel, "owner").await;
    for seq in 1..=100u64 {
        store
            .put(
                &channel,
                "owner",
                &packet(seq as u8),
                1000.0 + ((seq * 37) % 17) as f64 * 0.25,
                false,
            )
            .await
            .unwrap();
    }
    let snapshot = store.snapshot(&channel).await.unwrap();
    let mut conn = connection().await;
    let key = format!("live:channel:{channel}:input:buffer:chunk_timestamps");
    for quarter in 0..80 {
        let behind = quarter as f64 * 0.25;
        let chosen: i64 = script
            .key(&key)
            .arg(1006.0 - behind)
            .invoke_async(&mut conn)
            .await
            .unwrap();
        let expected = if chosen < 0 {
            let oldest: Vec<u64> = redis::cmd("ZRANGE")
                .arg(&key)
                .arg(0)
                .arg(0)
                .query_async(&mut conn)
                .await
                .unwrap();
            oldest.first().copied().unwrap().saturating_sub(1)
        } else {
            (chosen as u64).saturating_sub(1)
        };
        assert_eq!(
            snapshot.cursor_for_age(behind, 1006.0),
            Some(expected),
            "actual source Lua age lookup at {behind}s"
        );
    }
    store.cleanup(&channel, "owner").await.unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis"]
async fn abandoned_cleanup_requires_absent_owner_and_exact_old_buffer_fence() {
    let store = InputStore::connect(&url(), Default::default())
        .await
        .unwrap();
    let channel = channel();
    owner(&channel, "old").await;
    store
        .put(&channel, "old", &packet(1), 10.0, false)
        .await
        .unwrap();
    assert!(!store.cleanup_abandoned(&channel, "old").await.unwrap());
    owner(&channel, "new").await;
    assert!(
        !store.cleanup_abandoned(&channel, "old").await.unwrap(),
        "new ownership is protected before first publication"
    );
    store
        .put(&channel, "new", &packet(2), 11.0, false)
        .await
        .unwrap();
    let mut conn = connection().await;
    let _: () = redis::cmd("DEL")
        .arg(format!("live:channel:{channel}:owner"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(!store.cleanup_abandoned(&channel, "old").await.unwrap());
    assert!(store.cleanup_abandoned(&channel, "new").await.unwrap());
    let remaining: i64 = redis::cmd("EXISTS")
        .arg(format!("live:channel:{channel}:input:buffer:index"))
        .arg(format!("live:channel:{channel}:input:buffer:chunk:2"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(remaining, 0);
}
