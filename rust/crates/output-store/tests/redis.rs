//! Real Redis output-buffer contracts. Original Dispatcharr author credit retained.
use dispatcharr_output_store::{Error, OutputConfig, OutputStore};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
fn url() -> String {
    std::env::var("REDIS_TEST_URL").expect("requires real isolated Redis")
}
fn channel() -> String {
    format!(
        "output-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}
#[tokio::test]
#[ignore = "requires real Redis"]
async fn shared_binary_chunks_publish_and_fencing_after_takeover() {
    let config = OutputConfig {
        owner_ttl: Duration::from_millis(100),
        ..Default::default()
    };
    let first = OutputStore::connect(&url(), config.clone()).await.unwrap();
    let second = OutputStore::connect(&url(), config).await.unwrap();
    let channel = channel();
    let owner = first
        .claim(&channel, "hls", "owner1")
        .await
        .unwrap()
        .unwrap();
    assert!(second
        .claim(&channel, "hls", "owner2")
        .await
        .unwrap()
        .is_none());
    let data = [0, 255, 71, 188, 0, 2];
    assert_eq!(owner.put(&data, now()).await.unwrap(), 1);
    owner
        .publish(&serde_json::json!({"window":[{"seq":1,"dur":4.0,"disc":false}]}))
        .await
        .unwrap();
    assert_eq!(
        second
            .get(&channel, "hls", 1)
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        &data
    );
    let snapshot = second.snapshot(&channel, "hls").await.unwrap();
    assert_eq!(snapshot.index, 1);
    assert_eq!(snapshot.segments.len(), 1);
    assert!(snapshot.descriptor.is_some());
    tokio::time::sleep(Duration::from_millis(130)).await;
    let successor = second
        .claim(&channel, "hls", "owner2")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(owner.renew().await, Err(Error::LostOwnership));
    assert!(!owner.release().await.unwrap());
    assert_eq!(owner.put(b"stale", now()).await, Err(Error::LostOwnership));
    assert_eq!(successor.put(b"next", now()).await.unwrap(), 2);
    assert!(successor.release().await.unwrap());
    assert!(first.get(&channel, "hls", 1).await.unwrap().is_none());
}
#[tokio::test]
#[ignore = "requires real isolated Redis"]
async fn profiled_hls_readiness_follows_the_current_output_owner() {
    let first = OutputStore::connect(
        &url(),
        OutputConfig {
            owner_ttl: Duration::from_millis(300),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let second = OutputStore::connect(&url(), Default::default())
        .await
        .unwrap();
    let channel = channel();
    let format = "hls:p17";
    let owner = first
        .claim(&channel, format, "owner1")
        .await
        .unwrap()
        .unwrap();
    owner.set_state("active").await.unwrap();
    assert!(second.is_active(&channel, format).await.unwrap());

    tokio::time::timeout(Duration::from_secs(5), async {
        while second.is_active(&channel, format).await.unwrap() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        second.state(&channel, format).await.unwrap().as_deref(),
        Some("active")
    );

    let successor = second
        .claim(&channel, format, "owner2")
        .await
        .unwrap()
        .unwrap();
    assert!(!first.is_active(&channel, format).await.unwrap());
    assert_eq!(
        first.state(&channel, format).await.unwrap().as_deref(),
        Some("initializing")
    );
    assert_eq!(owner.set_state("active").await, Err(Error::LostOwnership));
    successor.set_state("active").await.unwrap();
    assert!(first.is_active(&channel, format).await.unwrap());
    assert!(successor.release().await.unwrap());
}

#[tokio::test]
#[ignore = "requires real Redis"]
async fn retained_read_owner_and_channel_capacity_survive_clones_and_ttl() {
    let store = OutputStore::connect(
        &url(),
        OutputConfig {
            max_segment_bytes: 8,
            max_channel_bytes: 12,
            max_read_bytes: 8,
            max_segments: 2,
            chunk_ttl: Duration::from_millis(150),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let channel = channel();
    let owner = store
        .claim(&channel, "hls", "owner")
        .await
        .unwrap()
        .unwrap();
    owner.put(b"abcdefgh", now()).await.unwrap();
    assert_eq!(owner.put(b"12345", now()).await, Err(Error::Capacity));
    let retained = store.get(&channel, "hls", 1).await.unwrap().unwrap();
    assert!(matches!(
        store.get(&channel, "hls", 1).await,
        Err(Error::Capacity)
    ));
    let retained = std::sync::Arc::new(retained);
    let clone = retained.clone();
    drop(retained);
    assert!(matches!(
        store.get(&channel, "hls", 1).await,
        Err(Error::Capacity)
    ));
    drop(clone);
    assert!(store.get(&channel, "hls", 1).await.unwrap().is_some());
    tokio::time::sleep(Duration::from_millis(180)).await;
    assert_eq!(owner.put(b"12345", now()).await.unwrap(), 2);
    assert_eq!(
        store
            .snapshot(&channel, "hls")
            .await
            .unwrap()
            .segments
            .len(),
        1
    );
    owner.release().await.unwrap();
}
#[tokio::test]
#[ignore = "requires real Redis"]
async fn corrupt_indices_and_oversized_reads_do_not_publish_partial_writes() {
    let store = OutputStore::connect(
        &url(),
        OutputConfig {
            max_segment_bytes: 8,
            max_read_bytes: 8,
            max_channel_bytes: 16,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let channel = channel();
    let owner = store
        .claim(&channel, "hls", "owner")
        .await
        .unwrap()
        .unwrap();
    let redis = redis::Client::open(url()).unwrap();
    let mut conn = redis.get_multiplexed_async_connection().await.unwrap();
    let index = format!("live:channel:{channel}:output:hls:buffer:index");
    let chunk = format!("live:channel:{channel}:output:hls:buffer:chunk:1");
    let _: () = redis::cmd("SET")
        .arg(&index)
        .arg("invalid")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(owner.put(b"new", now()).await, Err(Error::Corrupt));
    let exists: i64 = redis::cmd("EXISTS")
        .arg(&chunk)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(exists, 0);
    let _: () = redis::cmd("SET")
        .arg(&chunk)
        .arg("0123456789")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(matches!(
        store.get(&channel, "hls", 1).await,
        Err(Error::Corrupt)
    ));
    let _: () = redis::cmd("DEL")
        .arg(index)
        .arg(chunk)
        .query_async(&mut conn)
        .await
        .unwrap();
    owner.release().await.unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis"]
async fn fmp4_init_state_and_profile_keys_share_fenced_lifetime() {
    let store = OutputStore::connect(&url(), OutputConfig::default())
        .await
        .unwrap();
    let channel = channel();
    let lease = store
        .claim(&channel, "fmp4:p42", "fmp4owner")
        .await
        .unwrap()
        .unwrap();
    lease.put_init(b"ftyp-and-moov").await.unwrap();
    lease.set_state("active").await.unwrap();
    assert_eq!(
        store.state(&channel, "fmp4:p42").await.unwrap().as_deref(),
        Some("active")
    );
    assert_eq!(
        store
            .get_init(&channel, "fmp4:p42")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"ftyp-and-moov"
    );
    assert!(store.get_init(&channel, "fmp4").await.unwrap().is_none());
    lease.release().await.unwrap();
    assert!(store.state(&channel, "fmp4:p42").await.unwrap().is_none());
    assert!(store
        .get_init(&channel, "fmp4:p42")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
#[ignore = "requires real Redis"]
async fn cancelled_or_timed_out_reads_keep_admission_until_redis_response() {
    let store = OutputStore::connect(
        &url(),
        OutputConfig {
            max_segment_bytes: 8,
            max_read_bytes: 8,
            max_channel_bytes: 16,
            max_operations: 1,
            command_timeout: Duration::from_millis(40),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let channel = channel();
    let owner = store
        .claim(&channel, "hls", "owner")
        .await
        .unwrap()
        .unwrap();
    owner.put(b"12345678", now()).await.unwrap();
    let mut conn = redis::Client::open(url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(200)
        .arg("ALL")
        .query_async(&mut conn)
        .await
        .unwrap();
    let reader = store.clone();
    let target = channel.clone();
    let task = tokio::spawn(async move { reader.get(&target, "hls", 1).await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    task.abort();
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(
        matches!(store.get(&channel, "hls", 1).await, Err(Error::Capacity)),
        "cancelled caller must not free an inflight response budget"
    );
    tokio::time::sleep(Duration::from_millis(220)).await;
    assert!(store.get(&channel, "hls", 1).await.unwrap().is_some());
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(200)
        .arg("ALL")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(matches!(
        store.get(&channel, "hls", 1).await,
        Err(Error::Unavailable)
    ));
    assert!(
        matches!(store.get(&channel, "hls", 1).await, Err(Error::Capacity)),
        "deadline does not cancel the supervised Redis response"
    );
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(store.get(&channel, "hls", 1).await.unwrap().is_some());
    owner.release().await.unwrap();
}

#[tokio::test]
#[ignore = "requires real isolated Redis"]
async fn profile_writer_requires_actual_raw_owner_and_cannot_delete_successor_or_raw_history() {
    let store = OutputStore::connect(&url(), OutputConfig::default())
        .await
        .unwrap();
    let profile = store.profile(7).unwrap();
    let channel = channel();
    let raw_key = format!("live:channel:{channel}:owner");
    let mut redis = redis::Client::open(url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg(&raw_key)
        .arg("raw-owner-one")
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(profile
        .claim_retained(&channel, "foreign-raw-owner", "profile-one", ())
        .await
        .unwrap()
        .0
        .is_none());
    let first = profile
        .claim_retained(&channel, "raw-owner-one", "profile-one", ())
        .await
        .unwrap()
        .0
        .unwrap();
    assert!(profile
        .claim_retained(&channel, "raw-owner-one", "profile-two", ())
        .await
        .unwrap()
        .0
        .is_none());
    first.set_state_retained("active", ()).await.unwrap();
    assert_eq!(
        first
            .put_retained(b"actual-transformed-payload", now(), ())
            .await
            .unwrap()
            .0,
        1
    );
    assert_eq!(
        profile.get(&channel, 1).await.unwrap().unwrap().as_ref(),
        b"actual-transformed-payload"
    );
    assert_eq!(
        profile.state(&channel).await.unwrap().as_deref(),
        Some("active")
    );
    let ttl: i64 = redis::cmd("PTTL")
        .arg(format!("live:channel:{channel}:output:mpegts:p7:owner"))
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!((3_500_000..=3_600_000).contains(&ttl));
    let _: () = redis::cmd("SET")
        .arg(&raw_key)
        .arg("raw-owner-two")
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(
        first.put_retained(b"stale", now(), ()).await.unwrap_err(),
        Error::LostOwnership
    );
    assert_eq!(
        first.renew_retained(()).await.unwrap_err(),
        Error::LostOwnership
    );
    assert_eq!(
        first.set_state_retained("active", ()).await.unwrap_err(),
        Error::LostOwnership
    );
    assert!(first.release_retained(()).await.unwrap().0);
    let second = profile
        .claim_retained(&channel, "raw-owner-two", "profile-two", ())
        .await
        .unwrap()
        .0
        .unwrap();
    assert!(!first.release_retained(()).await.unwrap().0);
    assert_eq!(
        profile.snapshot(&channel).await.unwrap().owner.as_deref(),
        Some("profile-two")
    );
    let raw: String = redis::cmd("GET")
        .arg(&raw_key)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(raw, "raw-owner-two");
    assert!(second.release_retained(()).await.unwrap().0);
    let _: usize = redis::cmd("DEL")
        .arg(raw_key)
        .query_async(&mut redis)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires real isolated Redis"]
async fn profile_cursor_read_atomically_rejects_successor_sequence_while_handed_bytes_remain_owned()
{
    let store = OutputStore::connect(
        &url(),
        OutputConfig {
            max_segment_bytes: 8,
            max_read_bytes: 16,
            max_channel_bytes: 32,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let channel = channel();
    let profile = store.profile(7).unwrap();
    let mut redis = redis::Client::open(url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let raw_key = format!("live:channel:{channel}:owner");
    let _: () = redis::cmd("SET")
        .arg(&raw_key)
        .arg("raw-one")
        .query_async(&mut redis)
        .await
        .unwrap();
    let old = profile
        .claim_retained(&channel, "raw-one", "profile-one", ())
        .await
        .unwrap()
        .0
        .unwrap();
    old.put(b"old-data", now()).await.unwrap();
    let (handed, ()) = profile
        .get_owned_retained(&channel, "profile-one", 1, ())
        .await
        .unwrap();
    let handed = handed.unwrap();
    let stale_snapshot = profile.snapshot(&channel).await.unwrap();
    assert_eq!(stale_snapshot.owner.as_deref(), Some("profile-one"));
    // The real predecessor releases after the cursor snapshot but before GET.
    assert!(old.release_retained(()).await.unwrap().0);
    let _: () = redis::cmd("SET")
        .arg(&raw_key)
        .arg("raw-two")
        .query_async(&mut redis)
        .await
        .unwrap();
    let next = profile
        .claim_retained(&channel, "raw-two", "profile-two", ())
        .await
        .unwrap()
        .0
        .unwrap();
    assert_eq!(next.put(b"new-data", now()).await.unwrap(), 1);
    assert!(matches!(
        profile
            .get_owned_retained(&channel, "profile-one", 1, ())
            .await,
        Err(Error::LostOwnership)
    ));
    assert_eq!(
        handed.as_ref(),
        b"old-data",
        "already handed frame remains stable after old Redis keys retire"
    );
    let (fresh, ()) = profile
        .get_owned_retained(&channel, "profile-two", 1, ())
        .await
        .unwrap();
    assert_eq!(fresh.unwrap().as_ref(), b"new-data");
    assert!(
        !old.release_retained(()).await.unwrap().0,
        "stale cleanup must not remove successor"
    );
    assert_eq!(next.put(b"more", now()).await.unwrap(), 2);
    drop(handed);
    assert!(next.release_retained(()).await.unwrap().0);
    let _: usize = redis::cmd("DEL")
        .arg(raw_key)
        .query_async(&mut redis)
        .await
        .unwrap();
}
