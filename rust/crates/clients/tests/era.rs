//! Real Redis era bindings protect new/pre-owner/foreign client generations.
use dispatcharr_clients::{ClientConfig, ClientStore, Error};
use dispatcharr_contracts::{PlaybackViewer, UserLimitPolicy};
use redis::AsyncCommands;
use std::time::Duration;
use tokio::{sync::Mutex, time};
static EXCLUSIVE: Mutex<()> = Mutex::const_new(());
fn url() -> String {
    std::env::var("REDIS_TEST_URL").expect("isolated Redis")
}
fn channel() -> String {
    format!("era-{}", uuid::Uuid::new_v4())
}
fn viewer() -> PlaybackViewer {
    PlaybackViewer {
        user_id: None,
        username: None,
        ip_address: "127.0.0.1".parse().unwrap(),
        user_agent: "era-fixture".into(),
        stream_limit: 0,
        limit_policy: UserLimitPolicy::default(),
    }
}
async fn store(timeout: Duration) -> ClientStore {
    ClientStore::connect(
        &url(),
        ClientConfig {
            max_clients: 8,
            command_timeout: timeout,
            flush_interval: Duration::from_secs(30),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}
async fn connection() -> redis::aio::MultiplexedConnection {
    redis::Client::open(url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}
fn row(channel: &str, id: &str) -> String {
    format!("live:channel:{channel}:clients:{id}")
}
async fn owner(connection: &mut redis::aio::MultiplexedConnection, channel: &str, value: &str) {
    let _: () = connection
        .set(format!("live:channel:{channel}:owner"), value)
        .await
        .unwrap();
}
#[tokio::test]
#[ignore = "requires isolated Redis"]
async fn binding_only_local_empty_same_era_and_abandoned_cleanup_preserves_new_and_unbound_rows() {
    let _exclusive = EXCLUSIVE.lock().await;
    let first = store(Duration::from_secs(1)).await;
    let second = store(Duration::from_secs(1)).await;
    let channel = channel();
    let mut conn = connection().await;
    let old = first.register(&channel, &viewer(), "hls").await.unwrap();
    let unbound = second
        .register(&channel, &viewer(), "mpegts")
        .await
        .unwrap();
    let token = "Era_HLS_Token_012345678901234567";
    assert_eq!(token.len(), 32);
    old.bind_hls_token(token, None).await.unwrap();
    owner(&mut conn, &channel, "owner1").await;
    assert_eq!(first.bind_local_owner(&channel, "owner1").await.unwrap(), 1);
    let era: String = conn
        .hget(row(&channel, old.id()), "rust_input_owner")
        .await
        .unwrap();
    assert_eq!(era, "owner1");
    let era: String = conn
        .hget(row(&channel, unbound.id()), "rust_input_owner")
        .await
        .unwrap();
    assert!(era.is_empty(), "foreign pre-owner row must not be rebound");
    let another = first.register(&channel, &viewer(), "mpegts").await.unwrap();
    let era: String = conn
        .hget(row(&channel, another.id()), "rust_input_owner")
        .await
        .unwrap();
    assert_eq!(era, "owner1");
    owner(&mut conn, &channel, "owner2").await;
    assert_eq!(
        first.bind_local_owner(&channel, "owner2").await.unwrap(),
        0,
        "old era cannot be retagged"
    );
    let newer = second
        .register(&channel, &viewer(), "mpegts")
        .await
        .unwrap();
    assert_eq!(
        first
            .stop_abandoned_channel(&channel, "owner1")
            .await
            .unwrap(),
        0,
        "new owner presence protects every generation"
    );
    let _: () = conn
        .del(format!("live:channel:{channel}:owner"))
        .await
        .unwrap();
    assert_eq!(
        first
            .stop_abandoned_channel(&channel, "owner1")
            .await
            .unwrap(),
        2
    );
    assert!(*old.stopped().borrow());
    assert!(*another.stopped().borrow());
    assert!(!*unbound.stopped().borrow());
    assert!(!*newer.stopped().borrow());
    let cap: bool = conn
        .exists(format!("live:hls:session:{token}"))
        .await
        .unwrap();
    assert!(!cap);
    let count: usize = conn
        .scard(format!("live:channel:{channel}:clients"))
        .await
        .unwrap();
    assert_eq!(count, 2);
    unbound.release().await.unwrap();
    newer.release().await.unwrap();
    first.shutdown().await;
    second.shutdown().await;
}
#[tokio::test]
#[ignore = "requires isolated Redis"]
async fn queued_register_before_bind_reads_current_owner_without_missing_pending_client() {
    let _exclusive = EXCLUSIVE.lock().await;
    let store = store(Duration::from_secs(1)).await;
    let channel = channel();
    let mut conn = connection().await;
    owner(&mut conn, &channel, "owner").await;
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(300)
        .arg("ALL")
        .query_async(&mut conn)
        .await
        .unwrap();
    let pending = store.clone();
    let target = channel.clone();
    let registration =
        tokio::spawn(async move { pending.register(&target, &viewer(), "mpegts").await });
    time::sleep(Duration::from_millis(15)).await;
    let bind = store.bind_local_owner(&channel, "owner").await.unwrap();
    assert!(bind <= 1);
    let lease = registration.await.unwrap().unwrap();
    let era: String = conn
        .hget(row(&channel, lease.id()), "rust_input_owner")
        .await
        .unwrap();
    assert_eq!(era, "owner");
    lease.release().await.unwrap();
    store.shutdown().await;
}
#[tokio::test]
#[ignore = "requires isolated Redis"]
async fn malformed_owner_is_rejected_before_any_user_preemption() {
    let _exclusive = EXCLUSIVE.lock().await;
    let store = store(Duration::from_secs(1)).await;
    let old_channel = channel();
    let new_channel = channel();
    let mut conn = connection().await;
    let mut user = viewer();
    user.user_id = Some(918274);
    user.stream_limit = 1;
    let old = store.register(&old_channel, &user, "mpegts").await.unwrap();
    let _: usize = conn
        .rpush(format!("live:channel:{new_channel}:owner"), "invalid-list")
        .await
        .unwrap();
    assert!(matches!(
        store.register(&new_channel, &user, "mpegts").await,
        Err(Error::CorruptMetadata)
    ));
    assert!(!*old.stopped().borrow());
    let preempted: Option<String> = conn
        .hget(row(&old_channel, old.id()), "rust_preempted")
        .await
        .unwrap();
    assert!(preempted.is_none());
    old.release().await.unwrap();
    store.shutdown().await;
}
#[tokio::test]
#[ignore = "requires isolated Redis"]
async fn corrupt_target_capability_does_not_partially_remove_other_old_clients() {
    let _exclusive = EXCLUSIVE.lock().await;
    let store = store(Duration::from_secs(1)).await;
    let channel = channel();
    let mut conn = connection().await;
    owner(&mut conn, &channel, "owner").await;
    let first = store.register(&channel, &viewer(), "hls").await.unwrap();
    let second = store.register(&channel, &viewer(), "mpegts").await.unwrap();
    let token = "Era_HLS_Token_012345678901234567";
    first.bind_hls_token(token, None).await.unwrap();
    let _: () = conn
        .del(format!("live:channel:{channel}:owner"))
        .await
        .unwrap();
    let cap = format!("live:hls:session:{token}");
    let _: () = conn.del(&cap).await.unwrap();
    let _: usize = conn.rpush(&cap, "invalid-list").await.unwrap();
    assert_eq!(
        store.stop_abandoned_channel(&channel, "owner").await,
        Err(Error::RedisUnavailable)
    );
    let count: usize = conn
        .scard(format!("live:channel:{channel}:clients"))
        .await
        .unwrap();
    assert_eq!(count, 2);
    assert!(!*first.stopped().borrow());
    assert!(!*second.stopped().borrow());
    let _: () = conn.del(&cap).await.unwrap();
    first.bind_hls_token(token, None).await.unwrap();
    assert_eq!(
        store
            .stop_abandoned_channel(&channel, "owner")
            .await
            .unwrap(),
        2
    );
    store.shutdown().await;
}
#[tokio::test]
#[ignore = "requires isolated Redis"]
async fn abandoned_cleanup_finishes_after_caller_deadline_and_notifies_held_local_lease() {
    let _exclusive = EXCLUSIVE.lock().await;
    let store = store(Duration::from_millis(40)).await;
    let channel = channel();
    let mut conn = connection().await;
    owner(&mut conn, &channel, "owner").await;
    let held = store.register(&channel, &viewer(), "mpegts").await.unwrap();
    let _: () = conn
        .del(format!("live:channel:{channel}:owner"))
        .await
        .unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(300)
        .arg("ALL")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        store.stop_abandoned_channel(&channel, "owner").await,
        Err(Error::RedisUnavailable)
    );
    time::timeout(Duration::from_secs(2), async {
        while !*held.stopped().borrow() {
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let count: usize = conn
        .scard(format!("live:channel:{channel}:clients"))
        .await
        .unwrap();
    assert_eq!(count, 0);
    store.shutdown().await;
}

#[tokio::test]
#[ignore = "requires isolated Redis"]
async fn abandoned_metadata_cleanup_matches_old_fence_and_preflights_types() {
    let _exclusive = EXCLUSIVE.lock().await;
    let store = store(Duration::from_secs(1)).await;
    let channel = channel();
    let mut conn = connection().await;
    owner(&mut conn, &channel, "old").await;
    let held = store.register(&channel, &viewer(), "mpegts").await.unwrap();
    let metadata = format!("live:channel:{channel}:metadata");
    let heartbeat = "live:worker:era-test-worker:heartbeat";
    let _: () = conn.hset(&metadata, "rust_token", "new").await.unwrap();
    let _: () = conn.set(heartbeat, "alive").await.unwrap();
    let _: () = conn
        .del(format!("live:channel:{channel}:owner"))
        .await
        .unwrap();
    assert_eq!(
        store.stop_abandoned_channel(&channel, "old").await.unwrap(),
        1
    );
    let remains: bool = conn.exists(&metadata).await.unwrap();
    assert!(remains, "successor metadata is preserved");
    let _: () = conn.hset(&metadata, "rust_token", "old").await.unwrap();
    assert_eq!(
        store.stop_abandoned_channel(&channel, "old").await.unwrap(),
        0
    );
    let remains: bool = conn.exists(&metadata).await.unwrap();
    assert!(
        !remains,
        "old-owner metadata is removed even without old rows"
    );
    let alive: String = conn.get(heartbeat).await.unwrap();
    assert_eq!(alive, "alive");
    let _: () = conn.del(heartbeat).await.unwrap();
    drop(held);
    owner(&mut conn, &channel, "old").await;
    let lease = store.register(&channel, &viewer(), "mpegts").await.unwrap();
    let _: () = conn
        .del(format!("live:channel:{channel}:owner"))
        .await
        .unwrap();
    let _: usize = conn.rpush(&metadata, "wrong-type").await.unwrap();
    assert_eq!(
        store.stop_abandoned_channel(&channel, "old").await,
        Err(Error::RedisUnavailable)
    );
    let present: bool = conn.exists(row(&channel, lease.id())).await.unwrap();
    assert!(
        present,
        "corrupt metadata cannot cause partial client removal"
    );
    assert!(!*lease.stopped().borrow());
    let _: () = conn.del(&metadata).await.unwrap();
    lease.release().await.unwrap();
    store.shutdown().await;
}
