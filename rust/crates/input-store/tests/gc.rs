//! Real Redis durable cleanup; never run against production Redis.
//! Original Dispatcharr authors retain full attribution.
use dispatcharr_input_store::{Error, InputConfig, InputStore};
use redis::AsyncCommands;
use std::time::Duration;
const CATALOG: &str = "dispatcharr:rust:input:gc:catalog";
const DUE: &str = "dispatcharr:rust:input:gc:due";
async fn fixture() -> redis::aio::MultiplexedConnection {
    let mut connection = redis::Client::open(
        std::env::var("REDIS_TEST_URL").expect("dedicated disposable Redis required"),
    )
    .unwrap()
    .get_multiplexed_async_connection()
    .await
    .unwrap();
    let _: () = redis::cmd("FLUSHDB")
        .query_async(&mut connection)
        .await
        .unwrap();
    connection
}
async fn store(config: InputConfig) -> InputStore {
    InputStore::connect(&std::env::var("REDIS_TEST_URL").unwrap(), config)
        .await
        .unwrap()
}
fn config() -> InputConfig {
    InputConfig {
        chunk_ttl: Duration::from_millis(100),
        ..Default::default()
    }
}
async fn put(
    store: &InputStore,
    conn: &mut redis::aio::MultiplexedConnection,
    ch: &str,
    owner: &str,
) {
    let _: () = conn
        .set(format!("live:channel:{ch}:owner"), owner)
        .await
        .unwrap();
    store
        .put(ch, owner, &[0x47; 188], 1.0, false)
        .await
        .unwrap();
}
async fn due(conn: &mut redis::aio::MultiplexedConnection, ch: &str) {
    let _: () = conn.zadd(DUE, ch, 0).await.unwrap();
}
#[tokio::test]
#[ignore = "requires isolated real Redis; --test-threads=1; FLUSHDB"]
async fn restart_after_all_workers_and_fences_expire_reclaims_only_known_rust_index() {
    let mut conn = fixture().await;
    let first = store(config()).await;
    put(&first, &mut conn, "crashed", "old").await;
    let _: () = conn.del("live:channel:crashed:owner").await.unwrap();
    drop(first);
    let _: () = conn
        .set("live:channel:python:input:buffer:index", 17)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2200)).await;
    let restarted = store(config()).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    while conn
        .exists::<_, bool>("live:channel:crashed:input:buffer:index")
        .await
        .unwrap()
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "automatic restart reaper did not reclaim known orphan"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(restarted.reap_expired(32).await.unwrap(), 0);
    assert!(!conn
        .exists::<_, bool>("live:channel:crashed:input:buffer:index")
        .await
        .unwrap());
    assert_eq!(
        conn.get::<_, i64>("live:channel:python:input:buffer:index")
            .await
            .unwrap(),
        17
    );
    assert_eq!(conn.hlen::<_, usize>(CATALOG).await.unwrap(), 0);
    assert_eq!(conn.zcard::<_, usize>(DUE).await.unwrap(), 0);
}
#[tokio::test]
#[ignore = "requires isolated real Redis; --test-threads=1; FLUSHDB"]
async fn successors_and_changed_legacy_indexes_are_preserved() {
    let mut conn = fixture().await;
    let input = store(config()).await;
    for ch in ["successor", "changed", "published"] {
        put(&input, &mut conn, ch, "old").await;
        let _: () = conn.del(format!("live:channel:{ch}:owner")).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    let _: () = conn
        .set("live:channel:successor:owner", "new-before-first-publish")
        .await
        .unwrap();
    let _: () = conn
        .set("live:channel:changed:input:buffer:index", 77)
        .await
        .unwrap();
    put(&input, &mut conn, "published", "new-published").await;
    for ch in ["successor", "changed", "published"] {
        due(&mut conn, ch).await;
    }
    assert_eq!(input.reap_expired(32).await.unwrap(), 0);
    assert_eq!(
        conn.get::<_, String>("live:channel:successor:owner")
            .await
            .unwrap(),
        "new-before-first-publish"
    );
    assert_eq!(
        conn.get::<_, i64>("live:channel:changed:input:buffer:index")
            .await
            .unwrap(),
        77
    );
    assert_eq!(
        conn.get::<_, String>("live:channel:published:input:buffer:rust_owner")
            .await
            .unwrap(),
        "new-published"
    );
    assert!(!conn
        .hexists::<_, _, bool>(CATALOG, "changed")
        .await
        .unwrap());
    assert!(!conn
        .exists::<_, bool>("live:channel:changed:input:buffer:rust_gc_fence")
        .await
        .unwrap());
}
#[tokio::test]
#[ignore = "requires isolated real Redis; --test-threads=1; FLUSHDB"]
async fn catalog_admission_is_atomic_bounded_and_normal_cleanup_releases_it() {
    let mut conn = fixture().await;
    let input = store(InputConfig {
        max_catalog_entries: 2,
        ..config()
    })
    .await;
    put(&input, &mut conn, "one", "a").await;
    put(&input, &mut conn, "two", "b").await;
    let _: () = conn.set("live:channel:three:owner", "c").await.unwrap();
    assert_eq!(
        input
            .put("three", "c", &[0x47; 188], 1.0, false)
            .await
            .unwrap_err(),
        Error::Capacity
    );
    assert!(!conn
        .exists::<_, bool>("live:channel:three:input:buffer:index")
        .await
        .unwrap());
    assert_eq!(conn.hlen::<_, usize>(CATALOG).await.unwrap(), 2);
    assert!(input.cleanup("one", "a").await.unwrap());
    assert_eq!(conn.hlen::<_, usize>(CATALOG).await.unwrap(), 1);
    input
        .put("three", "c", &[0x47; 188], 1.0, false)
        .await
        .unwrap();
    assert_eq!(conn.hlen::<_, usize>(CATALOG).await.unwrap(), 2);
}
#[tokio::test]
#[ignore = "requires isolated real Redis; --test-threads=1; FLUSHDB"]
async fn cancelled_reaper_holds_operation_until_actual_redis_reply() {
    let mut conn = fixture().await;
    let input = store(InputConfig {
        max_operations: 1,
        max_queued_operations: 1,
        command_timeout: Duration::from_millis(150),
        ..config()
    })
    .await;
    input.reap_expired(1).await.unwrap(); // prime Lua cache
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(700)
        .arg("ALL")
        .query_async(&mut conn)
        .await
        .unwrap();
    let first = tokio::spawn({
        let input = input.clone();
        async move { input.reap_expired(1).await }
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    first.abort();
    let _ = first.await;
    let second = tokio::spawn({
        let input = input.clone();
        async move { input.reap_expired(1).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(input.reap_expired(1).await.unwrap_err(), Error::Capacity);
    assert_eq!(second.await.unwrap().unwrap_err(), Error::Unavailable);
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(input.reap_expired(1).await.unwrap(), 0);
}
