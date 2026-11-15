//! Actual raw Source last_data key semantics through the shared InputStore.
//! Original Dispatcharr authors retain protocol/application credit.
use dispatcharr_input_store::{Error, InputStore};
use std::time::{SystemTime, UNIX_EPOCH};
#[tokio::test]
#[ignore = "requires owned disposable real Redis"]
async fn partial_ingress_health_precedes_first_chunk_and_cannot_cross_owner_handover() {
    assert_eq!(
        std::env::var("M1_CORE_DISPOSABLE_FIXTURE").unwrap(),
        "owned-core-only"
    );
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut peer = redis::Client::open(url.clone())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let channel = format!(
        "source-health-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let owner_key = format!("live:channel:{channel}:owner");
    let key = format!("live:channel:{channel}:last_data");
    let index = format!("live:channel:{channel}:input:buffer:index");
    let store = InputStore::connect(&url, Default::default()).await.unwrap();
    assert_eq!(
        store
            .read_source_last_data_retained(&channel, ())
            .await
            .unwrap()
            .0,
        None
    );
    let _: () = redis::cmd("SET")
        .arg(&owner_key)
        .arg("owner-a")
        .query_async(&mut peer)
        .await
        .unwrap();
    store
        .put_source_last_data_retained(&channel, "owner-a", 1770000000.125, ())
        .await
        .unwrap();
    assert!(!redis::cmd("EXISTS")
        .arg(&index)
        .query_async::<bool>(&mut peer)
        .await
        .unwrap());
    assert_eq!(
        store
            .read_source_last_data_retained(&channel, ())
            .await
            .unwrap()
            .0,
        Some(1770000000.125)
    );
    let ttl: i64 = redis::cmd("TTL")
        .arg(&key)
        .query_async(&mut peer)
        .await
        .unwrap();
    assert!((59..=60).contains(&ttl));
    let _: () = redis::cmd("PEXPIRE")
        .arg(&key)
        .arg(1500)
        .query_async(&mut peer)
        .await
        .unwrap();
    store
        .read_source_last_data_retained(&channel, ())
        .await
        .unwrap();
    let ttl: i64 = redis::cmd("PTTL")
        .arg(&key)
        .query_async(&mut peer)
        .await
        .unwrap();
    assert!((0..=1500).contains(&ttl));
    let _: () = redis::cmd("SET")
        .arg(&owner_key)
        .arg("owner-b")
        .query_async(&mut peer)
        .await
        .unwrap();
    store
        .put_source_last_data_retained(&channel, "owner-b", 1770000002.0, ())
        .await
        .unwrap();
    assert_eq!(
        store
            .put_source_last_data_retained(&channel, "owner-a", 1770000003.0, ())
            .await,
        Err(Error::LostOwnership)
    );
    assert!(
        !store
            .clear_source_last_data_retained(&channel, "owner-a", ())
            .await
            .unwrap()
            .0
    );
    assert_eq!(
        store
            .read_source_last_data_retained(&channel, ())
            .await
            .unwrap()
            .0,
        Some(1770000002.0)
    );
    assert!(
        store
            .clear_source_last_data_retained(&channel, "owner-b", ())
            .await
            .unwrap()
            .0
    );
    assert_eq!(
        store
            .read_source_last_data_retained(&channel, ())
            .await
            .unwrap()
            .0,
        None
    );
    let _: () = redis::cmd("SET")
        .arg(&key)
        .arg("invalid")
        .query_async(&mut peer)
        .await
        .unwrap();
    assert_eq!(
        store.read_source_last_data_retained(&channel, ()).await,
        Err(Error::Corrupt)
    );
    let _: () = redis::cmd("DEL")
        .arg(&key)
        .arg(&owner_key)
        .query_async(&mut peer)
        .await
        .unwrap();
}
