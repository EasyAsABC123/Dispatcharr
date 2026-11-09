//! Exact retained-byte diagnostics against isolated Redis; original author credit retained.
use dispatcharr_input_store::{InputConfig, InputStore};
use std::time::{SystemTime, UNIX_EPOCH};
#[tokio::test]
#[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
async fn retained_diagnostics_are_exact_readonly_and_report_stale_owner_fences() {
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let store = InputStore::connect(&url, InputConfig::default())
        .await
        .unwrap();
    let mut redis = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let id = format!(
        "retained-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let prefix = format!("live:channel:{id}");
    let _: () = redis::cmd("SET")
        .arg(format!("{prefix}:owner"))
        .arg("old")
        .query_async(&mut redis)
        .await
        .unwrap();
    store
        .put(&id, "old", &[0x47; 376], 1.0, false)
        .await
        .unwrap();
    store
        .put(&id, "old", &[0x47; 188], 2.0, true)
        .await
        .unwrap();
    let key = format!("{prefix}:input:buffer:chunk:2");
    let before: i64 = redis::cmd("PTTL")
        .arg(&key)
        .query_async(&mut redis)
        .await
        .unwrap();
    let info = store.retained_diagnostics(&id).await.unwrap();
    assert_eq!(info["tracked_chunks"], 2);
    assert_eq!(info["retained_chunks"], 2);
    assert_eq!(info["logical_payload_bytes"], 564);
    assert_eq!(info["fence_matches_owner"], true);
    assert!(info["redis_known_key_allocation_bytes"].as_u64().unwrap() > 564);
    let after: i64 = redis::cmd("PTTL")
        .arg(&key)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(
        after <= before,
        "read-only diagnostics must not refresh TTL"
    );
    let _: () = redis::cmd("DEL")
        .arg(format!("{prefix}:input:buffer:chunk:1"))
        .query_async(&mut redis)
        .await
        .unwrap();
    let info = store.retained_diagnostics(&id).await.unwrap();
    assert_eq!(info["tracked_chunks"], 2);
    assert_eq!(info["retained_chunks"], 1);
    assert_eq!(info["logical_payload_bytes"], 188);
    let _: () = redis::cmd("SET")
        .arg(format!("{prefix}:owner"))
        .arg("successor")
        .query_async(&mut redis)
        .await
        .unwrap();
    let info = store.retained_diagnostics(&id).await.unwrap();
    assert_eq!(info["current_owner"], "successor");
    assert_eq!(info["published_owner"], "old");
    assert_eq!(info["fence_matches_owner"], false);
    assert_eq!(info["logical_payload_bytes"], 188);
    let _: () = redis::cmd("SET")
        .arg(format!("{prefix}:owner"))
        .arg("old")
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(store.cleanup(&id, "old").await.unwrap());
}
