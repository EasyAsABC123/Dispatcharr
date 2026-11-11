//! Actual Source13 expiry trace vs native real Redis; Original authors retain credit.
use dispatcharr_input_store::{InputConfig, InputStore};
use serde_json::{json, Value};
use std::time::Duration;
fn decode(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}
#[tokio::test]
#[ignore = "requires owned disposable real Redis"]
async fn original_same_producer_after_ttl_has_no_new_discontinuity() {
    assert_eq!(
        std::env::var("M1_CORE_DISPOSABLE_FIXTURE").unwrap(),
        "owned-core-only"
    );
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut redis = redis::Client::open(url.clone())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let channel = format!("m1-source-expiry-{}", std::process::id());
    let owner = "owned-core-unchanged-producer";
    let _: () = redis::cmd("SET")
        .arg(format!("live:channel:{channel}:owner"))
        .arg(owner)
        .query_async(&mut redis)
        .await
        .unwrap();
    let config = InputConfig {
        max_chunk_bytes: 376,
        max_channel_bytes: 16 * 376,
        max_chunks: 16,
        chunk_ttl: Duration::from_secs(2),
        preserve_source_era: true,
        ..Default::default()
    };
    let store = InputStore::connect(&url, config).await.unwrap();
    let source: Value = serde_json::from_str(include_str!(
        "../../stream-http/datasets/source-core-mark-3.json"
    ))
    .unwrap();
    let before = source["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["label"] == "second_mark_flushes_previously_stamped_pending_PID")
        .unwrap();
    for row in before["chunks"].as_array().unwrap() {
        let seq = row["sequence"].as_u64().unwrap();
        let data = decode(row["hex"].as_str().unwrap());
        assert_eq!(
            store
                .put(&channel, owner, &data, seq as f64, seq == 3)
                .await
                .unwrap(),
            seq
        );
    }
    tokio::time::sleep(Duration::from_millis(2200)).await;
    let expired = store.snapshot(&channel).await.unwrap();
    assert!(expired.chunks.is_empty());
    let native_expired_index = expired.index;
    drop(expired);
    let after = source["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["label"] == "fresh_after_expiry")
        .unwrap();
    let data = decode(after["chunks"][0]["hex"].as_str().unwrap());
    assert_eq!(
        store.put(&channel, owner, &data, 5.0, false).await.unwrap(),
        5
    );
    let bytes = store.get(&channel, 5).await.unwrap().unwrap();
    assert_eq!(bytes.as_ref(), data);
    let native = bytes.discontinuity;
    drop(bytes);
    let original = after["discontinuities"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v == 5);
    println!(
        "M1_SOURCE_EXPIRY_JSON={}",
        json!({"full_bytes_equal":true,"Original_expired_index":4,"native_expired_snapshot_index":native_expired_index,"sequence":5,"coordinator_owner_unchanged":true,"Original_marked":original,"native_marked":native})
    );
    assert!(store.cleanup(&channel, owner).await.unwrap());
    let _: () = redis::cmd("DEL")
        .arg(format!("live:channel:{channel}:owner"))
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(
        native, original,
        "Source same-producer expiry does not introduce a new discontinuity"
    );
}

#[tokio::test]
#[ignore = "requires owned disposable real Redis"]
async fn source_expiry_receipt_never_adopts_foreign_missing_or_corrupt_era() {
    use dispatcharr_input_store::Error;
    assert_eq!(
        std::env::var("M1_CORE_DISPOSABLE_FIXTURE").unwrap(),
        "owned-core-only"
    );
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut redis = redis::Client::open(url.clone())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    for mode in [
        "foreign",
        "missing",
        "json-bad",
        "oversized",
        "catalog-mismatch",
        "index-mismatch",
    ] {
        let channel = format!("m1-receipt-{}-{mode}", std::process::id());
        let key = format!("live:channel:{channel}:owner");
        let fence = format!("live:channel:{channel}:input:buffer:rust_gc_fence");
        let _: () = redis::cmd("SET")
            .arg(&key)
            .arg("old")
            .query_async(&mut redis)
            .await
            .unwrap();
        let store = InputStore::connect(
            &url,
            InputConfig {
                preserve_source_era: true,
                chunk_ttl: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let mut data = [7u8; 188];
        data[0] = 0x47;
        assert_eq!(
            store.put(&channel, "old", &data, 1.0, false).await.unwrap(),
            1
        );
        tokio::time::sleep(Duration::from_millis(75)).await;
        let owner = if mode == "foreign" { "new" } else { "old" };
        if mode == "foreign" {
            let _: () = redis::cmd("SET")
                .arg(&key)
                .arg(owner)
                .query_async(&mut redis)
                .await
                .unwrap();
        }
        if mode == "missing" {
            let _: () = redis::cmd("DEL")
                .arg(&fence)
                .query_async(&mut redis)
                .await
                .unwrap();
        }
        if mode == "json-bad" || mode == "oversized" {
            let value = if mode == "json-bad" {
                "broken".to_owned()
            } else {
                "x".repeat(1025)
            };
            let _: () = redis::cmd("SET")
                .arg(&fence)
                .arg(value)
                .query_async(&mut redis)
                .await
                .unwrap();
        }
        if mode == "catalog-mismatch" {
            let _: () = redis::cmd("HSET")
                .arg("dispatcharr:rust:input:gc:catalog")
                .arg(&channel)
                .arg("other")
                .query_async(&mut redis)
                .await
                .unwrap();
        }
        if mode == "index-mismatch" {
            let value = json!({"owner":"old","index":"999"}).to_string();
            let _: () = redis::cmd("SET")
                .arg(&fence)
                .arg(&value)
                .query_async(&mut redis)
                .await
                .unwrap();
            let _: () = redis::cmd("HSET")
                .arg("dispatcharr:rust:input:gc:catalog")
                .arg(&channel)
                .arg(value)
                .query_async(&mut redis)
                .await
                .unwrap();
        }
        let result = store.put(&channel, owner, &data, 2.0, false).await;
        if matches!(mode, "foreign" | "missing") {
            assert_eq!(result.unwrap(), 2);
            assert!(store.get(&channel, 2).await.unwrap().unwrap().discontinuity);
            assert!(store.cleanup(&channel, owner).await.unwrap());
        } else {
            assert_eq!(result, Err(Error::Corrupt));
            let index: String = redis::cmd("GET")
                .arg(format!("live:channel:{channel}:input:buffer:index"))
                .query_async(&mut redis)
                .await
                .unwrap();
            assert_eq!(index, "1");
        }
        // Fixture cleanup is bounded to the exact owned channel and its catalog row.
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(format!("live:channel:{channel}:*"))
            .query_async(&mut redis)
            .await
            .unwrap();
        assert!(keys.len() <= 16);
        if !keys.is_empty() {
            let _: () = redis::cmd("DEL")
                .arg(keys)
                .query_async(&mut redis)
                .await
                .unwrap();
        }
        let _: () = redis::cmd("HDEL")
            .arg("dispatcharr:rust:input:gc:catalog")
            .arg(&channel)
            .query_async(&mut redis)
            .await
            .unwrap();
        let _: () = redis::cmd("ZREM")
            .arg("dispatcharr:rust:input:gc:due")
            .arg(&channel)
            .query_async(&mut redis)
            .await
            .unwrap();
    }
}
