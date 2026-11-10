//! Actual Original Django DVR-cache interoperability for the SeriesRules subset.
//! Original Dispatcharr authors retain application/algorithm credit.
use dispatcharr_ingest_epg::{cache_policy::CachedSwapPolicy, SwapPolicyContext};
use dispatcharr_store::Store;
use serde_json::Value;
use std::{
    io::{self, Read},
    sync::{atomic::AtomicBool, Arc},
};
#[tokio::test]
#[ignore = "Owned copied database and isolated Source DVR cache only"]
async fn native_series_dvr_policy_peer() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let database = std::env::var("POSTGRES_DB").unwrap();
    assert!(
        database.starts_with("rust_probe_backup_jobs_")
            && database.len() <= 63
            && database
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    );
    let mut raw = String::new();
    io::stdin().take(65537).read_to_string(&mut raw).unwrap();
    assert!(raw.len() <= 65536);
    let input: Value = serde_json::from_str(&raw).unwrap();
    let url = format!("postgres://dispatch:playback-fixture-only@127.0.0.1:5432/{database}");
    assert_eq!(input["database_url"], url);
    let cache = input["cache_url"].as_str().unwrap();
    assert_eq!(cache, "redis://:playback-fixture-only@127.0.0.1:6379/0");
    let store = Store::connect(&url, 1).await.unwrap();
    let policy = CachedSwapPolicy::new(store.clone(), Some(cache.into()));
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let value = policy
        .retained_dvr_settings(SwapPolicyContext {
            cancelled: Arc::new(AtomicBool::new(false)),
            retained: caller,
        })
        .await
        .unwrap();
    assert_eq!(value, input["expected"]);
    assert_eq!(policy.physical_operations(), 0);
    assert!(weak.upgrade().is_none());
    store.pool().close().await;
    println!(
        "NATIVE_SERIES_POLICY_JSON={}",
        serde_json::json!({"passed":true,"value":value,"caller_released":true,"physical_operations":0})
    );
}
