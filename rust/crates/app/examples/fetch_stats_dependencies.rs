//! Guarded fixture bridge to the existing global scanner and ChannelsRedis bus.
//! Original Dispatcharr authors retain application/algorithm credit and AGPL.
use dispatcharr_clients::{ClientConfig, ClientStore};
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_realtime::{BusConfig, RealtimeBus};
use dispatcharr_stream_http::{EngineConfig, StreamEngine};
use serde_json::{json, Value};
use std::{
    io::{self, Read},
    sync::Arc,
    time::Duration,
};

#[tokio::main]
async fn main() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    assert!(std::env::var("POSTGRES_DB")
        .unwrap()
        .starts_with("rust_probe_backup_jobs_"));
    let mut raw = String::new();
    io::stdin()
        .take(4 * 1024 * 1024 + 1)
        .read_to_string(&mut raw)
        .unwrap();
    assert!(raw.len() <= 4 * 1024 * 1024);
    let input: Value = serde_json::from_str(&raw).unwrap();
    // Endpoint is fixed by the private Compose fixture, not stdin authority.
    let url = "redis://:playback-fixture-only@localhost:6379/0";
    let owner = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let output = match input["action"].as_str().unwrap() {
        "build" => {
            let clients = ClientStore::connect(url, ClientConfig::default())
                .await
                .unwrap();
            let engine = StreamEngine::new_with_services(
                EngineConfig::default(),
                None,
                Some(clients.clone()),
            )
            .unwrap();
            let result = engine.global_live_statistics_retained(owner).await;
            while weak.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            assert!(engine.shutdown_wait(Duration::from_secs(5)).await);
            clients.shutdown().await;
            match result {
                Ok(value) => json!({"ok":true,"value":value}),
                Err(_) => json!({"ok":false,"kind":"build"}),
            }
        }
        "send" => {
            let bus = RealtimeBus::connect(url, BusConfig::default())
                .await
                .unwrap();
            let result = bus
                .send_retained("updates", input["event"].clone(), owner)
                .await;
            while weak.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            match result {
                Ok(()) => json!({"ok":true}),
                Err(error) => json!({"ok":false,"kind":format!("{error:?}")}),
            }
        }
        _ => panic!("Unsupported guarded fixture command"),
    };
    println!("NATIVE_STATS_DEPENDENCY_JSON={output}");
}
