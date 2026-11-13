//! Unexecuted opt-in Native24 real-router peer. No Source application/Main.
//! Snapshot/seed/import/controller code supplies separately sealed context.
use super::*;
use crate::{AuthConfig, NotificationCache, Store};
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
fn save(path: &std::path::Path, value: &Value) {
    let raw = serde_json::to_vec_pretty(value).unwrap();
    assert!(raw.len() <= 65536);
    std::fs::write(path, raw).unwrap();
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires sealed owned Native24 storage/controller; no default app work"]
async fn actual_api_key_repository_metadata_twenty_four_peer() {
    assert_eq!(
        std::env::var("PLUGIN_REPO_NATIVE").as_deref(),
        Ok("owned-repository-metadata-only")
    );
    let database = std::env::var("POSTGRES_DB").unwrap();
    let tail = database.strip_prefix("rust_probe_pluginrepo_").unwrap();
    assert_eq!(tail.len(), 10);
    assert!(tail.bytes().all(|b| b.is_ascii_hexdigit()));
    let url = std::env::var("DATABASE_TEST_URL").unwrap();
    assert_eq!(
        reqwest::Url::parse(&url).unwrap().path(),
        format!("/{database}")
    );
    let redis = std::env::var("REDIS_TEST_URL").unwrap();
    assert!(redis.ends_with("/1"));
    let root = PathBuf::from(std::env::var("PLUGIN_REPO_NATIVE_INPUTS").unwrap());
    let output = PathBuf::from(std::env::var("PLUGIN_REPO_NATIVE_OUTPUT").unwrap());
    assert!(output.is_dir());
    let source = root.join("Source24-private.json");
    let raw = std::fs::read(&source).unwrap();
    assert!(raw.len() <= 67108864);
    assert_eq!(
        format!("{:x}", Sha256::digest(&raw)),
        std::env::var("PLUGIN_REPO_SOURCE_SHA256").unwrap()
    );
    let reference: Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(reference["actual_request_count"], 24);
    assert_eq!(reference["Source_framework_APIClient_only"], true);
    assert_eq!(reference["force_authenticate_used"], false);
    let store = Store::connect(&url, 4).await.unwrap();
    let cache = NotificationCache::connect(&redis).await.unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("owned-native-pluginrepo-fixture-signing-only".into()).unwrap(),
    )
    .with_notification_cache(cache.clone());
    let settings = reference["seed"]["settings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["key"] == "system_settings")
        .unwrap()["value"]
        .clone();
    // Declared imported model-save lifecycle, using the REAL existing setter.
    // The separately sealed seed/import helper owns exact rows + five Redis keys.
    let setter_began = Utc::now();
    auth.set_display_timezone_on_settings_save("system_settings", &settings);
    let setter_completed = Utc::now();
    let workers = auth.user_agent_workers.clone();
    let counter = Arc::new(AtomicUsize::new(0));
    let observed = counter.clone();
    let app = PluginRepositories::new(auth.clone())
        .router()
        .layer(axum::middleware::from_fn(
            move |request: Request, next: axum::middleware::Next| {
                let observed = observed.clone();
                async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                    next.run(request).await
                }
            },
        ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let _ = stop_rx.await;
        })
        .await
    });
    save(
        &output.join("peer-ready.json"),
        &json!({"address":address.to_string(),"database":database,"actual_adapter":"PluginRepositories::router","Main":false,"cfgtest_business_substitution":false,"four_writer_permits_initially_available":workers.available_permits(),"Source_authority_sha256":format!("{:x}",Sha256::digest(&raw)),"imported_model_save_setter":{"began_utc":setter_began.to_rfc3339(),"completed_utc":setter_completed.to_rfc3339(),"actual_zone":auth.display_timezone_name(),"Native_post_save_signal_claim":false},"source_cache_import_context":"only sealed initial five keys, no warmed groups/history reset"}),
    );
    let stopping = output.join("peer-stop");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(240);
    while !stopping.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "Peer240sStopDeadline"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    stop_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        counter.load(Ordering::SeqCst),
        24,
        "Exactly24BusinessRequestsNoHTTPReadiness"
    );
    // Listener is stopped before admission drain. These are the actual existing
    // four slots; no extra permit or caller owner is created for business work.
    let drained = tokio::time::timeout(
        Duration::from_secs(15),
        workers.clone().acquire_many_owned(4),
    )
    .await
    .unwrap()
    .unwrap();
    drop(drained);
    assert_eq!(workers.available_permits(), 4);
    store.pool().close().await;
    save(
        &output.join("peer-terminal.json"),
        &json!({"passed":true,"actual_requests":counter.load(Ordering::SeqCst),"actual_server_wait_exit0":true,"listener_stopped_before_four_slot_drain":true,"same_four_slots_available_after_drain":workers.available_permits(),"pool_is_closed":store.pool().is_closed(),"pool_size_after_close":store.pool().size(),"pool_num_idle_after_close_diagnostic":store.pool().num_idle(),"max_pool_connections":4,"while_ELF_alive_independent_backend_observation":"UNOBSERVED","Main_or_full_M8":false}),
    );
}

// Fresh Source2 rendered-error peer; historical Native24 peer above is unchanged.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires sealed owned Native2 rendered-error storage/controller; no default app work"]
async fn actual_api_key_repository_rendered_two_peer() {
    assert_eq!(
        std::env::var("PLUGIN_REPO_NATIVE").as_deref(),
        Ok("owned-repository-rendered-only")
    );
    let database = std::env::var("POSTGRES_DB").unwrap();
    let tail = database.strip_prefix("rust_probe_pluginrepo_").unwrap();
    assert_eq!(tail.len(), 10);
    assert!(tail.bytes().all(|b| b.is_ascii_hexdigit()));
    let url = std::env::var("DATABASE_TEST_URL").unwrap();
    assert_eq!(
        reqwest::Url::parse(&url).unwrap().path(),
        format!("/{database}")
    );
    let redis = std::env::var("REDIS_TEST_URL").unwrap();
    assert!(redis.ends_with("/1"));
    let root = PathBuf::from(std::env::var("PLUGIN_REPO_NATIVE_INPUTS").unwrap());
    let output = PathBuf::from(std::env::var("PLUGIN_REPO_NATIVE_OUTPUT").unwrap());
    assert!(output.is_dir());
    let source = root.join("Source2-private.json");
    let raw = std::fs::read(&source).unwrap();
    assert!(raw.len() <= 67108864);
    assert_eq!(
        format!("{:x}", Sha256::digest(&raw)),
        std::env::var("PLUGIN_REPO_SOURCE_SHA256").unwrap()
    );
    let reference: Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(reference["actual_request_count"], 2);
    assert_eq!(reference["Source_framework_APIClient_only"], true);
    assert_eq!(reference["force_authenticate_used"], false);
    assert_eq!(reference["client_raise_request_exception"], false);
    let store = Store::connect(&url, 4).await.unwrap();
    let cache = NotificationCache::connect(&redis).await.unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("owned-native-pluginrepo-rendered-signing-only".into()).unwrap(),
    )
    .with_notification_cache(cache.clone());
    let settings = reference["seed"]["settings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["key"] == "system_settings")
        .unwrap()["value"]
        .clone();
    // Declared imported model-save lifecycle, using the REAL existing setter.
    // The separately sealed seed/import helper owns exact rows + five Redis keys.
    let setter_began = Utc::now();
    auth.set_display_timezone_on_settings_save("system_settings", &settings);
    let setter_completed = Utc::now();
    let workers = auth.user_agent_workers.clone();
    let counter = Arc::new(AtomicUsize::new(0));
    let observed = counter.clone();
    let app = PluginRepositories::new(auth.clone())
        .router()
        .layer(axum::middleware::from_fn(
            move |request: Request, next: axum::middleware::Next| {
                let observed = observed.clone();
                async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                    next.run(request).await
                }
            },
        ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let _ = stop_rx.await;
        })
        .await
    });
    save(
        &output.join("peer-ready.json"),
        &json!({"address":address.to_string(),"database":database,"actual_adapter":"PluginRepositories::router","Main":false,"cfgtest_business_substitution":false,"four_writer_permits_initially_available":workers.available_permits(),"Source_authority_sha256":format!("{:x}",Sha256::digest(&raw)),"imported_model_save_setter":{"began_utc":setter_began.to_rfc3339(),"completed_utc":setter_completed.to_rfc3339(),"actual_zone":auth.display_timezone_name(),"Native_post_save_signal_claim":false},"source_cache_import_context":"only sealed initial five keys, no warmed groups/history reset"}),
    );
    let stopping = output.join("peer-stop");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(240);
    while !stopping.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "Peer240sStopDeadline"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    stop_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        counter.load(Ordering::SeqCst),
        2,
        "Exactly2BusinessRequestsNoHTTPReadiness"
    );
    // Listener is stopped before admission drain. These are the actual existing
    // four slots; no extra permit or caller owner is created for business work.
    let drained = tokio::time::timeout(
        Duration::from_secs(15),
        workers.clone().acquire_many_owned(4),
    )
    .await
    .unwrap()
    .unwrap();
    drop(drained);
    assert_eq!(workers.available_permits(), 4);
    store.pool().close().await;
    save(
        &output.join("peer-terminal.json"),
        &json!({"passed":true,"actual_requests":counter.load(Ordering::SeqCst),"actual_server_wait_exit0":true,"listener_stopped_before_four_slot_drain":true,"same_four_slots_available_after_drain":workers.available_permits(),"pool_is_closed":store.pool().is_closed(),"pool_size_after_close":store.pool().size(),"pool_num_idle_after_close_diagnostic":store.pool().num_idle(),"max_pool_connections":4,"while_ELF_alive_independent_backend_observation":"UNOBSERVED","Main_or_full_M8":false}),
    );
}
