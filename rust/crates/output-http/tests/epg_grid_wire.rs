//! Actual original-network versus independent native-network guide gate.
//! Unexecuted draft until the owned real-clock original fixture is captured.
//! All application credit belongs to original Dispatcharr contributors.
use dispatcharr_output_http::{GridGateway, GridReadLimits};
use dispatcharr_store::{AuthConfig, AuthService, NotificationCache, Store};
use std::{net::SocketAddr, time::Duration};
#[tokio::test]
#[ignore = "requires disposable guide real-clock Source-network fixture"]
async fn actual_original_and_native_network_grid_match_without_clock_patch() {
    assert_eq!(
        std::env::var("EPG_READ_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 8)
        .await
        .unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(
        NotificationCache::connect(&std::env::var("REDIS_TEST_URL").unwrap())
            .await
            .unwrap(),
    );
    // Uses actual Utc::now, not the pinned Factory clock helper.
    let gateway = GridGateway::new(store, auth, GridReadLimits::default()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = gateway.clone().router();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let mut command = tokio::process::Command::new("python");
    command
        .arg(std::env::var("GRID_WIRE_DRIVER").unwrap())
        .arg("--base")
        .arg(base)
        .arg("--fixture")
        .arg(std::env::var("GUIDE_WIRE_FIXTURE").unwrap())
        .kill_on_drop(true);
    let result = tokio::time::timeout(Duration::from_secs(120), command.output())
        .await
        .unwrap()
        .unwrap();
    assert!(gateway.shutdown(Duration::from_secs(2)).await);
    server.abort();
    println!("{}", String::from_utf8_lossy(&result.stdout));
    assert!(
        result.status.success(),
        "network guide mismatch: {}",
        String::from_utf8_lossy(&result.stderr)
    );
}
