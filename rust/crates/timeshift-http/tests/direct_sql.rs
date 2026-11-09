//! Native direct-auth wire proof on actual original Django PostgreSQL/Redis.
//! All original application credit belongs to Dispatcharr contributors.
use dispatcharr_contracts::{BoxFuture, UpdateEventError, UpdateEventSink};
use dispatcharr_coordination::{Coordinator, LeaseConfig};
use dispatcharr_store::{AuthConfig, AuthService, NotificationCache, Store};
use dispatcharr_timeshift_http::{
    CatchupManagement, CatchupMedia, CatchupMediaConfig, CatchupSessions, SessionConfig,
};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct FailedEventSink(Mutex<Vec<serde_json::Value>>);
impl UpdateEventSink for FailedEventSink {
    fn send<'a>(
        &'a self,
        group: &'a str,
        message: serde_json::Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            assert_eq!(group, "updates");
            assert_eq!(message["type"], "update");
            assert_eq!(message["data"]["type"], "timeshift_stats");
            assert_eq!(message["data"]["success"], true);
            let stats: serde_json::Value =
                serde_json::from_str(message["data"]["stats"].as_str().unwrap()).unwrap();
            assert_eq!(stats["total_connections"], 0);
            assert_eq!(stats["timeshift_sessions"], serde_json::json!([]));
            self.0.lock().unwrap().push(message);
            // Like Source best-effort background dispatch, sink failure must
            // not turn a successfully applied administrative stop into500.
            Err(UpdateEventError::Unavailable)
        })
    }
}

#[tokio::test]
#[ignore = "requires disposable original Django SQL/Redis fixture and driver"]
async fn actual_store_auth_native_timeshift_entrypoints() {
    assert_eq!(
        std::env::var("DIRECT_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let database = std::env::var("DATABASE_TEST_URL").unwrap();
    assert!(database.contains("playback-fixture-only@"));
    let redis = std::env::var("REDIS_TEST_URL").unwrap();
    let store = Store::connect(&database, 16).await.unwrap();
    assert!(store
        .user_by_username("rust-catchup-owner")
        .await
        .unwrap()
        .is_some());
    let original_server: String = sqlx::query_scalar(
        "SELECT server_url FROM m3u_m3uaccount WHERE name='Rust VOD fixture XC'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    let physical_origin = std::env::var("TIMESHIFT_DIRECT_PHYSICAL_ORIGIN").unwrap();
    sqlx::query("UPDATE m3u_m3uaccount SET server_url=$1 WHERE name='Rust VOD fixture XC'")
        .bind(format!("{physical_origin}/player_api.php"))
        .execute(store.pool())
        .await
        .unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    );
    let auth = auth.with_notification_cache(NotificationCache::connect(&redis).await.unwrap());
    let sessions = CatchupSessions::new(
        store.clone(),
        auth.clone(),
        &redis,
        SessionConfig::default(),
    )
    .await
    .unwrap();
    let coordinator = Coordinator::connect(
        &redis,
        LeaseConfig {
            namespace: format!("test:native-direct:{}", uuid::Uuid::new_v4()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let engine = CatchupMedia::new(
        store.clone(),
        auth.clone(),
        sessions.clone(),
        coordinator,
        &redis,
        CatchupMediaConfig::default(),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let event_sink = Arc::new(FailedEventSink::default());
    let management = if std::env::var("TIMESHIFT_NATIVE_KIND").as_deref() == Ok("management") {
        Some(
            CatchupManagement::new(store.clone(), auth, engine.clone(), &redis)
                .await
                .unwrap()
                .with_events(event_sink.clone()),
        )
    } else {
        None
    };
    let mut router = engine.clone().router().merge(sessions.router());
    if let Some(management) = &management {
        router = router.merge(management.clone().router());
    }
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let mut command = tokio::process::Command::new("python");
    command
        .arg(std::env::var("TIMESHIFT_DIRECT_DRIVER").unwrap())
        .env("TIMESHIFT_DIRECT_BASE", base)
        .env(
            "TIMESHIFT_DIRECT_LOGICAL_ORIGIN",
            original_server.trim_end_matches("/player_api.php"),
        )
        .kill_on_drop(true);
    let result = tokio::time::timeout(std::time::Duration::from_secs(120), command.output())
        .await
        .expect("bounded direct-auth driver")
        .unwrap();
    let shutdown_management = if let Some(management) = management {
        management.shutdown().await
    } else {
        true
    };
    let shutdown = engine.shutdown().await && shutdown_management;
    server.abort();
    sqlx::query("UPDATE m3u_m3uaccount SET server_url=$1 WHERE name='Rust VOD fixture XC'")
        .bind(original_server)
        .execute(store.pool())
        .await
        .unwrap();
    if std::env::var("TIMESHIFT_NATIVE_KIND").as_deref() == Ok("management")
        && result.status.success()
    {
        let expected = if std::env::var("TIMESHIFT_MANAGEMENT_ACTIVE").as_deref() == Ok("1") {
            3
        } else {
            1
        };
        assert_eq!(
            event_sink.0.lock().unwrap().len(),
            expected,
            "one event attempt per successful stop, even when the sink fails"
        );
    }
    assert!(
        shutdown,
        "all direct-auth reservations must drain at shutdown"
    );
    assert!(
        result.status.success(),
        "driver failed: {} {}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(
        serde_json::from_slice::<serde_json::Value>(&result.stdout).unwrap()
            ["native_direct_capture_complete"]
            .as_bool()
            .unwrap()
    );
}
