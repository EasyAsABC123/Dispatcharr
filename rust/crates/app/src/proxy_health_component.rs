//! Same-Core Engine component test. All application credit remains with the original authors.
use crate::proxy_health::CoreProxyHealthSource;
use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{header, Method, Request, StatusCode},
    Router,
};
use dispatcharr_realtime::{BusConfig, RealtimeBus};
use dispatcharr_store::{
    AuthConfig, AuthService, CoreSettingsSpecialEffects, CoreSettingsWrites, NotificationCache,
    Store,
};
use dispatcharr_stream_http::{EngineConfig, StreamEngine};
use serde_json::{json, Value};
use std::{env, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tower::ServiceExt;

fn setting_request(method: Method, row: u64, bearer: &str, value: Option<Value>) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(format!("/api/core/settings/{row}/"))
        .header(header::AUTHORIZATION, format!("Bearer {bearer}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(value.map_or_else(Body::empty, |value| Body::from(value.to_string())))
        .unwrap();
    request
        .extensions_mut()
        .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 2], 12345))));
    request
}

async fn update_proxy(router: &Router, row: u64, bearer: &str, initialization: u64) {
    let response = router
        .clone()
        .oneshot(setting_request(
            Method::PATCH,
            row,
            bearer,
            Some(json!({"value": {
                "stream_startup_timeout": 3,
                "connection_timeout": 4,
                "channel_init_grace_period": initialization,
            }})),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[ignore = "requires retained owned Core PostgreSQL/Redis test role and signed admin fixture"]
async fn owned_engine_consumes_same_core_cache_without_a_fifth_writer() {
    assert_eq!(
        env::var("CORE_PROXY_HEALTH_AUTHORITY").as_deref(),
        Ok("owned-c176-synthetic-component-only")
    );
    let database = env::var("POSTGRES_DB").unwrap();
    let identity = database.strip_prefix("rust_special_").unwrap();
    assert!(
        !identity.is_empty()
            && database.len() <= 63
            && identity
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    );
    let database_url = env::var("DATABASE_TEST_URL").unwrap();
    let parsed = url::Url::parse(&database_url).unwrap();
    assert_eq!(parsed.path().strip_prefix('/'), Some(database.as_str()));
    assert_eq!(parsed.host_str(), Some("127.0.0.1"));
    let log_dir = PathBuf::from(env::var("CORE_SPECIAL_LOG_DIRECTORY").unwrap());
    assert!(log_dir.is_absolute() && log_dir.is_dir());
    let row = env::var("CORE_PROXY_HEALTH_ROW")
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let jwt_secret = env::var("DJANGO_SECRET_KEY").unwrap();
    let signed_admin = env::var("CORE_PROXY_HEALTH_SIGNED_ADMIN").unwrap();
    let redis_url = env::var("CORE_SPECIAL_REDIS_URL").unwrap();
    let parsed = url::Url::parse(&redis_url).unwrap();
    assert_eq!(parsed.scheme(), "redis");
    assert_eq!(parsed.host_str(), Some("127.0.0.1"));

    tokio::time::timeout(Duration::from_secs(30), async {
        let store = Store::connect(&database_url, 4).await.unwrap();
        assert_eq!(
            store.setting("proxy_settings").await.unwrap()["channel_init_grace_period"],
            60
        );
        let cache = NotificationCache::connect(&redis_url).await.unwrap();
        let bus = RealtimeBus::connect(&redis_url, BusConfig::default())
            .await
            .unwrap();
        let auth = AuthService::new(store.clone(), AuthConfig::new(jwt_secret).unwrap())
            .with_notification_cache(cache)
            .with_event_sink(Arc::new(bus.clone()));
        let core = CoreSettingsWrites::new_main(auth.clone())
            .unwrap()
            .with_special_effects(CoreSettingsSpecialEffects::new(log_dir).unwrap())
            .unwrap();
        let router = auth.router_with_core_settings(core.clone()).unwrap();
        let engine = StreamEngine::new(EngineConfig::default())
            .unwrap()
            .with_health_settings(Arc::new(CoreProxyHealthSource::new(core.clone())))
            .unwrap();

        // Warm the real component cache just as the retained Source helper does.
        // There is no added HTTP control route or synthetic semaphore permit.
        let warm = core.proxy_settings().await.unwrap();
        assert_eq!(warm.health_knobs().unwrap().initialization.as_secs(), 60);
        drop(warm);

        let mut retained = Vec::with_capacity(3);
        for _ in 0..3 {
            let response = router
                .clone()
                .oneshot(setting_request(Method::GET, row, &signed_admin, None))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            retained.push(response);
        }
        let ownership = core.ownership_state();
        assert_eq!(ownership["active"], 3);
        assert_eq!(ownership["available_writer_permits"], 0);
        assert_eq!(ownership["retained_proxy_entries"], 1);
        let fifth = router
            .clone()
            .oneshot(setting_request(Method::GET, row, &signed_admin, None))
            .await
            .unwrap();
        assert_eq!(fifth.status(), StatusCode::SERVICE_UNAVAILABLE);
        let fallback = Duration::from_secs(99);
        assert_eq!(
            engine.hls_initialization_grace(fallback).await.unwrap(),
            Duration::from_secs(60)
        );
        assert_eq!(core.ownership_state(), ownership);
        drop(fifth);
        drop(retained);

        // The inherited hit did not install a TS-owned slot. A normal writer
        // clears Base, then the Engine's next ordinary call fills its own slot.
        update_proxy(&router, row, &signed_admin, 5).await;
        assert_eq!(core.ownership_state()["retained_proxy_entries"], 0);
        assert_eq!(
            engine.hls_initialization_grace(fallback).await.unwrap(),
            Duration::from_secs(5)
        );
        update_proxy(&router, row, &signed_admin, 6).await;
        assert_eq!(
            engine.hls_initialization_grace(fallback).await.unwrap(),
            Duration::from_secs(5)
        );

        assert!(engine.shutdown_wait(Duration::from_secs(2)).await);
        assert!(core.shutdown().await);
        let drained = core.ownership_state();
        assert_eq!(drained["active"], 0);
        assert_eq!(drained["available_writer_permits"], 4);
        assert_eq!(drained["retained_proxy_entries"], 0);
        assert_eq!(drained["admission_closed"], true);
        assert_eq!(
            engine
                .hls_initialization_grace(fallback)
                .await
                .unwrap_err()
                .status,
            503
        );
        bus.stop_accepting();
        drop(router);
        store.pool().close().await;
    })
    .await
    .expect("owned Core and Engine component deadline");
}
