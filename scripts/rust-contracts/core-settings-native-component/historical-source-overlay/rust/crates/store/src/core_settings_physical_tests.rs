//! Test-only real HTTP peer for the finite owned CoreSettings fixture.
//! Original Dispatcharr authors retain credit. No production ingress is enabled.
use super::*;
use crate::{AuthConfig, NotificationCache, Store};
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

#[derive(Clone)]
struct Control {
    service: CoreSettingsWrites,
    closing: Arc<AtomicBool>,
    notify: Arc<Notify>,
    held: Arc<tokio::sync::Mutex<Vec<Response>>>,
}
async fn state(State(c): State<Control>) -> axum::Json<Value> {
    axum::Json(json!({
        "active": c.service.active_requests(),
        "available_writer_permits": c.service.state.auth.user_agent_workers.available_permits(),
        "cache_physical_operations": c.service.state.auth.notification_cache.as_ref().unwrap().physical_operations(),
        "admission_closed": c.service.state.stopped.load(Ordering::Acquire)
    }))
}
async fn close(State(c): State<Control>) -> axum::Json<Value> {
    c.closing.store(true, Ordering::Release);
    c.notify.notify_waiters();
    axum::Json(json!({"close_requested": true}))
}

async fn drain(State(c): State<Control>) -> axum::Json<Value> {
    axum::Json(json!({"shutdown_drained": c.service.shutdown().await}))
}
async fn hold_frame(
    State(c): State<Control>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> axum::Json<Value> {
    let mut held = c.held.lock().await;
    assert!(held.len() < 4);
    let mut request = Request::builder()
        .method("GET")
        .uri("/api/core/settings/1002/")
        .body(axum::body::Body::empty())
        .unwrap();
    *request.headers_mut() = headers;
    let response = c
        .service
        .run(peer, Some("1002".into()), request, Kind::Read)
        .await
        .unwrap();
    let status = response.status().as_u16();
    held.push(response);
    axum::Json(json!({"held_count":held.len(),"Core_status":status}))
}
async fn release_frame(State(c): State<Control>) -> axum::Json<Value> {
    let response = std::mem::take(&mut *c.held.lock().await);
    let count = response.len();
    drop(response);
    axum::Json(json!({"released":count}))
}

#[tokio::test]
#[ignore = "requires reviewed owned PostgreSQL/Redis fixture and exact finite controller"]
async fn actual_core32_http_peer() {
    assert_eq!(
        std::env::var("CORE_SETTINGS_AUTHORITY").as_deref(),
        Ok("owned-native-core32-only")
    );
    let root = PathBuf::from(std::env::var("CORE_SETTINGS_OUTPUT").unwrap());
    assert!(root.is_dir());
    let token = std::env::var("CORE_SETTINGS_TOKEN").unwrap();
    assert_eq!(token.len(), 32);
    assert!(token
        .bytes()
        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
    let database = std::env::var("POSTGRES_DB").unwrap();
    let suffix = database.strip_prefix("rust_probe_core32_").unwrap();
    assert_eq!(suffix.len(), 10);
    assert!(suffix
        .bytes()
        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 4)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT current_database()")
            .fetch_one(store.pool())
            .await
            .unwrap(),
        database
    );
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("core32-owned-no-JWT-use".into()).unwrap(),
    )
    .with_notification_cache(
        NotificationCache::connect(&std::env::var("CORE_SETTINGS_REDIS_URL").unwrap())
            .await
            .unwrap(),
    );
    let service = CoreSettingsWrites::new(auth).unwrap();
    let control = Control {
        service: service.clone(),
        closing: Arc::new(AtomicBool::new(false)),
        notify: Arc::new(Notify::new()),
        held: Arc::new(tokio::sync::Mutex::new(Vec::new())),
    };
    let controls = Router::new()
        .route("/__core_fixture_state__", get(state))
        .route("/__core_fixture_close__", post(close))
        .route("/__core_fixture_drain__", post(drain))
        .route(
            "/__core_fixture_hold_frame__",
            post(hold_frame).delete(release_frame),
        )
        .with_state(control.clone());
    let app = service.clone().router().merge(controls);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let ready = root.join("core-peer-ready-private.json");
    assert!(!ready.exists());
    std::fs::write(&ready, serde_json::to_vec(&json!({"pid":std::process::id(),"port":address.port(),"token":token,"database":database,"initial_active":0,"initial_permits":4})).unwrap()).unwrap();
    let closing = control.clone();
    let serve = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        loop {
            let notified = closing.notify.notified();
            if closing.closing.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    });
    tokio::time::timeout(Duration::from_secs(600), serve)
        .await
        .unwrap()
        .unwrap();
    let drained = service.shutdown().await;
    assert!(drained);
    assert_eq!(service.active_requests(), 0);
    assert_eq!(service.state.auth.user_agent_workers.available_permits(), 4);
    assert_eq!(
        service
            .state
            .auth
            .notification_cache
            .as_ref()
            .unwrap()
            .physical_operations(),
        0
    );
    store.pool().close().await;
    std::fs::write(root.join("core-peer-terminal-private.json"), serde_json::to_vec(&json!({"shutdown_drained":drained,"active":0,"available_writer_permits":4,"cache_physical_operations":0,"pool_closed":store.pool().is_closed()})).unwrap()).unwrap();
}
