//! Fixture-only real TCP Core51 peer assembled with the EXISTING RealtimeBus.
//! No Store->Realtime dependency cycle, fake event sink or new writer pool.
use crate::{BusConfig, RealtimeBus};
use axum::{
    extract::{ConnectInfo, State},
    http::HeaderMap,
    response::Response,
    routing::{get, post},
    Router,
};
use dispatcharr_store::{
    AuthConfig, AuthService, CoreSettingsSpecialEffects, CoreSettingsWrites, NotificationCache,
    Store,
};
use serde_json::{json, Value};
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{Mutex, Notify};
use tower::ServiceExt;
#[derive(Clone)]
struct Control {
    service: CoreSettingsWrites,
    closed: Arc<AtomicBool>,
    notify: Arc<Notify>,
    frames: Arc<Mutex<Vec<Response>>>,
    reads: Arc<Mutex<Vec<dispatcharr_store::ProxySettingsRead>>>,
    observation_busy: Arc<AtomicBool>,
}
struct Readout {
    bytes: Vec<u8>,
    _read: Option<dispatcharr_store::ProxySettingsRead>,
    _gate: ReadoutGate,
}
impl AsRef<[u8]> for Readout {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
struct ReadoutGate(Arc<AtomicBool>);
impl Drop for ReadoutGate {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
struct Count(usize);
impl std::io::Write for Count {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(b.len())
            .filter(|n| *n <= 65536)
            .ok_or_else(|| std::io::Error::other("ReadoutCapacity"))?;
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
async fn state(State(c): State<Control>) -> Response {
    use axum::response::IntoResponse;
    if c.observation_busy
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let gate = ReadoutGate(c.observation_busy.clone());
    let read = c.service.cached_proxy_settings();
    let ownership = c.service.ownership_state();
    let value = (ownership, read.as_ref().map(|v| v.value()));
    let mut count = Count(0);
    if serde_json::to_writer(&mut count, &value).is_err() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let mut bytes = Vec::with_capacity(count.0);
    serde_json::to_writer(&mut bytes, &value).unwrap();
    let bytes = axum::body::Bytes::from_owner(Readout {
        bytes,
        _read: read,
        _gate: gate,
    });
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        bytes,
    )
        .into_response()
}
async fn stop(State(c): State<Control>) -> axum::Json<Value> {
    c.closed.store(true, Ordering::Release);
    c.notify.notify_waiters();
    axum::Json(json!({"stop_requested":true}))
}
async fn drain(State(c): State<Control>) -> axum::Json<Value> {
    axum::Json(json!({"drained":c.service.shutdown().await}))
}
async fn warm(State(c): State<Control>) -> axum::Json<Value> {
    match c.service.proxy_settings().await {
        Ok(read) => {
            drop(read);
            axum::Json(json!({"loaded":true,"ownership":c.service.ownership_state()}))
        }
        Err(error) => axum::Json(
            json!({"loaded":false,"error":format!("{error:?}"),"ownership":c.service.ownership_state()}),
        ),
    }
}
async fn warm_group(State(c): State<Control>) -> axum::Json<Value> {
    let group = std::env::var("CORE_SPECIAL_GROUP").unwrap();
    match c.service.settings_group(&group).await {
        Ok(read) => {
            drop(read);
            axum::Json(json!({"loaded":true,"group":group,"ownership":c.service.ownership_state()}))
        }
        Err(error) => axum::Json(
            json!({"loaded":false,"error":format!("{error:?}"),"ownership":c.service.ownership_state()}),
        ),
    }
}
async fn warm_ua(State(c): State<Control>) -> axum::Json<Value> {
    match c.service.default_user_agent().await {
        Ok(read) => {
            drop(read);
            axum::Json(json!({"loaded":true,"ownership":c.service.ownership_state()}))
        }
        Err(error) => axum::Json(
            json!({"loaded":false,"error":format!("{error:?}"),"ownership":c.service.ownership_state()}),
        ),
    }
}
async fn hold_read(State(c): State<Control>) -> axum::Json<Value> {
    let mut reads = c.reads.lock().await;
    assert!(reads.len() < 4);
    match c.service.proxy_settings().await {
        Ok(read) => {
            reads.push(read);
            axum::Json(json!({"held_reads":reads.len(),"ownership":c.service.ownership_state()}))
        }
        Err(error) => axum::Json(
            json!({"error":format!("{error:?}"),"ownership":c.service.ownership_state()}),
        ),
    }
}
async fn release_read(State(c): State<Control>) -> axum::Json<Value> {
    let reads = std::mem::take(&mut *c.reads.lock().await);
    let count = reads.len();
    drop(reads);
    axum::Json(json!({"released_reads":count,"ownership":c.service.ownership_state()}))
}
async fn hold_frame(
    State(c): State<Control>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> axum::Json<Value> {
    let mut frames = c.frames.lock().await;
    assert!(frames.len() < 4);
    // Native-only response ownership control. This synthetic internal request is
    // NOT a Source business request or a claim of unread TCP client backpressure.
    let mut request = axum::http::Request::builder()
        .method("GET")
        .uri(std::env::var("CORE_SPECIAL_FRAME_TARGET").unwrap())
        .body(axum::body::Body::empty())
        .unwrap();
    *request.headers_mut() = headers;
    request.extensions_mut().insert(ConnectInfo(peer));
    let response = c.service.clone().router().oneshot(request).await.unwrap();
    let status = response.status().as_u16();
    frames.push(response);
    axum::Json(
        json!({"held_frames":frames.len(),"status":status,"ownership":c.service.ownership_state()}),
    )
}
async fn release_frame(State(c): State<Control>) -> axum::Json<Value> {
    let frames = std::mem::take(&mut *c.frames.lock().await);
    let count = frames.len();
    drop(frames);
    axum::Json(json!({"released_frames":count,"ownership":c.service.ownership_state()}))
}
#[tokio::test]
#[ignore = "requires exact reviewed owned Core51/Realtime fixture; no Source replay"]
async fn actual_core51_realtime_http_peer() {
    assert_eq!(
        std::env::var("CORE_SPECIAL_AUTHORITY").as_deref(),
        Ok("owned-native-special51-only")
    );
    let database = std::env::var("POSTGRES_DB").unwrap();
    assert!(
        database.starts_with("rust_special_")
            && database.len() <= 63
            && database
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    );
    let root = PathBuf::from(std::env::var("CORE_SPECIAL_OUTPUT").unwrap());
    assert!(root.is_dir());
    let token = std::env::var("CORE_SPECIAL_TOKEN").unwrap();
    assert_eq!(token.len(), 32);
    assert!(token
        .bytes()
        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 4)
        .await
        .unwrap();
    assert_eq!(sqlx_database(&store).await, database);
    let redis = std::env::var("CORE_SPECIAL_REDIS_URL").unwrap();
    let cache = NotificationCache::connect(&redis).await.unwrap();
    // Existing bounded transport implementation, actual unconsumed Channels
    // frames. Its global transport budgets are not a fifth request allowance.
    let bus = RealtimeBus::connect(&redis, BusConfig::default())
        .await
        .unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("owned-special-no-JWT-use".into()).unwrap(),
    )
    .with_notification_cache(cache)
    .with_event_sink(Arc::new(bus));
    let existing_collection_get = auth.clone().core_settings_collection_router();
    let effects = CoreSettingsSpecialEffects::new(PathBuf::from(
        std::env::var("CORE_SPECIAL_LOG_DIRECTORY").unwrap(),
    ))
    .unwrap();
    let service = CoreSettingsWrites::new(auth)
        .unwrap()
        .with_special_effects(effects)
        .unwrap();
    let c = Control {
        service: service.clone(),
        closed: Arc::new(AtomicBool::new(false)),
        notify: Arc::new(Notify::new()),
        frames: Arc::new(Mutex::new(Vec::new())),
        reads: Arc::new(Mutex::new(Vec::new())),
        observation_busy: Arc::new(AtomicBool::new(false)),
    };
    let controls = Router::new()
        .route("/__special_state__", get(state))
        .route("/__special_stop__", post(stop))
        .route("/__special_drain__", post(drain))
        .route("/__special_warm_proxy__", post(warm))
        .route("/__special_warm_group__", post(warm_group))
        .route("/__special_warm_ua__", post(warm_ua))
        .route(
            "/__special_hold_read__",
            post(hold_read).delete(release_read),
        )
        .route(
            "/__special_hold_frame__",
            post(hold_frame).delete(release_frame),
        )
        .with_state(c.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let path = root.join("special-peer-ready-private.json");
    assert!(!path.exists());
    std::fs::write(path,serde_json::to_vec(&json!({"pid":std::process::id(),"port":address.port(),"token":token,"database":database,"ownership":service.ownership_state(),"real_realtime_bus":true})).unwrap()).unwrap();
    let close = c.clone();
    let serve = axum::serve(
        listener,
        service
            .clone()
            .router()
            .merge(existing_collection_get)
            .merge(controls)
            .into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        loop {
            let notified = close.notify.notified();
            if close.closed.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    });
    tokio::time::timeout(Duration::from_secs(600), serve)
        .await
        .unwrap()
        .unwrap();
    assert!(c.frames.lock().await.is_empty());
    assert!(c.reads.lock().await.is_empty());
    assert!(service.shutdown().await);
    assert_eq!(service.ownership_state()["available_writer_permits"], 4);
    store.pool().close().await;
    std::fs::write(
        root.join("special-peer-terminal-private.json"),
        serde_json::to_vec(
            &json!({"pool_closed":store.pool().is_closed(),"ownership":service.ownership_state()}),
        )
        .unwrap(),
    )
    .unwrap();
}
async fn sqlx_database(store: &Store) -> String {
    sqlx::query_scalar::<_, String>("SELECT current_database()")
        .fetch_one(store.pool())
        .await
        .unwrap()
}
