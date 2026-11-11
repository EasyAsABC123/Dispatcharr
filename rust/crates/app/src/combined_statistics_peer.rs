//! Environment-guarded actual SQL/Redis ownership observer. Test binary only.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::*;
use axum::routing::{get, post};
use std::{future::IntoFuture, path::PathBuf, sync::Mutex};

#[derive(Clone)]
struct Peer {
    service: CombinedStatistics,
    held: Arc<Mutex<Vec<Body>>>,
    observers: Arc<Mutex<Vec<tokio::task::JoinHandle<Response>>>>,
}
async fn state(State(peer): State<Peer>) -> axum::Json<Value> {
    let inner = &peer.service.inner;
    axum::Json(
        json!({"active":inner.config.workers-inner.slots.available_permits(),
        "retained_bytes":inner.config.memory_bytes-inner.memory.available_permits(),
        "closed":inner.slots.is_closed(),"held_frames":peer.held.lock().unwrap().len()}),
    )
}
async fn hold(State(peer): State<Peer>, mut request: Request<Body>) -> Response {
    *request.uri_mut() = "/proxy/stats/".parse().unwrap();
    let response = dispatch(State(peer.service.clone()), request).await;
    if response.status() != StatusCode::OK {
        return response;
    }
    peer.held.lock().unwrap().push(response.into_body());
    axum::Json(json!({"held":true})).into_response()
}
async fn start_observer(State(peer): State<Peer>, mut request: Request<Body>) -> Response {
    *request.uri_mut() = "/proxy/stats/".parse().unwrap();
    let service = peer.service.clone();
    let handle = tokio::spawn(async move { dispatch(State(service), request).await });
    peer.observers.lock().unwrap().push(handle);
    axum::Json(json!({"started":true})).into_response()
}
async fn abort_observers(State(peer): State<Peer>) -> Response {
    let handles = std::mem::take(&mut *peer.observers.lock().unwrap());
    let count = handles.len();
    for handle in handles {
        handle.abort();
        let _ = handle.await;
    }
    axum::Json(json!({"aborted_observer_futures":count})).into_response()
}
async fn release(State(peer): State<Peer>) -> Response {
    peer.held.lock().unwrap().clear();
    axum::Json(json!({"released":true})).into_response()
}
async fn close(State(peer): State<Peer>) -> Response {
    let drained = peer.service.shutdown(Duration::from_millis(10)).await;
    axum::Json(json!({"drained":drained})).into_response()
}

#[tokio::test]
#[ignore = "Owned combined statistics physical SQL/Redis/frame fixture only"]
async fn actual_combined_stats_physical_peer() {
    assert_eq!(
        std::env::var("COMBINED_STATS_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let directory = PathBuf::from(std::env::var("COMBINED_STATS_PEER_DIR").unwrap());
    assert!(directory.starts_with("/evidence") && directory.as_os_str().len() < 256);
    assert!(!directory
        .components()
        .any(|part| matches!(part, std::path::Component::ParentDir)));
    let url = "redis://:playback-fixture-only@127.0.0.1:6379/0";
    let store = dispatcharr_store::Store::connect(
        "postgresql://dispatch:playback-fixture-only@127.0.0.1:5432/dispatcharr",
        8,
    )
    .await
    .unwrap();
    let auth = AuthService::new(
        store.clone(),
        dispatcharr_store::AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(
        dispatcharr_store::NotificationCache::connect(url)
            .await
            .unwrap(),
    );
    let coordinator = dispatcharr_coordination::Coordinator::connect(
        url,
        dispatcharr_coordination::LeaseConfig::default(),
    )
    .await
    .unwrap();
    let clients = dispatcharr_clients::ClientStore::connect(
        url,
        dispatcharr_clients::ClientConfig::default(),
    )
    .await
    .unwrap();
    let live = StreamEngine::new_with_services(
        dispatcharr_stream_http::EngineConfig::default(),
        None,
        Some(clients.clone()),
    )
    .unwrap();
    let sessions = dispatcharr_timeshift_http::CatchupSessions::new(
        store.clone(),
        auth.clone(),
        url,
        dispatcharr_timeshift_http::SessionConfig::default(),
    )
    .await
    .unwrap();
    let media = dispatcharr_timeshift_http::CatchupMedia::new(
        store.clone(),
        auth.clone(),
        sessions,
        coordinator,
        url,
        dispatcharr_timeshift_http::CatchupMediaConfig::default(),
    )
    .await
    .unwrap();
    let catchup = CatchupManagement::new(store.clone(), auth.clone(), media.clone(), url)
        .await
        .unwrap();
    let mut config = Config::default();
    if std::env::var("COMBINED_STATS_SMALL_OUTPUT").as_deref() == Ok("1") {
        config.response_bytes = 64 * 1024;
    }
    let service = CombinedStatistics::connect(
        auth,
        store,
        live.clone(),
        Some(catchup.clone()),
        Some(url),
        config,
    )
    .await
    .unwrap();
    let peer = Peer {
        service: service.clone(),
        held: Arc::new(Mutex::new(Vec::new())),
        observers: Arc::new(Mutex::new(Vec::new())),
    };
    let controls = Router::new()
        .route("/owned/stats/state", get(state))
        .route("/owned/stats/hold", get(hold))
        .route("/owned/stats/start-observer", get(start_observer))
        .route("/owned/stats/abort-observers", post(abort_observers))
        .route("/owned/stats/release", post(release))
        .route("/owned/stats/close", post(close))
        .with_state(peer.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:9194")
        .await
        .unwrap();
    std::fs::write(directory.join("ready"), b"ready").unwrap();
    let stop = directory.join("stop");
    let server = axum::serve(
        listener,
        service
            .router()
            .merge(controls)
            .into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        while !stop.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    tokio::time::timeout(Duration::from_secs(90), server.into_future())
        .await
        .unwrap()
        .unwrap();
    peer.held.lock().unwrap().clear();
    assert!(service.shutdown(Duration::from_secs(8)).await);
    assert_eq!(
        service.inner.memory.available_permits(),
        service.inner.config.memory_bytes
    );
    live.shutdown();
    clients.shutdown().await;
    assert!(catchup.shutdown().await);
    assert!(media.shutdown().await);
}
