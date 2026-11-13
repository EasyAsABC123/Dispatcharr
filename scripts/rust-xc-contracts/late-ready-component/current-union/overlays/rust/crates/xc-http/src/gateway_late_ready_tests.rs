//! Native-only fault peer: same admitted request/memory owners, actual SQL.
//! This observer-abort control is distinct from TCP client disconnection.
//! Original Dispatcharr and SQLx authors retain source/algorithm credit.
use super::*;
use std::path::PathBuf;
use tokio::task::JoinHandle;

type Observer = JoinHandle<Result<Response, StoreError>>;
#[derive(Clone)]
struct Control {
    gateway: XcGateway,
    query: String,
    observer: Arc<tokio::sync::Mutex<Option<Observer>>>,
    stopped: Arc<Notify>,
    prelude_phase: Arc<std::sync::atomic::AtomicUsize>,
}
async fn prelude(State(c): State<Control>) -> axum::Json<serde_json::Value> {
    assert!(c.observer.lock().await.is_none());
    assert_eq!(
        c.prelude_phase
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire),
        Ok(0)
    );
    let result = match c.gateway.handle(request(&c.query, false)).await {
        Err(_) => json!({"kind":"gateway_error","Native_gateway_prelude_calls":1}),
        Ok(response) => {
            let status = response.status().as_u16();
            let data = response.into_body().collect().await.unwrap().to_bytes();
            assert!(data.len() <= c.gateway.inner.config.max_response_bytes);
            let bytes = data.len();
            drop(data);
            json!({"kind":"response","status":status,"body_bytes":bytes,"Native_gateway_prelude_calls":1})
        }
    };
    c.prelude_phase.store(
        if result["status"] == 200 { 2 } else { 3 },
        Ordering::Release,
    );
    axum::Json(result)
}
async fn start(State(c): State<Control>) -> axum::Json<serde_json::Value> {
    assert_eq!(c.prelude_phase.load(Ordering::Acquire), 2);
    let mut slot = c.observer.lock().await;
    assert!(slot.is_none());
    let gateway = c.gateway.clone();
    let input = request(&c.query, false);
    *slot = Some(tokio::spawn(async move { gateway.handle(input).await }));
    axum::Json(json!({"actual_observer_started":true}))
}
async fn state(State(c): State<Control>) -> axum::Json<serde_json::Value> {
    axum::Json(json!({
        "active_requests":c.gateway.active_requests(),
        "available_memory_bytes":c.gateway.available_memory_bytes(),
        "maximum_memory_bytes":c.gateway.inner.config.max_memory_bytes,
        "workers":c.gateway.inner.workers.load(Ordering::Acquire),
        "observer_finished":c.observer.lock().await.as_ref().is_some_and(JoinHandle::is_finished),
        "admission_closed":c.gateway.inner.stopped.load(Ordering::Acquire)
    }))
}
async fn abort(State(c): State<Control>) -> axum::Json<serde_json::Value> {
    c.observer.lock().await.as_ref().unwrap().abort();
    axum::Json(
        json!({"explicit_native_observer_abort_requested":true,"TCP_handler_abort_claimed":false}),
    )
}
async fn collect(State(c): State<Control>) -> axum::Json<serde_json::Value> {
    let observer = c.observer.lock().await.take().unwrap();
    let result = match observer.await {
        Err(error) => json!({"kind":"join_error","cancelled":error.is_cancelled()}),
        Ok(Err(_)) => json!({"kind":"gateway_error","Source_error_body_parity":false}),
        Ok(Ok(response)) => {
            let status = response.status().as_u16();
            let data = response.into_body().collect().await.unwrap().to_bytes();
            assert!(data.len() <= c.gateway.inner.config.max_response_bytes);
            json!({"kind":"response","status":status,"body_bytes":data.len(),"Source_error_body_parity":false})
        }
    };
    axum::Json(result)
}
async fn shutdown_probe(State(c): State<Control>) -> axum::Json<serde_json::Value> {
    axum::Json(json!({"shutdown_drained":c.gateway.shutdown().await}))
}
async fn stop(State(c): State<Control>) -> axum::Json<serde_json::Value> {
    c.stopped.notify_one();
    axum::Json(json!({"stop_requested":true}))
}

#[tokio::test]
#[ignore = "requires reviewed real PostgreSQL ErrorResponse/lateReady fixture; no Main"]
async fn actual_error_ready_owned_request_peer() {
    assert_eq!(
        std::env::var("XC_ERROR_AUTHORITY").as_deref(),
        Ok("owned-xc-late-ready-only")
    );
    let kind: u8 = std::env::var("XC_ERROR_CASE").unwrap().parse().unwrap();
    assert!((1..=5).contains(&kind));
    let folder = PathBuf::from(std::env::var("XC_ERROR_OUTPUT").unwrap());
    assert!(folder.is_dir());
    let token = std::env::var("XC_ERROR_TOKEN").unwrap();
    assert_eq!(token.len(), 32);
    assert!(token
        .bytes()
        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
    let (gateway, base) = fixture().await;
    assert_eq!(gateway.inner.config.max_requests, 1);
    assert_eq!(
        gateway.inner.config.request_deadline,
        Duration::from_secs(15)
    );
    let gateway = gateway.with_vod_lists_for_test();
    let gateway = if kind >= 4 {
        gateway.with_vod_info_for_test(
            std::env::var("XC_INFO_ELIGIBILITY_MICROS")
                .unwrap()
                .parse()
                .unwrap(),
        )
    } else {
        gateway
    };
    let action = match kind {
        1 | 2 => "get_vod_categories".into(),
        3 => "get_vod_streams".into(),
        4 => format!(
            "get_vod_info&vod_id={}",
            std::env::var("XC_INFO_MOVIE_ID")
                .unwrap()
                .parse::<i64>()
                .unwrap()
        ),
        5 => format!(
            "get_series_info&series_id={}",
            std::env::var("XC_INFO_SERIES_ID")
                .unwrap()
                .parse::<i64>()
                .unwrap()
        ),
        _ => unreachable!(),
    };
    let control = Control {
        gateway: gateway.clone(),
        query: format!("{base}&action={action}"),
        observer: Arc::new(tokio::sync::Mutex::new(None)),
        stopped: Arc::new(Notify::new()),
        prelude_phase: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    };
    let app = Router::new()
        .route("/prelude", axum::routing::post(prelude))
        .route("/start", axum::routing::post(start))
        .route("/state", axum::routing::get(state))
        .route("/abort", axum::routing::post(abort))
        .route("/collect", axum::routing::post(collect))
        .route("/stop", axum::routing::post(stop))
        .route("/shutdown_probe", axum::routing::post(shutdown_probe))
        .with_state(control.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    std::fs::write(folder.join("ready-private.json"),serde_json::to_vec(&json!({"port":port,"PID":std::process::id(),"token":token,"case":kind,"max_requests":1,"request_deadline_seconds":15,"Main":false})).unwrap()).unwrap();
    let stopped = control.stopped.clone();
    tokio::time::timeout(
        Duration::from_secs(120),
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                stopped.notified().await;
            })
            .into_future(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(gateway.shutdown().await);
    assert_eq!(gateway.active_requests(), 0);
    assert_eq!(
        gateway.available_memory_bytes(),
        gateway.inner.config.max_memory_bytes
    );
    gateway.inner.store.pool().close().await;
    std::fs::write(folder.join("terminal-private.json"),serde_json::to_vec(&json!({"active_requests":0,"available_memory_bytes":gateway.available_memory_bytes(),"maximum_memory_bytes":gateway.inner.config.max_memory_bytes,"pool_closed":true})).unwrap()).unwrap();
}
