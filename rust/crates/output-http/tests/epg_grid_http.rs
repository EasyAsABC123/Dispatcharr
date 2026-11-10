//! Independent native HTTP versus actual original APIRequestFactory captures.
//! This is not a packaged application or Source-network GET parity claim.
//! All original application credit belongs to Dispatcharr's authors/contributors.
use axum::http::HeaderMap;
use dispatcharr_output_http::{GridGateway, GridReadLimits};
use dispatcharr_store::{AuthConfig, AuthService, NotificationCache, Store};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{Connection, Executor};
use std::{net::SocketAddr, time::Duration};

async fn setup(limits: GridReadLimits) -> (Store, GridGateway, Value) {
    assert_eq!(
        std::env::var("EPG_READ_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("GUIDE_READ_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let now = chrono::DateTime::parse_from_rfc3339(fixture["clock"].as_str().unwrap())
        .unwrap()
        .with_timezone(&chrono::Utc);
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
    let gateway = GridGateway::new(store.clone(), auth, limits)
        .unwrap()
        .with_clock(move || now)
        .unwrap();
    (store, gateway, fixture)
}
fn headers(case: &Value) -> HeaderMap {
    let mut result = HeaderMap::new();
    for (key, value) in case["headers"].as_object().unwrap() {
        let name = key
            .strip_prefix("HTTP_")
            .unwrap()
            .replace('_', "-")
            .to_lowercase();
        result.insert(
            axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            value.as_str().unwrap().parse().unwrap(),
        );
    }
    result
}
fn admin_headers(fixture: &Value) -> HeaderMap {
    headers(
        fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["path"] == "/api/epg/grid/" && c["role"] == "admin")
            .unwrap(),
    )
}
async fn drained(gateway: &GridGateway) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while gateway.physical_operations() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
#[ignore = "requires actual original guide corpus and disposable PostgreSQL/Redis"]
async fn original_factory_grid_corpus_and_actual_network_head() {
    let (_, gateway, fixture) = setup(GridReadLimits::default()).await;
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
    let client = reqwest::Client::new();
    let mut observed = 0;
    let mut exceptions = 0;
    let mut unsupported = 0;
    for case in fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .chain(
            fixture
                .get("negotiation_cases")
                .and_then(Value::as_array)
                .into_iter()
                .flatten(),
        )
        .filter(|c| c["path"].as_str().unwrap().starts_with("/api/epg/grid/"))
    {
        let expected = &case["expected"];
        if expected.get("unhandled_exception").is_some() {
            // Factory does not run Django's outer error middleware; no inferred HTTP500 body.
            exceptions += 1;
            continue;
        }
        let method = case["method"].as_str().unwrap();
        let response = client
            .request(
                method.parse().unwrap(),
                format!("{base}{}", case["path"].as_str().unwrap()),
            )
            .headers(headers(case))
            .send()
            .await
            .unwrap();
        if expected["content_type"]
            .as_str()
            .is_some_and(|v| v.starts_with("text/html"))
        {
            assert_eq!(
                response.status().as_u16(),
                501,
                "unsupported original browsable error: {case}"
            );
            println!(
                "GRID_BROWSABLE_ERROR_UNSUPPORTED method={method} role={}",
                case["role"]
            );
            unsupported += 1;
            continue;
        }
        assert_eq!(
            response.status().as_u16() as u64,
            expected["status"].as_u64().unwrap(),
            "{case}"
        );
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap(),
            expected["content_type"].as_str().unwrap(),
            "{case}"
        );
        for key in ["Allow", "WWW-Authenticate"] {
            if let Some(value) = expected["selected_headers"][key].as_str() {
                assert_eq!(
                    response.headers().get(key).map(|v| v.to_str().unwrap()),
                    Some(value),
                    "header {key}: {case}"
                );
            }
        }
        let response_headers = response.headers().clone();
        let body = response.bytes().await.unwrap();
        if method == "HEAD" {
            let wire = &expected["actual_http_head"];
            assert_eq!(wire["body_bytes"], 0);
            assert!(body.is_empty());
            assert_eq!(wire["status"], expected["status"]);
            assert_eq!(wire["content_type"], expected["content_type"]);
            assert_eq!(
                response_headers
                    .get("content-length")
                    .map(|v| v.to_str().unwrap()),
                wire["selected_headers"]["Content-Length"].as_str()
            );
        } else {
            assert_eq!(
                body.len() as u64,
                expected["body_bytes"].as_u64().unwrap(),
                "{case}"
            );
            assert_eq!(
                format!("{:x}", Sha256::digest(&body)),
                expected["body_sha256"].as_str().unwrap(),
                "{case}"
            );
        }
        observed += 1;
    }
    assert_eq!(
        observed + exceptions + unsupported,
        40 + fixture
            .get("negotiation_cases")
            .and_then(Value::as_array)
            .map_or(0, Vec::len)
    );
    assert!(gateway.shutdown(Duration::from_secs(2)).await);
    server.abort();
    println!("GRID_HTTP_FACTORY_CASES={observed}; BROWSABLE_ERROR_HTML_UNSUPPORTED={unsupported}; FACTORY_UNHANDLED_EXCEPTIONS_NOT_INFERRED={exceptions}; ACTUAL_SOURCE_HEAD=1");
}
#[tokio::test]
#[ignore = "requires disposable guide SQL fixture and actual PostgreSQL lock waits"]
async fn physical_admission_survives_abort_timeout_and_detached_frames() {
    let limits = GridReadLimits {
        concurrent_responses: 1,
        memory_bytes: 96 * 1024 * 1024,
        deadline: Duration::from_millis(150),
    };
    let (store, gateway, fixture) = setup(limits).await;
    let peer = "127.0.0.1:12345".parse().unwrap();
    {
        let mut conn = sqlx::PgConnection::connect(&std::env::var("DATABASE_TEST_URL").unwrap())
            .await
            .unwrap();
        let mut lock = conn.begin().await.unwrap();
        lock.execute("LOCK TABLE epg_programdata IN ACCESS EXCLUSIVE MODE")
            .await
            .unwrap();
        let reader = gateway.clone();
        let auth = admin_headers(&fixture);
        let task = tokio::spawn(async move { reader.read(peer, auth, Vec::new()).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let waiting: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT p.id::bigint AS id,p.start_time%'").fetch_one(store.pool()).await.unwrap();
                if waiting > 0 { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.unwrap();
        task.abort();
        let _ = task.await;
        assert_eq!(gateway.physical_operations(), 1);
        assert!(gateway
            .read(peer, admin_headers(&fixture), Vec::new())
            .await
            .is_err());
        assert!(!gateway.shutdown(Duration::from_millis(20)).await);
        lock.rollback().await.unwrap();
        drained(&gateway).await;
    }
    let (_, timeout_gateway, _) = setup(limits).await;
    let mut conn = sqlx::PgConnection::connect(&std::env::var("DATABASE_TEST_URL").unwrap())
        .await
        .unwrap();
    let mut lock = conn.begin().await.unwrap();
    lock.execute("LOCK TABLE epg_programdata IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    assert!(timeout_gateway
        .read(peer, admin_headers(&fixture), Vec::new())
        .await
        .is_err());
    assert_eq!(timeout_gateway.physical_operations(), 1);
    lock.rollback().await.unwrap();
    drained(&timeout_gateway).await;
    assert!(timeout_gateway.shutdown(Duration::from_secs(1)).await);
    let (_, frames_gateway, _) = setup(GridReadLimits::default()).await;
    let mut frames = frames_gateway
        .read(peer, admin_headers(&fixture), Vec::new())
        .await
        .unwrap();
    let held = frames.pop().unwrap();
    let final_clone = held.clone();
    drop(frames);
    drop(held);
    assert_eq!(frames_gateway.physical_operations(), 0);
    let charged = GridReadLimits::default().memory_bytes - frames_gateway.available_memory_bytes();
    assert!(
        charged > 64 * 1024 && charged < 96 * 1024 * 1024,
        "actual capacity charge: {charged}"
    );
    assert!(frames_gateway.shutdown(Duration::from_secs(1)).await);
    assert_eq!(
        GridReadLimits::default().memory_bytes - frames_gateway.available_memory_bytes(),
        charged
    );
    drop(final_clone);
    assert_eq!(
        frames_gateway.available_memory_bytes(),
        GridReadLimits::default().memory_bytes
    );
    println!("GRID_SQL_ABORT_TIMEOUT_REJECT_SHUTDOWN_AND_FINAL_CLONE=PASS");
}

async fn admitted(gateway: &GridGateway, count: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while gateway.physical_operations() != count {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("admitted grid owners did not reach expected count");
}
async fn blocked_programmes(store: &Store, count: i64) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let waiting: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT p.id::bigint AS id,p.start_time%'").fetch_one(store.pool()).await.unwrap();
            if waiting == count { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.expect("grid SQL did not physically block at configured concurrency");
}
#[tokio::test]
#[ignore = "requires disposable original grid fixture and actual PostgreSQL"]
async fn ten_callers_queue_without_raising_active_work_or_memory_limits() {
    let (store, gateway, fixture) = setup(GridReadLimits::default()).await;
    let peer = "127.0.0.1:12345".parse().unwrap();
    let reference_frames = gateway
        .read(peer, admin_headers(&fixture), Vec::new())
        .await
        .unwrap();
    let expected: Vec<u8> = reference_frames
        .iter()
        .flat_map(|frame| frame.iter().copied())
        .collect();
    drop(reference_frames);
    let mut actor = sqlx::PgConnection::connect(&std::env::var("DATABASE_TEST_URL").unwrap())
        .await
        .unwrap();
    let mut lock = actor.begin().await.unwrap();
    lock.execute("LOCK TABLE epg_programdata IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let reader = gateway.clone();
        let auth = admin_headers(&fixture);
        tasks.push(tokio::spawn(async move {
            reader.read(peer, auth, Vec::new()).await
        }));
    }
    admitted(&gateway, 10).await;
    blocked_programmes(&store, 2).await;
    // The input-owner budget is separate from the unchanged two active jobs.
    assert!(gateway
        .read(peer, admin_headers(&fixture), Vec::new())
        .await
        .is_err());
    assert_eq!(gateway.available_memory_bytes(), 0);
    assert!(!gateway.shutdown(Duration::from_millis(20)).await);
    assert!(gateway
        .read(peer, admin_headers(&fixture), Vec::new())
        .await
        .is_err());
    lock.rollback().await.unwrap();
    for task in tasks {
        let frames = task.await.unwrap().unwrap();
        let body: Vec<u8> = frames
            .iter()
            .flat_map(|frame| frame.iter().copied())
            .collect();
        assert_eq!(body, expected);
    }
    drained(&gateway).await;
    assert_eq!(
        gateway.available_memory_bytes(),
        GridReadLimits::default().memory_bytes
    );
    assert!(gateway.shutdown(Duration::from_secs(1)).await);
    println!("GRID_TEN_QUEUED_TWO_ACTIVE_UNCHANGED_MEMORY_EXACT_ENTITIES=PASS");
}
#[tokio::test]
#[ignore = "requires disposable original grid fixture and actual PostgreSQL"]
async fn queued_abort_expiry_and_shutdown_never_start_late_sql() {
    let limits = GridReadLimits {
        concurrent_responses: 1,
        memory_bytes: 96 * 1024 * 1024,
        deadline: Duration::from_millis(150),
    };
    let (store, gateway, fixture) = setup(limits).await;
    let peer = "127.0.0.1:12345".parse().unwrap();
    let mut actor = sqlx::PgConnection::connect(&std::env::var("DATABASE_TEST_URL").unwrap())
        .await
        .unwrap();
    let mut lock = actor.begin().await.unwrap();
    lock.execute("LOCK TABLE epg_programdata IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let reader = gateway.clone();
    let auth = admin_headers(&fixture);
    let physical = tokio::spawn(async move { reader.read(peer, auth, Vec::new()).await });
    blocked_programmes(&store, 1).await;
    let reader = gateway.clone();
    let auth = admin_headers(&fixture);
    let canceled = tokio::spawn(async move { reader.read(peer, auth, Vec::new()).await });
    admitted(&gateway, 2).await;
    canceled.abort();
    let _ = canceled.await;
    admitted(&gateway, 1).await;
    assert!(gateway
        .read(peer, admin_headers(&fixture), Vec::new())
        .await
        .is_err());
    admitted(&gateway, 1).await;
    blocked_programmes(&store, 1).await;
    assert!(!gateway.shutdown(Duration::from_millis(20)).await);
    assert!(gateway
        .read(peer, admin_headers(&fixture), Vec::new())
        .await
        .is_err());
    lock.rollback().await.unwrap();
    assert!(physical.await.unwrap().is_err());
    drained(&gateway).await;
    assert_eq!(gateway.available_memory_bytes(), limits.memory_bytes);
    assert!(gateway.shutdown(Duration::from_secs(1)).await);
    println!("GRID_QUEUED_CANCEL_EXPIRE_NO_LATE_SQL_PHYSICAL_DRAIN=PASS");
}
