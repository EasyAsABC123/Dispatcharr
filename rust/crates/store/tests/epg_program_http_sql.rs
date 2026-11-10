//! Actual original JSON/HEAD programme reads and physical capacity ownership.
//! Original Dispatcharr authors retain full application credit.
use axum::extract::ConnectInfo;
use axum::http::HeaderMap;
use dispatcharr_store::{
    AuthConfig, AuthService, EpgProgramReadLimits, EpgProgramReads, Store, StoreError,
};
use serde_json::Value;
use sqlx::Acquire;
use std::{net::SocketAddr, time::Duration};

async fn drained(service: &EpgProgramReads) {
    tokio::time::timeout(Duration::from_secs(4), async {
        while service.physical_operations() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
#[ignore = "requires actual original EPG oracle and disposable PostgreSQL"]
async fn original_programme_json_http_and_physical_read_limits() {
    assert_eq!(
        std::env::var("EPG_READ_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value =
        serde_json::from_slice(&std::fs::read(std::env::var("EPG_READ_FIXTURE").unwrap()).unwrap())
            .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 6)
        .await
        .unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    );
    let service = EpgProgramReads::new(auth.clone(), EpgProgramReadLimits::default()).unwrap();
    let peer: SocketAddr = "127.0.0.1:12345".parse().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = service
        .clone()
        .router()
        .layer(axum::Extension(ConnectInfo(peer)));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let client = reqwest::Client::new();
    let mut compared = 0;
    for case in fixture["cases"].as_array().unwrap() {
        let path = case["path"].as_str().unwrap();
        if !path.starts_with("/api/epg/programs/") || case["headers"]["HTTP_ACCEPT"] == "text/html"
        {
            continue;
        }
        let mut request = client.request(
            case["method"].as_str().unwrap().parse().unwrap(),
            format!("http://{address}{path}"),
        );
        for (name, value) in case["headers"].as_object().unwrap() {
            if let Some(name) = name.strip_prefix("HTTP_") {
                request = request.header(name.replace('_', "-"), value.as_str().unwrap());
            }
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let bytes = response.bytes().await.unwrap();
        assert_eq!(
            status,
            case["expected"]["status"].as_u64().unwrap() as u16,
            "{path}, role{}",
            case["role"]
        );
        if case["method"] == "HEAD" {
            assert!(bytes.is_empty())
        } else {
            let actual: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(
                actual, case["expected"]["json"],
                "{path}, role{}",
                case["role"]
            );
        }
        compared += 1;
    }
    assert!(compared >= 30);
    println!("EPG_PROGRAM_HTTP_CASES={compared}");
    server.abort();
    let _ = server.await;
    assert!(service.shutdown(Duration::from_secs(3)).await);
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["path"] == "/api/epg/programs/" && c["role"] == "admin" && c["method"] == "GET")
        .unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(
        "authorization",
        case["headers"]["HTTP_AUTHORIZATION"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap(),
    );
    let limits = EpgProgramReadLimits {
        concurrent_responses: 1,
        ..EpgProgramReadLimits::default()
    };
    let retained = EpgProgramReads::new(auth.clone(), limits).unwrap();
    let bytes = retained
        .read(peer, headers.clone(), None, "http://localhost".into())
        .await
        .unwrap();
    let clone = bytes.clone();
    drop(bytes);
    assert_eq!(retained.physical_operations(), 1);
    assert!(matches!(
        retained
            .read(peer, headers.clone(), None, "http://localhost".into())
            .await,
        Err(StoreError::StatusBody(503, _))
    ));
    drop(clone);
    drained(&retained).await;
    assert!(retained.shutdown(Duration::from_secs(1)).await);
    let blocked = EpgProgramReads::new(auth.clone(), limits).unwrap();
    let mut conn = store.pool().acquire().await.unwrap();
    let mut lock = conn.begin().await.unwrap();
    sqlx::query("LOCK TABLE epg_programdata IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let task = {
        let blocked = blocked.clone();
        let headers = headers.clone();
        tokio::spawn(async move {
            blocked
                .read(peer, headers, None, "http://localhost".into())
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2),async {loop {
        let waiting:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT p.id::bigint AS id,p.start_time%'").fetch_one(store.pool()).await.unwrap();
        if waiting>0 {break}tokio::time::sleep(Duration::from_millis(10)).await;
    }}).await.unwrap();
    task.abort();
    let _ = task.await;
    assert_eq!(blocked.physical_operations(), 1);
    assert!(matches!(
        blocked
            .read(peer, headers.clone(), None, "http://localhost".into())
            .await,
        Err(StoreError::StatusBody(503, _))
    ));
    lock.rollback().await.unwrap();
    drop(conn);
    drained(&blocked).await;
    assert!(blocked.shutdown(Duration::from_secs(1)).await);
    let small = EpgProgramReads::new(
        auth.clone(),
        EpgProgramReadLimits {
            response_bytes: 128,
            ..limits
        },
    )
    .unwrap();
    assert!(matches!(
        small
            .read(peer, headers.clone(), None, "http://localhost".into())
            .await,
        Err(StoreError::StatusBody(503, _))
    ));
    drained(&small).await;
    let id = case["expected"]["json"][0]["id"].as_i64().unwrap();
    let old: Option<String> =
        sqlx::query_scalar("SELECT description FROM epg_programdata WHERE id=$1")
            .bind(id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    sqlx::query("UPDATE epg_programdata SET description=repeat('x',2000000) WHERE id=$1")
        .bind(id)
        .execute(store.pool())
        .await
        .unwrap();
    let guarded = EpgProgramReads::new(
        auth,
        EpgProgramReadLimits {
            row_bytes: 1024,
            ..limits
        },
    )
    .unwrap();
    let result = guarded
        .read(peer, headers, None, "http://localhost".into())
        .await;
    sqlx::query("UPDATE epg_programdata SET description=$2 WHERE id=$1")
        .bind(id)
        .bind(old)
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(result, Err(StoreError::StatusBody(503, _))));
    drained(&guarded).await;
    println!("EPG_PROGRAM_RESOURCE_SCENARIOS=4");
}
