//! Actual original current-programme parser/DB selection, no HTTP acceptance.
//! Original Dispatcharr authors retain full application credit.
use chrono::{DateTime, Utc};
use dispatcharr_store::{CurrentProgramSelection, Store, StoreError};
use serde_json::{json, Value};

#[tokio::test]
#[ignore = "requires actual original guide corpus and disposable PostgreSQL"]
async fn original_current_programme_db_selection() {
    assert_eq!(
        std::env::var("EPG_READ_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("GUIDE_READ_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let now = DateTime::parse_from_rfc3339(fixture["clock"].as_str().unwrap())
        .unwrap()
        .with_timezone(&Utc);
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 4)
        .await
        .unwrap();
    let mut compared = 0;
    for case in fixture["cases"].as_array().unwrap() {
        if case["path"] != "/api/epg/current-programs/"
            || case["role"] != "admin"
            || case["method"] != "POST"
            || !case["raw_body"].is_null()
            || case["content_type"] != "application/json"
        {
            continue;
        }
        let selection = CurrentProgramSelection::from_json(&case["body"]);
        let result = match selection {
            Ok(selection) => store
                .current_programmes_db(selection, now, 50000, 16 * 1024 * 1024)
                .await
                .map(|v| serde_json::from_slice::<Value>(&v).unwrap()),
            Err(error) => Err(error),
        };
        match result {
            Ok(value) => {
                assert_eq!(case["expected"]["status"], 200, "case{compared}");
                assert_eq!(
                    value, case["expected"]["json"],
                    "case{compared}, body{}",
                    case["body"]
                );
            }
            Err(StoreError::StatusBody(status, body)) => {
                if case["expected"].get("unhandled_exception").is_some() {
                    assert_eq!(status, 500, "case{compared}")
                } else {
                    assert_eq!(
                        json!(status),
                        case["expected"]["status"],
                        "case{compared}, body{}",
                        case["body"]
                    );
                    assert_eq!(body, case["expected"]["json"], "case{compared}");
                }
            }
            Err(error) => panic!("unexpected native error at{compared}: {error}"),
        }
        compared += 1;
    }
    assert!(compared >= 40);
    println!("CURRENT_PROGRAMME_DB_CASES={compared}");
}

#[tokio::test]
#[ignore = "requires original guide fixture and real PostgreSQL physical lock waits"]
async fn current_programme_response_and_cancelled_sql_retain_shared_admission() {
    use axum::http::HeaderMap;
    use dispatcharr_store::{AuthConfig, AuthService, EpgProgramReadLimits, EpgProgramReads};
    use sqlx::Acquire;
    use std::{net::SocketAddr, time::Duration};
    assert_eq!(
        std::env::var("EPG_READ_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("GUIDE_READ_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let now = DateTime::parse_from_rfc3339(fixture["clock"].as_str().unwrap())
        .unwrap()
        .with_timezone(&Utc);
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 6)
        .await
        .unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    );
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["path"] == "/api/epg/current-programs/" && case["role"] == "admin")
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
    let peer: SocketAddr = "127.0.0.1:12345".parse().unwrap();
    let limits = EpgProgramReadLimits {
        concurrent_responses: 1,
        ..EpgProgramReadLimits::default()
    };
    async fn drained(service: &EpgProgramReads) {
        tokio::time::timeout(Duration::from_secs(4), async {
            while service.physical_operations() != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
    let retained = EpgProgramReads::new(auth.clone(), limits).unwrap();
    let bytes = retained
        .read_current_db(peer, headers.clone(), json!({}), now)
        .await
        .unwrap();
    let clone = bytes.clone();
    drop(bytes);
    assert_eq!(retained.physical_operations(), 1);
    assert!(matches!(
        retained
            .read_current_db(peer, headers.clone(), json!({}), now)
            .await,
        Err(StoreError::StatusBody(503, _))
    ));
    drop(clone);
    drained(&retained).await;
    assert!(retained.shutdown(Duration::from_secs(1)).await);
    for logical_timeout in [false, true] {
        let service = EpgProgramReads::new(
            auth.clone(),
            EpgProgramReadLimits {
                deadline: if logical_timeout {
                    Duration::from_millis(100)
                } else {
                    Duration::from_secs(2)
                },
                ..limits
            },
        )
        .unwrap();
        let mut conn = store.pool().acquire().await.unwrap();
        let mut lock = conn.begin().await.unwrap();
        sqlx::query("LOCK TABLE epg_programdata IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *lock)
            .await
            .unwrap();
        if logical_timeout {
            assert!(matches!(
                service
                    .read_current_db(peer, headers.clone(), json!({}), now)
                    .await,
                Err(StoreError::StatusBody(503, _))
            ));
        } else {
            let task = {
                let service = service.clone();
                let headers = headers.clone();
                tokio::spawn(
                    async move { service.read_current_db(peer, headers, json!({}), now).await },
                )
            };
            tokio::time::timeout(Duration::from_secs(1),async {loop {
                let waiting:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT p.id::bigint AS id,p.epg_id%'").fetch_one(store.pool()).await.unwrap();
                if waiting>0 {break}tokio::time::sleep(Duration::from_millis(5)).await;
            }}).await.unwrap();
            task.abort();
            let _ = task.await;
        }
        assert_eq!(service.physical_operations(), 1);
        assert!(matches!(
            service
                .read_current_db(peer, headers.clone(), json!({}), now)
                .await,
            Err(StoreError::StatusBody(503, _))
        ));
        lock.rollback().await.unwrap();
        drop(conn);
        drained(&service).await;
        assert!(service.shutdown(Duration::from_secs(1)).await);
    }
    // Transaction/statement timeout restores defaults on its returned pool connection.
    let final_response = EpgProgramReads::new(auth, limits).unwrap();
    drop(
        final_response
            .read_current_db(peer, headers, json!({}), now)
            .await
            .unwrap(),
    );
    drained(&final_response).await;
    assert!(final_response.shutdown(Duration::from_secs(1)).await);
    println!("CURRENT_PROGRAMME_RESOURCE_SCENARIOS=3");
}

#[tokio::test]
#[ignore = "requires actual original guide view and outer HTTP error capture"]
async fn original_current_programme_http_with_pinned_clock() {
    use axum::extract::ConnectInfo;
    use dispatcharr_store::{AuthConfig, AuthService, EpgProgramReadLimits, EpgProgramReads};
    use sha2::{Digest, Sha256};
    use std::{net::SocketAddr, time::Duration};
    assert_eq!(
        std::env::var("EPG_READ_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("GUIDE_READ_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let now = DateTime::parse_from_rfc3339(fixture["clock"].as_str().unwrap())
        .unwrap()
        .with_timezone(&Utc);
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 6)
        .await
        .unwrap();
    let auth = AuthService::new(
        store,
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    );
    let service = EpgProgramReads::new(auth, EpgProgramReadLimits::default())
        .unwrap()
        .with_current_clock(move || now);
    let peer: SocketAddr = "127.0.0.1:12345".parse().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = service
        .clone()
        .current_router()
        .layer(axum::Extension(ConnectInfo(peer)));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let client = reqwest::Client::new();
    let mut compared = 0;
    for case in fixture["cases"].as_array().unwrap() {
        if case["path"] != "/api/epg/current-programs/" {
            continue;
        }
        let method = case["method"].as_str().unwrap();
        let body = case["raw_body"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| serde_json::to_string(&case["body"]).unwrap());
        let mut request = client
            .request(
                method.parse().unwrap(),
                format!("http://{address}{}", case["path"].as_str().unwrap()),
            )
            .body(body)
            .header("content-type", case["content_type"].as_str().unwrap());
        for (name, value) in case["headers"].as_object().unwrap() {
            if let Some(name) = name.strip_prefix("HTTP_") {
                request = request.header(name.replace('_', "-"), value.as_str().unwrap());
            }
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let bytes = response.bytes().await.unwrap();
        let expected = case["expected"]
            .get("actual_http")
            .unwrap_or(&case["expected"]);
        assert_eq!(
            json!(status),
            expected["status"],
            "case{compared}, role{}, body{}",
            case["role"],
            case["body"]
        );
        assert_eq!(
            headers.get("content-type").unwrap().to_str().unwrap(),
            expected["content_type"].as_str().unwrap(),
            "case{compared}"
        );
        if method == "HEAD" {
            assert!(bytes.is_empty());
            assert_eq!(
                headers.get("content-length").unwrap().to_str().unwrap(),
                expected["body_bytes"].to_string()
            );
        } else {
            assert_eq!(
                format!("{:x}", Sha256::digest(&bytes)),
                expected["body_sha256"].as_str().unwrap(),
                "case{compared}, role{}, body{}, actual{}",
                case["role"],
                case["body"],
                String::from_utf8_lossy(&bytes)
            );
        }
        compared += 1;
    }
    assert_eq!(compared, 64);
    server.abort();
    let _ = server.await;
    assert!(service.shutdown(Duration::from_secs(3)).await);
    println!("CURRENT_PROGRAMME_HTTP_CASES={compared}");
}
