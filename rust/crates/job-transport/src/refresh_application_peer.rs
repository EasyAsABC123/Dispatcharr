//! Disposable copied-DB actual TCP application peer. Never invoked at startup.
use super::*;
use dispatcharr_contracts::UpdateEventError;
use sha2::Digest;
use std::io::{self, BufRead, Read};
struct Sink(Arc<std::sync::Mutex<Vec<Value>>>);
impl UpdateEventSink for Sink {
    fn send<'a>(
        &'a self,
        group: &'a str,
        value: Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            let mut records = self.0.lock().unwrap();
            if records.len() >= 64 || serde_json::to_vec(&value).unwrap().len() > 65536 {
                return Err(UpdateEventError::Capacity);
            }
            records.push(json!({"group":group,"message":value}));
            Ok(())
        })
    }
}
#[tokio::test]
#[ignore = "Actual owned copied DB/authenticated HTTP application import + drain"]
async fn native_refresh_application_http_peer() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let database = std::env::var("POSTGRES_DB").unwrap();
    assert!(
        database.starts_with("rust_probe_backup_jobs_")
            && database.len() <= 63
            && database
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    );
    let mut raw = String::new();
    io::stdin().lock().take(65537).read_line(&mut raw).unwrap();
    assert!(raw.len() <= 65536);
    let input: Value = serde_json::from_str(&raw).unwrap();
    let db_url = format!("postgres://dispatch:playback-fixture-only@127.0.0.1:5432/{database}");
    assert_eq!(input["database_url"], db_url);
    for (key, db) in [("broker_url", 8), ("result_url", 9)] {
        let endpoint = redis::Client::open(input[key].as_str().unwrap()).unwrap();
        let info = endpoint.get_connection_info();
        assert_eq!(info.redis.db, db);
        assert_eq!(
            info.redis.password.as_deref(),
            Some("playback-fixture-only")
        );
        assert!(
            matches!(&info.addr,redis::ConnectionAddr::Tcp(host,port) if matches!(host.as_str(),"localhost"|"127.0.0.1") && *port==6379)
        );
    }
    let store = Store::connect(&db_url, 2).await.unwrap();
    let auth = AuthService::new(
        store.clone(),
        dispatcharr_store::AuthConfig::new("playback-fixture-only".into()).unwrap(),
    );
    let publications = Arc::new(std::sync::Mutex::new(Vec::new()));
    let service = RefreshApplication::connect(
        store.clone(),
        auth,
        Arc::new(Sink(publications.clone())),
        RefreshApplicationOptions {
            broker: input["broker_url"].as_str().unwrap().into(),
            backend: input["result_url"].as_str().unwrap().into(),
            cache: "redis://:playback-fixture-only@127.0.0.1:6379/0".into(),
            cache_dir: "/evidence/application-refresh-cache".into(),
        },
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let router = service.clone().router();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    let client = reqwest::Client::new();
    let mut records = Vec::new();
    for row in input["requests"].as_array().unwrap() {
        let mut request = client
            .request(
                row["method"].as_str().unwrap().parse().unwrap(),
                format!("http://{address}/api/epg/import/"),
            )
            .header("accept", "application/json")
            .header("content-type", "application/json");
        if let Some(token) = row["token"].as_str() {
            request = request.bearer_auth(token);
        }
        let response = request
            .body(row["raw"].as_str().unwrap().to_owned())
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let mime = response
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let bytes = response.bytes().await.unwrap();
        assert!(bytes.len() <= 65536);
        records.push(json!({"name":row["name"],"status":status,"body":serde_json::from_slice::<Value>(&bytes).ok(),"bytes":bytes.len(),"body_sha256":format!("{:x}",sha2::Sha256::digest(&bytes)),"mime":mime}));
    }
    let result_count = input["expected_results"].as_u64().unwrap();
    let mut result = redis::Client::open(input["result_url"].as_str().unwrap())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let keys: Vec<String> = redis::cmd("KEYS")
                .arg("celery-tasks:celery-task-meta-*")
                .query_async(&mut result)
                .await
                .unwrap();
            if keys.len() as u64 == result_count {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    // Hold a real PostgreSQL reply inside the supervised HTTP publication task.
    // Dropping its HTTP client must not drop request admission or invent202.
    let control = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await
        .unwrap();
    let mut transaction = control.begin().await.unwrap();
    sqlx::query("LOCK TABLE epg_epgsource IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *transaction)
        .await
        .unwrap();
    let http_client = client.clone();
    let token = input["control_token"].as_str().unwrap().to_owned();
    let source_id = input["control_source_id"]
        .as_i64()
        .filter(|v| *v > 0)
        .unwrap();
    let blocked_request = tokio::spawn(async move {
        http_client
            .post(format!("http://{address}/api/epg/import/"))
            .bearer_auth(token)
            .header("content-type", "application/json")
            .body(json!({"id":source_id}).to_string())
            .send()
            .await
    });
    tokio::time::timeout(Duration::from_secs(3),async {
        loop {
            sqlx::query("SELECT pg_stat_clear_snapshot()").execute(&mut *transaction).await.unwrap();
            let waiting:i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT EXISTS(SELECT 1 FROM epg_epgsource WHERE id=$1%'").fetch_one(&mut *transaction).await.unwrap();
            if waiting==1 {break}
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.unwrap();
    assert_eq!(service.inner.http.available_permits(), 3);
    blocked_request.abort();
    let _ = blocked_request.await;
    assert!(!service.shutdown(Duration::from_millis(20)).await);
    assert_eq!(service.inner.http.available_permits(), 3);
    transaction.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while service.inner.http.available_permits() != 4 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    control.close().await;
    let state = service.last_intake_error();
    let drained = service.shutdown(Duration::from_secs(10)).await;
    assert!(drained);
    stop.send(()).unwrap();
    server.await.unwrap();
    if let Some(denied_broker) = input["denied_broker_url"].as_str() {
        let endpoint = redis::Client::open(denied_broker).unwrap();
        let info = endpoint.get_connection_info();
        assert_eq!(info.redis.db, 8);
        assert_eq!(info.redis.username.as_deref(), Some("rust_import_denied"));
        assert_eq!(
            info.redis.password.as_deref(),
            Some("playback-fixture-only")
        );
        assert!(
            matches!(&info.addr,redis::ConnectionAddr::Tcp(host,port) if matches!(host.as_str(),"localhost"|"127.0.0.1") && *port==6379)
        );
        let auth = AuthService::new(
            store.clone(),
            dispatcharr_store::AuthConfig::new("playback-fixture-only".into()).unwrap(),
        );
        let denied = RefreshApplication::connect(
            store.clone(),
            auth,
            Arc::new(Sink(publications.clone())),
            RefreshApplicationOptions {
                broker: denied_broker.into(),
                backend: input["result_url"].as_str().unwrap().into(),
                cache: "redis://:playback-fixture-only@127.0.0.1:6379/0".into(),
                cache_dir: "/evidence/application-refresh-cache".into(),
            },
        )
        .await
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let router = denied.clone().router();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
        });
        let response = client
            .post(format!("http://{address}/api/epg/import/"))
            .bearer_auth(input["control_token"].as_str().unwrap())
            .header("content-type", "application/json")
            .body(json!({"id":source_id}).to_string())
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let mime = response
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let bytes = response.bytes().await.unwrap();
        records.push(json!({"name":"broker-lpush-denied","status":status,"body":serde_json::from_slice::<Value>(&bytes).ok(),"bytes":bytes.len(),"body_sha256":format!("{:x}",sha2::Sha256::digest(&bytes)),"mime":mime}));
        assert!(denied.shutdown(Duration::from_secs(5)).await);
        stop.send(()).unwrap();
        server.await.unwrap();
    }
    store.pool().close().await;
    println!(
        "NATIVE_REFRESH_APPLICATION_JSON={}",
        json!({"records":records,"drained":drained,"driver_state":format!("{state:?}"),"normal_results":result_count,"held_sql_caller_abort":true,"active_shutdown_reported_incomplete":true,"aborted_http_response_observed":false,"publications":publications.lock().unwrap().clone()})
    );
}
