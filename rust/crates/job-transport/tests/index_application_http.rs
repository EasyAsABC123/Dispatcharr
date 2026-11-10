//! Actual HTTP Parsing→Redis publication→native intake→SQL index→programme.
//! Original Dispatcharr authors retain all application/algorithm credit.
use dispatcharr_job_transport::{
    index_application::{IndexApplication, Options},
    transport::{Config, Jobs},
};
use dispatcharr_store::{AuthConfig, AuthService, EpgProgramReadLimits, EpgProgramReads, Store};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
#[tokio::test]
#[ignore = "requires copied Original HTTP current-programme fixture and isolated Redis8/9"]
async fn actual_http_prepared_index_application_then_background_programme() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let database = std::env::var("POSTGRES_DB").unwrap();
    assert!(database.starts_with("rust_probe_backup_"));
    let data: Value =
        serde_json::from_slice(&std::fs::read(std::env::var("EPG_PATH_FIXTURE").unwrap()).unwrap())
            .unwrap();
    let case = &data["current_http_driver"];
    let id = case["source_id"].as_i64().unwrap();
    let epg = case["epg_entry_id"].as_i64().unwrap();
    let now: chrono::DateTime<chrono::Utc> = case["clock"].as_str().unwrap().parse().unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 1)
        .await
        .unwrap();
    let owner: String = sqlx::query_scalar("SELECT name FROM epg_epgsource WHERE id=$1")
        .bind(id)
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(owner, "rust-current-http-driver-owned");
    let redis0 = std::env::var("REDIS_TEST_URL").unwrap();
    assert!(redis0.ends_with("/0"));
    let prefix = redis0.strip_suffix("/0").unwrap();
    let jobs = Jobs::connect_index(
        &format!("{prefix}/8"),
        &format!("{prefix}/9"),
        Config::default(),
    )
    .await
    .unwrap();
    let index_app = IndexApplication::with_clock(
        store.clone(),
        Options {
            broker: format!("{prefix}/8"),
            backend: format!("{prefix}/9"),
            cache: redis0.clone(),
            source_cwd: "/app".into(),
            media_root: "/app/media".into(),
            cache_dir: "/evidence/native-http-index-cache".into(),
        },
        Arc::new(move || now),
    )
    .await
    .unwrap();
    let service = EpgProgramReads::new(
        AuthService::new(
            store.clone(),
            AuthConfig::new("playback-fixture-only".into()).unwrap(),
        ),
        EpgProgramReadLimits::default(),
    )
    .unwrap()
    .with_current_clock(move || now)
    .with_current_file_lookup(index_app.lookup(), "/app/media/cached_epg".into())
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer: std::net::SocketAddr = "127.0.0.1:12345".parse().unwrap();
    let router = service
        .clone()
        .current_router()
        .layer(axum::Extension(axum::extract::ConnectInfo(peer)));
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let clock = chrono::Utc::now().timestamp();
    let token = jsonwebtoken::encode(&jsonwebtoken::Header::default(), &json!({"user_id":case["user_id"],"token_type":"access","iat":clock,"exp":clock+120,"jti":"owned-http-driver"}), &jsonwebtoken::EncodingKey::from_secret(b"playback-fixture-only")).unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let post = || {
        client
            .post(format!("http://{address}/api/epg/current-programs/"))
            .bearer_auth(&token)
            .header("content-type", "application/json")
            .body(json!({"epg_data_ids":[epg]}).to_string())
    };
    let first = post().send().await.unwrap();
    assert_eq!(
        first.status().as_u16(),
        case["first"]["status"].as_u64().unwrap() as u16
    );
    let first: Value = serde_json::from_slice(&first.bytes().await.unwrap()).unwrap();
    assert_eq!(first, case["first"]["body"]);
    let mut redis = redis::Client::open(format!("{prefix}/8"))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let queued: Vec<String> = redis::cmd("LRANGE")
        .arg("celery")
        .arg(0)
        .arg(1)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(queued.len(), 1);
    let envelope: Value = serde_json::from_str(&queued[0]).unwrap();
    let task_id: uuid::Uuid = envelope["headers"]["id"].as_str().unwrap().parse().unwrap();
    assert_eq!(
        envelope["headers"]["task"],
        case["initial_envelope"]["headers"]["task"]
    );
    // Preparing the service must not consume the published head before all
    // application dependencies are ready and start() explicitly enables intake.
    assert!(jobs
        .read_index(task_id, Arc::new(()))
        .await
        .unwrap()
        .is_none());
    index_app.start(Arc::new(())).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if jobs
            .read_index(task_id, Arc::new(()))
            .await
            .unwrap()
            .is_some()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "background driver completion deadline"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(index_app.last_execution_error().is_none());
    let second = post().send().await.unwrap();
    assert_eq!(
        second.status().as_u16(),
        case["second"]["status"].as_u64().unwrap() as u16
    );
    let second: Value = serde_json::from_slice(&second.bytes().await.unwrap()).unwrap();
    assert_eq!(second, case["second"]["body"]);
    let _ = stop.send(());
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    assert!(service.shutdown(Duration::from_secs(3)).await);
    assert!(index_app.shutdown(Duration::from_secs(3)).await);
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    assert_eq!(index_app.physical_lookup_jobs(), 0);
    assert_eq!(index_app.retained_lookup_memory_bytes(), 0);
    let unacked: i64 = redis::cmd("HLEN")
        .arg("unacked")
        .query_async(&mut redis)
        .await
        .unwrap();
    let queue: i64 = redis::cmd("LLEN")
        .arg("celery")
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!((unacked, queue), (0, 0));
    let mut cache = redis::Client::open(redis0)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let file_lock: bool = redis::cmd("EXISTS")
        .arg(format!("task_lock_epg_source_file_{id}"))
        .query_async(&mut cache)
        .await
        .unwrap();
    assert!(!file_lock);
    store.pool().close().await;
}
