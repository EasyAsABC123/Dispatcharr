//! UNREGISTERED QA peer text. Disposable copied SQL/Redis only. Same normal
//! configured worker/service; explicit fixed parent-route fixture control.
use crate::{
    configured_recording_worker::{Assembly, Family, Worker},
    recording_artwork::Publisher,
    transport::{Config, Jobs},
};
use dispatcharr_coordination::{TaskLockConfig, TaskLocks};
use dispatcharr_store::Store;
use serde_json::{json, Value};
use std::{
    io::{BufRead, Write},
    sync::Arc,
    time::Duration,
};
fn read(input: &mut impl BufRead) -> Value {
    let mut raw = Vec::new();
    loop {
        let part = input.fill_buf().unwrap();
        assert!(!part.is_empty());
        let n = part
            .iter()
            .position(|v| *v == b'\n')
            .map_or(part.len(), |n| n + 1);
        assert!(raw.len() + n <= 8192);
        raw.extend_from_slice(&part[..n]);
        input.consume(n);
        if raw.ends_with(b"\n") {
            break;
        }
    }
    serde_json::from_slice(&raw).unwrap()
}
fn output(value: Value) {
    println!("NATIVE_PARENTED_SERIES_JSON={value}");
    std::io::stdout().flush().unwrap();
}
fn endpoint(url: &str, database: i64) {
    let client = redis::Client::open(url).unwrap();
    let info = client.get_connection_info();
    assert_eq!(info.redis.db, database);
    assert!(
        matches!(&info.addr,redis::ConnectionAddr::Tcp(h,p)if matches!(h.as_str(),"localhost"|"127.0.0.1")&&(*p==6379||(61000..=65000).contains(p)))
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn actual_parented_series_peer() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let database = std::env::var("POSTGRES_DB").unwrap();
    assert!(database.starts_with("rust_probe_backup_jobs_"));
    let stdin = std::io::stdin();
    let mut input = stdin.lock();
    let cfg = read(&mut input);
    let broker = cfg["broker_url"].as_str().unwrap();
    let backend = cfg["result_url"].as_str().unwrap();
    let cache = cfg["cache_url"].as_str().unwrap();
    endpoint(broker, 8);
    endpoint(backend, 9);
    endpoint(cache, 0);
    for name in ["events_url", "locks_url"] {
        if let Some(url) = cfg[name].as_str() {
            endpoint(url, 0);
        }
    }
    let database_url = cfg["database_url"].as_str().unwrap();
    let options: sqlx::postgres::PgConnectOptions = database_url.parse().unwrap();
    assert_eq!(options.get_database(), Some(database.as_str()));
    assert!(matches!(options.get_host(), "localhost" | "127.0.0.1"));
    assert!(options.get_port() == 5432 || (61000..=65000).contains(&options.get_port()));
    assert_eq!(options.get_username(), "dispatch");
    let store = Store::connect(database_url, 1).await.unwrap();
    let actual: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(actual, database);
    let owned: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM dispatcharr_channels_channel WHERE name='Owned Parented Channel'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(owned, 1);
    let jobs = Jobs::connect_fixture(broker, backend, &database, Config::default())
        .await
        .unwrap();
    let publisher = Arc::new(
        Publisher::connect_parented_fixture(broker, backend, &database, Config::default())
            .await
            .unwrap(),
    );
    let events = Arc::new(
        dispatcharr_realtime::RealtimeBus::connect(
            cfg["events_url"].as_str().unwrap_or(cache),
            Default::default(),
        )
        .await
        .unwrap(),
    );
    let initial_connections = events.available_connections();
    let initial_publish_bytes = events.available_publish_bytes();
    let locks = TaskLocks::connect(
        cfg["locks_url"].as_str().unwrap_or(cache),
        TaskLockConfig::default(),
    )
    .await
    .unwrap();
    use base64::Engine;
    let decode = |name: &str| {
        base64::engine::general_purpose::STANDARD
            .decode(cfg[name].as_str().unwrap())
            .unwrap()
    };
    let mut refresh_frame: serde_json::Value =
        serde_json::from_slice(&decode("refresh_frame_base64")).unwrap();
    assert_eq!(
        refresh_frame["properties"]["delivery_info"],
        json!({"exchange":"","routing_key":"rust-owned-parented-refresh"})
    );
    refresh_frame["properties"]["delivery_info"]["routing_key"] = json!("celery");
    let refresh =
        crate::refresh_protocol::parse(&serde_json::to_vec(&refresh_frame).unwrap()).unwrap();
    let message =
        crate::transport::parse_parented_series_fixture(&decode("series_frame_base64")).unwrap();
    let cap = crate::recording_artwork::RefreshSeriesChild::from_completed_refresh(
        &refresh,
        &decode("refresh_backend_base64"),
        &message,
    )
    .unwrap();
    let worker = Worker::new_parented_series_fixture(
        Assembly {
            jobs: jobs.clone(),
            locks,
            store: store.clone(),
            cache: cache.into(),
            artwork: publisher.clone(),
            events: events.clone(),
            family: Family::Series,
        },
        cap,
    )
    .unwrap();
    let mut pending = None;
    let mut weak = None;
    output(
        json!({"prepared":true,"parent_route":"rust-owned-parented-series","child_route":"rust-owned-parented-artwork","children_consumed":false}),
    );
    loop {
        let command = read(&mut input);
        match command["action"].as_str().unwrap() {
            "run" => {
                let caller = Arc::new(());
                let reference = Arc::downgrade(&caller);
                let began = chrono::Utc::now();
                let result = worker.run_once(caller).await;
                output(
                    json!({"action":"run","began_utc":began.to_rfc3339(),"finished_utc":chrono::Utc::now().to_rfc3339(),"ok":result.is_ok(),"result":format!("{result:?}"),"caller_released":reference.upgrade().is_none(),"available_slots":worker.available_slots()}),
                );
            }
            "start" => {
                assert!(pending.is_none());
                let caller = Arc::new(());
                weak = Some(Arc::downgrade(&caller));
                let worker = worker.clone();
                pending = Some(tokio::spawn(async move { worker.run_once(caller).await }));
                output(json!({"started":true,"utc":chrono::Utc::now().to_rfc3339()}));
            }
            "poll" => output(
                json!({"caller_pending":pending.as_ref().is_some_and(|h|!h.is_finished()),"caller_released":weak.as_ref().is_some_and(|w|w.upgrade().is_none()),"available_slots":worker.available_slots(),"event_connections":events.available_connections(),"event_publish_bytes":events.available_publish_bytes()}),
            ),
            "abort" => {
                let handle = pending.as_ref().unwrap();
                handle.abort();
                output(json!({"abort_requested":true,"utc":chrono::Utc::now().to_rfc3339()}));
            }
            "wait" => {
                let handle = pending.take().unwrap();
                let result = tokio::time::timeout(Duration::from_secs(35), handle).await;
                output(
                    json!({"wait":format!("{result:?}"),"utc":chrono::Utc::now().to_rfc3339(),"available_slots":worker.available_slots()}),
                );
            }
            "drain" => {
                let began = chrono::Utc::now();
                let result = worker.shutdown(Duration::from_secs(10)).await;
                output(
                    json!({"drained":result.is_ok(),"result":format!("{result:?}"),"began_utc":began.to_rfc3339(),"finished_utc":chrono::Utc::now().to_rfc3339(),"caller_released":weak.as_ref().is_none_or(|w|w.upgrade().is_none()),"event_connections":events.available_connections(),"event_publish_bytes":events.available_publish_bytes()}),
                );
            }
            "close" => {
                assert!(pending.is_none());
                worker.shutdown(Duration::from_secs(10)).await.unwrap();
                assert_eq!(events.available_connections(), initial_connections);
                assert_eq!(events.available_publish_bytes(), initial_publish_bytes);
                publisher.shutdown(Duration::from_secs(10)).await.unwrap();
                jobs.shutdown(Duration::from_secs(10)).await.unwrap();
                events.stop_accepting();
                drop(worker);
                drop(events);
                drop(publisher);
                store.pool().close().await;
                output(json!({"closed":true}));
                break;
            }
            _ => panic!("finite peer command only"),
        }
    }
}
