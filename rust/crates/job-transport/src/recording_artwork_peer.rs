//! Owned copied-DB artwork peer. Same normal Store/service/worker execution.
use crate::{
    recording_artwork_protocol::SeriesArtworkChild,
    recording_artwork_worker::{Run, Worker},
    recording_local_artwork_service::Service,
    transport::Config,
};
use dispatcharr_store::{
    recording_local_artwork::{Domain, LocalArtworkStore},
    Store,
};
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
        assert!(raw.len() + n <= 16384);
        raw.extend_from_slice(&part[..n]);
        input.consume(n);
        if raw.ends_with(b"\n") {
            break;
        }
    }
    serde_json::from_slice(&raw).unwrap()
}
fn endpoint(url: &str, database: i64) {
    let client = redis::Client::open(url).unwrap();
    let info = client.get_connection_info();
    assert_eq!(info.redis.db, database);
    assert!(
        matches!(&info.addr,redis::ConnectionAddr::Tcp(h,p)if matches!(h.as_str(),"localhost"|"127.0.0.1")&&(*p==6379||(61000..=65000).contains(p)))
    );
}

fn output(value: Value) {
    println!("NATIVE_LOCAL_ARTWORK_JSON={value}");
    std::io::stdout().flush().unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Owned copied DB+Redis8/9+Original producer; no default consumer"]
async fn actual_local_artwork_peer() {
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
    let events_url = cfg["events_url"].as_str().unwrap();
    endpoint(broker, 8);
    endpoint(backend, 9);
    endpoint(events_url, 0);
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
    use base64::Engine;
    let decode = |key: &str| {
        base64::engine::general_purpose::STANDARD
            .decode(cfg[key].as_str().unwrap())
            .unwrap()
    };
    let mut refresh_frame: Value = serde_json::from_slice(&decode("refresh_frame_base64")).unwrap();
    assert_eq!(
        refresh_frame["properties"]["delivery_info"],
        json!({"exchange":"","routing_key":"rust-owned-parented-refresh"})
    );
    refresh_frame["properties"]["delivery_info"]["routing_key"] = json!("celery");
    let refresh =
        crate::refresh_protocol::parse(&serde_json::to_vec(&refresh_frame).unwrap()).unwrap();
    let parent =
        crate::transport::parse_parented_series_fixture(&decode("series_frame_base64")).unwrap();
    let refresh_cap = crate::recording_artwork::RefreshSeriesChild::from_completed_refresh(
        &refresh,
        &decode("refresh_backend_base64"),
        &parent,
    )
    .unwrap();
    let child =
        crate::transport::parse_local_artwork_fixture(&decode("artwork_frame_base64")).unwrap();
    let cap = SeriesArtworkChild::from_completed_series(
        refresh_cap,
        &parent,
        &decode("series_backend_base64"),
        &child,
    )
    .unwrap();
    let events = Arc::new(
        dispatcharr_realtime::RealtimeBus::connect(events_url, Default::default())
            .await
            .unwrap(),
    );
    let connections = events.available_connections();
    let bytes = events.available_publish_bytes();
    let persistence = Arc::new(LocalArtworkStore::new(
        store.pool().clone(),
        Domain::captured_local("/app/media/rust-owned-parented-artwork/logo.png".into()).unwrap(),
    ));
    let service = Arc::new(Service::new(persistence, events.clone()));
    let worker = Worker::connect_fixture(
        broker,
        backend,
        &database,
        Config::default(),
        service.clone(),
        cap,
    )
    .await
    .unwrap();
    output(
        json!({"prepared":true,"route":"rust-owned-parented-artwork","task_id":child.task_id.to_string(),"root":child.root.to_string(),"parent":child.parent.to_string(),"captured_ETA":child.eta.to_rfc3339(),"provider_fallback":false}),
    );
    let mut pending = None;
    let mut weak = None;
    loop {
        let command = read(&mut input);
        match command["action"].as_str().unwrap() {
            "run" => {
                let caller = Arc::new(());
                let reference = Arc::downgrade(&caller);
                let began = chrono::Utc::now();
                let result = worker.run_once(caller).await;
                let typed = match &result {
                    Ok(Run::Completed { task_id, changed }) => {
                        json!({"state":"Completed","task_id":task_id.to_string(),"changed":changed})
                    }
                    Ok(Run::AlreadySucceeded { task_id, changed }) => {
                        json!({"state":"AlreadySucceeded","task_id":task_id.to_string(),"changed":changed})
                    }
                    _ => Value::Null,
                };
                output(
                    json!({"action":"run","began_utc":began.to_rfc3339(),"finished_utc":chrono::Utc::now().to_rfc3339(),"ok":result.is_ok(),"result":format!("{result:?}"),"typed_result":typed,"caller_released":reference.upgrade().is_none(),"available_slots":worker.available_slots()}),
                );
            }
            "start" => {
                assert!(pending.is_none());
                let caller = Arc::new(());
                weak = Some(Arc::downgrade(&caller));
                let owned = worker.clone();
                pending = Some(tokio::spawn(async move { owned.run_once(caller).await }));
                output(json!({"started":true,"utc":chrono::Utc::now().to_rfc3339()}));
            }
            "poll" => output(
                json!({"caller_pending":pending.as_ref().is_some_and(|h|!h.is_finished()),"caller_released":weak.as_ref().is_some_and(|w|w.upgrade().is_none()),"available_slots":worker.available_slots(),"event_connections":events.available_connections(),"event_publish_bytes":events.available_publish_bytes()}),
            ),
            "abort" => {
                pending.as_ref().unwrap().abort();
                output(json!({"abort_requested":true,"utc":chrono::Utc::now().to_rfc3339()}));
            }
            "wait" => {
                let result =
                    tokio::time::timeout(Duration::from_secs(35), pending.take().unwrap()).await;
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
                assert_eq!(events.available_connections(), connections);
                assert_eq!(events.available_publish_bytes(), bytes);
                events.stop_accepting();
                drop(worker);
                drop(service);
                drop(events);
                store.pool().close().await;
                output(json!({"closed":true}));
                break;
            }
            _ => panic!("finite peer action only"),
        }
    }
}
