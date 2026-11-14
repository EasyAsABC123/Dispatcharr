//! Inert interactive physical peer draft. No task starts on import or test list.
//! Same production Dvr paths; commands only observe/drop this actual observer.
use crate::recording_dvr::{Assembly, Dvr, Run};
use dispatcharr_store::Store;
use serde_json::{json, Value};
use std::{
    io::{BufRead, Write},
    sync::Arc,
    time::{Duration, Instant},
};
const MODES: [&str; 7] = [
    "cache-preack",
    "sql-preack-ready",
    "sql-update-error-ready",
    "capture-observer-loss",
    "channels-reply",
    "result-reply",
    "preexisting-active",
];
fn input(stream: &mut impl BufRead) -> Value {
    let mut bytes = Vec::new();
    loop {
        let part = stream.fill_buf().expect("owned control input");
        assert!(!part.is_empty(), "owned command EOF is a negative");
        let n = part
            .iter()
            .position(|b| *b == b'\n')
            .map_or(part.len(), |n| n + 1);
        assert!(bytes.len() + n <= 16384);
        bytes.extend_from_slice(&part[..n]);
        stream.consume(n);
        if bytes.ends_with(b"\n") {
            break;
        }
    }
    serde_json::from_slice(&bytes).expect("finite command JSON")
}
fn response(value: &Value) {
    println!("NATIVE_DVR_PHYSICAL_JSON={value}");
    std::io::stdout().flush().expect("owned response flush");
}
fn persist(report: &Value) {
    let bytes = serde_json::to_vec_pretty(report).expect("finite physical JSON");
    assert!(bytes.len() <= 2 * 1024 * 1024);
    let path = "/evidence/native-dvr-physical-private.json.pending";
    std::fs::write(path, bytes).expect("owned physical capture");
    std::fs::rename(path, "/evidence/native-dvr-physical-private.json")
        .expect("physical receipt publication");
}
fn endpoint(raw: &str, database: i64) {
    let client = redis::Client::open(raw).expect("owned fixture Redis URL");
    let info = client.get_connection_info();
    assert_eq!(info.redis.db, database);
    assert!(
        matches!(&info.addr,redis::ConnectionAddr::Tcp(host,port)if matches!(host.as_str(),"localhost"|"127.0.0.1")&&matches!(port,6379|62131|62132|62133))
    );
}
fn outcome(result: &Result<Run, crate::recording_dvr::Error>) -> Value {
    match result {
        Ok(Run::Empty) => json!({"kind":"Empty"}),
        Ok(Run::UnsupportedHead) => json!({"kind":"UnsupportedHead"}),
        Ok(Run::Completed {
            task,
            metadata_written,
            bytes_written,
            file_size,
            processes,
            active_observations,
        }) => {
            json!({"kind":"Completed","task":task,"metadata_written":metadata_written,"bytes_written":bytes_written,"file_size":file_size,"processes":processes,"active_observations":active_observations})
        }
        Err(error) => json!({"kind":"Operational","diagnostic":format!("{error:?}")}),
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Owned copied DB/real Original model+artwork+Beat and exact fixed physical relay gate; explicit Root admission only"]
async fn actual_retained_dvr_gate_peer() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    assert_eq!(
        std::env::var("DVR_NATIVE_MODE").as_deref(),
        Ok("physical-once")
    );
    let stdin = std::io::stdin();
    let mut stream = stdin.lock();
    let config = input(&mut stream);
    let mode = config["mode"].as_str().expect("fixed mode");
    assert!(MODES.contains(&mode));
    let database = std::env::var("POSTGRES_DB").expect("copied DB");
    assert!(database.starts_with("rust_probe_backup_jobs_") && database.len() == 33);
    let database_url = config["database_url"].as_str().expect("SQL URL");
    let sql: sqlx::postgres::PgConnectOptions = database_url.parse().expect("owned SQL");
    assert_eq!(sql.get_database(), Some(database.as_str()));
    assert!(matches!(sql.get_host(), "localhost" | "127.0.0.1"));
    assert!(matches!(sql.get_port(), 5432 | 62130));
    assert_eq!(sql.get_username(), "dispatch");
    let broker = config["broker_url"].as_str().unwrap();
    let result = config["result_url"].as_str().unwrap();
    let cache = config["cache_url"].as_str().unwrap();
    let events_url = config["events_url"].as_str().unwrap();
    endpoint(broker, 8);
    endpoint(result, 9);
    endpoint(cache, 0);
    endpoint(events_url, 0);
    let store = Store::connect(database_url, 1)
        .await
        .expect("actual same production Store");
    let actual: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(store.pool())
        .await
        .expect("actual copied DB identity");
    assert_eq!(actual, database);
    let events = Arc::new(
        dispatcharr_realtime::RealtimeBus::connect(events_url, Default::default())
            .await
            .expect("real retained Channels sink"),
    );
    let event_connections_before = events.available_connections();
    let event_bytes_before = events.available_publish_bytes();
    let dvr = Arc::new(
        Dvr::connect(
            Assembly {
                store: store.clone(),
                broker: broker.into(),
                result: result.into(),
                cache: cache.into(),
                events: events.clone(),
                explicit_internal_ts_base: "http://origin:8080".into(),
                configured_plugin_directory: "/data/plugins".into(),
                media_uid: u32::try_from(config["media_uid"].as_u64().unwrap()).unwrap(),
            },
            Arc::new(()),
        )
        .await
        .expect("actual inert assembly"),
    );
    let began = Instant::now();
    let caller: Arc<dyn Send + Sync> = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let operation = dvr.clone();
    let mut observer = Some(tokio::spawn(
        async move { operation.run_once(caller).await },
    ));
    let mut report = json!({"passed":false,"capture_only":true,"mode":mode,"database":database,"Source_task_execution":false,"Main_execution":false,"began_utc":chrono::Utc::now(),"commands":[],"same_original_caller_not_a_dummy":true,"event_connections_before":event_connections_before,"event_bytes_before":event_bytes_before});
    persist(&report);
    response(
        &json!({"stage":"started","mode":mode,"utc":chrono::Utc::now(),"monotonic_elapsed_seconds":began.elapsed().as_secs_f64()}),
    );
    let mut aborted = false;
    for expected_id in 1_u64..=16 {
        let command = input(&mut stream);
        assert_eq!(command["id"].as_u64(), Some(expected_id));
        let action = command["action"].as_str().expect("known action");
        let mut value = json!({"id":expected_id,"action":action,"utc":chrono::Utc::now(),"monotonic_elapsed_seconds":began.elapsed().as_secs_f64()});
        match action {
            "sample" => {}
            "abort-observer" => {
                assert!(matches!(mode,"cache-preack"|"sql-preack-ready"|"capture-observer-loss"),"post-claim error cases retain their real result observer for exact error settlement");
                assert!(!aborted);
                let handle = observer.take().expect("one actual observer");
                handle.abort();
                let result = handle.await;
                value["actual_observer_cancelled"] =
                    json!(result.as_ref().is_err_and(|e| e.is_cancelled()));
                aborted = true;
            }
            "drain-probe" => {
                let result = dvr.drain(Duration::from_millis(20)).await;
                value["drained"] = json!(result.is_ok());
                value["actual_drain_result"] = json!(format!("{result:?}"));
            }
            "finish" => {
                if let Some(handle) = observer.take() {
                    match tokio::time::timeout(Duration::from_secs(45), handle).await {
                        Ok(Ok(result)) => value["actual_observer_result"] = outcome(&result),
                        Ok(Err(error)) => {
                            value["actual_observer_join_error"] = json!(error.to_string())
                        }
                        Err(error) => value["observer_wait_deadline"] = json!(error.to_string()),
                    }
                }
                let result = dvr.drain(Duration::from_millis(100)).await;
                value["drained"] = json!(result.is_ok());
                value["actual_drain_result"] = json!(format!("{result:?}"));
            }
            _ => panic!("unsupported physical control, no arbitrary action or PID"),
        }
        value["same_original_caller_weak_alive"] = json!(weak.upgrade().is_some());
        value["available_slots"] = json!(dvr.available_slots());
        value["event_connections_available"] = json!(events.available_connections());
        value["event_bytes_available"] = json!(events.available_publish_bytes());
        value["diagnostics"] = dvr.diagnostics();
        value["observer_aborted"] = json!(aborted);
        value["observer_finished"] =
            json!(observer.as_ref().is_some_and(|handle| handle.is_finished()));
        report["commands"]
            .as_array_mut()
            .unwrap()
            .push(value.clone());
        persist(&report);
        response(&value);
        if action == "finish" {
            // The controller must not finish an aborted normal observer while
            // its real capture is running. Known post-claim operational errors
            // intentionally retain the bounded uncertain original scope.
            if mode == "capture-observer-loss" {
                assert!(
                    weak.upgrade().is_none() && dvr.available_slots() == 1,
                    "actual body must settle before fixture closes its Store"
                );
            }
            drop(dvr);
            report["event_connections_after"] = json!(events.available_connections());
            report["event_bytes_after"] = json!(events.available_publish_bytes());
            drop(events);
            store.pool().close().await;
            report["actual_first_close_pool_size"] = json!(store.pool().size());
            report["capture_complete"] = json!(true);
            report["passed"] = json!(true);
            report["ended_utc"] = json!(chrono::Utc::now());
            persist(&report);
            return;
        }
    }
    panic!("finite physical control budget exhausted");
}
