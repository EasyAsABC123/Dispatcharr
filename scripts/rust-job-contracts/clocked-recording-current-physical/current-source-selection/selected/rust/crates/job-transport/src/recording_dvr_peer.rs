//! Inert ignored peer draft. Actual Source model/artwork/Beat produce the frame;
//! the same normal library owns DVR execution. No Main/default consumer enabled.
use crate::recording_dvr::{Assembly, Dvr, Run};
use dispatcharr_store::Store;
use serde_json::{json, Value};
use std::{
    io::{BufRead, Write},
    os::unix::fs::MetadataExt,
    sync::Arc,
    time::Duration,
};

fn read(input: &mut impl BufRead) -> Value {
    let mut raw = Vec::new();
    loop {
        let part = input.fill_buf().expect("bounded owned input");
        assert!(!part.is_empty(), "one complete config line");
        let count = part
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(part.len(), |n| n + 1);
        assert!(raw.len() + count <= 16384, "fixture input16KiB");
        raw.extend_from_slice(&part[..count]);
        input.consume(count);
        if raw.ends_with(b"\n") {
            break;
        }
    }
    serde_json::from_slice(&raw).expect("actual config JSON")
}
fn endpoint(value: &str, db: i64) {
    let client = redis::Client::open(value).expect("owned Redis URI");
    let info = client.get_connection_info();
    assert_eq!(info.redis.db, db);
    assert!(
        matches!(&info.addr,redis::ConnectionAddr::Tcp(host,port)if matches!(host.as_str(),"localhost"|"127.0.0.1")&&*port==6379)
    );
}
fn persist(value: &Value) {
    let raw = serde_json::to_vec_pretty(value).expect("finite diagnostic JSON");
    assert!(raw.len() <= 2 * 1024 * 1024, "bounded native transcript");
    let pending = "/evidence/native-dvr-private.json.pending";
    std::fs::write(pending, raw).expect("owned diagnostic file");
    std::fs::rename(pending, "/evidence/native-dvr-private.json")
        .expect("atomic diagnostic publication");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Owned copied DB, natural real Beat dvr frame and local synthetic origin only; Root admission required"]
async fn actual_source_beat_normal_dvr_peer() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    assert_eq!(
        std::env::var("DVR_NATIVE_MODE").as_deref(),
        Ok("natural-Beat-normal-once")
    );
    let stdin = std::io::stdin();
    let mut input = stdin.lock();
    let config = read(&mut input);
    drop(input);
    let database = std::env::var("POSTGRES_DB").expect("owned database");
    assert!(
        database.starts_with("rust_probe_backup_jobs_") && database.len() == 33,
        "owned copied database only"
    );
    let database_url = config["database_url"].as_str().expect("owned SQL");
    let options: sqlx::postgres::PgConnectOptions = database_url.parse().expect("owned SQL URI");
    assert_eq!(options.get_database(), Some(database.as_str()));
    assert!(matches!(options.get_host(), "localhost" | "127.0.0.1"));
    assert!(
        matches!(options.get_port(), 5432 | 62130),
        "direct SQL or the one fixed owned observation relay"
    );
    assert_eq!(options.get_username(), "dispatch");
    let broker = config["broker_url"].as_str().expect("broker8");
    let result = config["result_url"].as_str().expect("result9");
    let cache = config["cache_url"].as_str().expect("cache0");
    endpoint(broker, 8);
    endpoint(result, 9);
    endpoint(cache, 0);
    let store = Store::connect(database_url, 1)
        .await
        .expect("actual bounded Store");
    let actual: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(store.pool())
        .await
        .expect("actual database identity");
    assert_eq!(actual, database);
    let events = Arc::new(
        dispatcharr_realtime::RealtimeBus::connect(cache, Default::default())
            .await
            .expect("actual Channels sink"),
    );
    let event_connections = events.available_connections();
    let event_bytes = events.available_publish_bytes();
    let media_uid = u32::try_from(
        config["media_uid"]
            .as_u64()
            .expect("actual configured fixture UID"),
    )
    .expect("UID range");
    let root_before = match std::fs::symlink_metadata("/data/recordings") {
        Ok(metadata) => {
            assert!(metadata.is_dir() && !metadata.file_type().is_symlink());
            assert_eq!(metadata.uid(), media_uid);
            json!({"exists":true,"uid":metadata.uid(),"inode":metadata.ino()})
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = std::fs::symlink_metadata("/data").expect("actual configured parent");
            assert!(parent.is_dir() && !parent.file_type().is_symlink());
            assert_eq!(parent.uid(), media_uid);
            json!({"exists":false,"parent_uid":parent.uid(),"parent_inode":parent.ino()})
        }
        Err(error) => panic!("read-only root context: {error}"),
    };
    let assembly = Assembly {
        store: store.clone(),
        broker: broker.into(),
        result: result.into(),
        cache: cache.into(),
        events: events.clone(),
        explicit_internal_ts_base: "http://origin:8080".into(),
        configured_plugin_directory: "/data/plugins".into(),
        media_uid,
    };
    let caller: Arc<dyn Send + Sync> = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let began = chrono::Utc::now();
    let mut report = json!({"passed":false,"capture_only":true,"normal_execution_qualified_before_run":false,"Source_task_execution":false,"Main_execution":false,"mode":"natural-Beat-normal-once","database":database,"command_began_utc":began,"config":config,"media_uid":media_uid,"recording_root_before_admission":root_before});
    persist(&report);
    let dvr = Dvr::connect(assembly, caller.clone())
        .await
        .expect("explicit inert DVR assembly");
    drop(caller);
    let observer = Arc::new(());
    let observer_weak = Arc::downgrade(&observer);
    let outcome = tokio::time::timeout(Duration::from_secs(180), dvr.run_once(observer)).await;
    report["command_returned_utc"] = json!(chrono::Utc::now());
    report["diagnostics_before_drain"] = dvr.diagnostics();
    report["actual_run"] = match &outcome {
        Ok(Ok(Run::Completed {
            task,
            metadata_written,
            bytes_written,
            file_size,
            processes,
            active_observations,
        })) => {
            json!({"kind":"Completed","task":task,"metadata_written":metadata_written,"bytes_written":bytes_written,"file_size":file_size,"processes":processes,"active_observations":active_observations})
        }
        Ok(Ok(Run::Empty)) => json!({"kind":"Empty"}),
        Ok(Ok(Run::UnsupportedHead)) => json!({"kind":"UnsupportedHead"}),
        Ok(Err(error)) => json!({"kind":"Operational","diagnostic":format!("{error:?}")}),
        Err(error) => {
            json!({"kind":"ObserverDeadline","diagnostic":error.to_string(),"does_not_establish_physical_settlement":true})
        }
    };
    persist(&report); // Every actual result/diagnostic precedes an assertion.
    if !matches!(outcome, Ok(Ok(Run::Completed { .. }))) {
        dvr.request_shutdown();
    }
    let drain = dvr.drain(Duration::from_secs(15)).await;
    report["drain"] = json!({"returned_utc":chrono::Utc::now(),"ok":drain.is_ok(),"diagnostic":format!("{drain:?}")});
    report["diagnostics_after_drain"] = dvr.diagnostics();
    report["caller_weak_alive_after_run"] = json!(observer_weak.upgrade().is_some());
    report["constructor_caller_weak_alive"] = json!(weak.upgrade().is_some());
    report["event_connections_after"] = json!(events.available_connections());
    report["event_connections_before"] = json!(event_connections);
    report["event_bytes_after"] = json!(events.available_publish_bytes());
    report["event_bytes_before"] = json!(event_bytes);
    persist(&report);
    assert!(
        matches!(outcome, Ok(Ok(Run::Completed { .. }))),
        "actual diagnostic saved before refusing nonnormal outcome"
    );
    assert!(drain.is_ok());
    assert_eq!(dvr.available_slots(), 1);
    assert!(observer_weak.upgrade().is_none());
    assert_eq!(events.available_connections(), event_connections);
    assert_eq!(events.available_publish_bytes(), event_bytes);
    drop(dvr);
    drop(events);
    store.pool().close().await;
    report["pool_size_after_explicit_close"] = json!(store.pool().size());
    report["completed_utc"] = json!(chrono::Utc::now());
    report["capture_complete"] = json!(true);
    report["passed"] = json!(true);
    persist(&report);
    println!(
        "NATIVE_DVR_COMPLETE={}",
        json!({"passed":true,"task":report["actual_run"]["task"],"file":"native-dvr-private.json"})
    );
    std::io::stdout()
        .flush()
        .expect("bounded completion receipt");
}
