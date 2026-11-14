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

// Report-only lossless relocation. IPC responses stay byte-equivalent and the
// existing TOTAL1MiB collector remains a hard acceptance limit. The two real
// capture/remux records each publish stderr only once, after reader settlement.
const STDERR_BLOB_CAP: usize = 65_536;
const STDERR_BLOBS: usize = 2;
fn capture_error(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}
fn read_stderr_blob(path: &std::path::Path) -> std::io::Result<Vec<u8>> {
    use std::{io::Read, os::unix::fs::OpenOptionsExt};
    if !std::fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(capture_error("stderr blob must be regular"));
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let mut bytes = Vec::new();
    file.take((STDERR_BLOB_CAP + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > STDERR_BLOB_CAP {
        return Err(capture_error("stderr blob capacity"));
    }
    Ok(bytes)
}
fn stderr_blob(root: &std::path::Path, bytes: &[u8]) -> std::io::Result<Value> {
    use sha2::{Digest, Sha256};
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    if bytes.len() > STDERR_BLOB_CAP {
        return Err(capture_error("actual stderr tail capacity"));
    }
    let folder = root.join("native-dvr-stderr-by-sha256");
    match std::fs::create_dir(&folder) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e),
    }
    if !std::fs::symlink_metadata(&folder)?.file_type().is_dir() {
        return Err(capture_error(
            "stderr folder must be owned regular directory",
        ));
    }
    let mut count = 0_usize;
    let mut total = 0_usize;
    for entry in std::fs::read_dir(&folder)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_str().ok_or(capture_error("stderr name UTF8"))?;
        if !name.is_ascii()
            || name.len() != 68
            || !name.ends_with(".bin")
            || !name[..64]
                .bytes()
                .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v))
        {
            return Err(capture_error("unknown stderr file retained/refused"));
        }
        let observed = read_stderr_blob(&entry.path())?;
        if format!("{:x}", Sha256::digest(&observed)) != name[..64] {
            return Err(capture_error("stderr content hash mismatch"));
        }
        count += 1;
        total = total
            .checked_add(observed.len())
            .ok_or(capture_error("stderr total overflow"))?;
    }
    if count > STDERR_BLOBS || total > STDERR_BLOBS * STDERR_BLOB_CAP {
        return Err(capture_error("stderr two-process total capacity"));
    }
    let hash = format!("{:x}", Sha256::digest(bytes));
    let target = folder.join(format!("{hash}.bin"));
    if target.exists() {
        if read_stderr_blob(&target)? != bytes {
            return Err(capture_error("stderr dedup bytes mismatch"));
        }
    } else {
        if count == STDERR_BLOBS || total + bytes.len() > STDERR_BLOBS * STDERR_BLOB_CAP {
            return Err(capture_error("more than two immutable process tails"));
        }
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&target)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    Ok(
        json!({"$native_dvr_stderr_ref":{"path":format!("native-dvr-stderr-by-sha256/{hash}.bin"),"sha256":hash,"bytes":bytes.len(),"codec":"raw-u8-tail-v1"}}),
    )
}
fn spill_processes(rows: &mut Value, root: &std::path::Path) -> std::io::Result<()> {
    let rows = rows
        .as_array_mut()
        .ok_or(capture_error("process rows array"))?;
    if rows.len() > 2 {
        return Err(capture_error("two actual process slots"));
    }
    for row in rows {
        if let Some(stderr) = row.get_mut("stderr").filter(|v| !v.is_null()) {
            let tail = stderr
                .get_mut("tail")
                .ok_or(capture_error("actual stderr tail missing"))?;
            let values = tail
                .as_array()
                .ok_or(capture_error("raw stderr byte array"))?;
            if values.len() > STDERR_BLOB_CAP {
                return Err(capture_error("actual stderr tail capacity"));
            }
            let bytes: Vec<u8> = values
                .iter()
                .map(|v| {
                    v.as_u64()
                        .filter(|v| *v <= 255)
                        .map(|v| v as u8)
                        .ok_or(capture_error("actual stderr u8"))
                })
                .collect::<std::io::Result<_>>()?;
            *tail = stderr_blob(root, &bytes)?;
        }
    }
    Ok(())
}
fn spill_report(report: &Value, root: &std::path::Path) -> std::io::Result<Value> {
    let mut projected = report.clone();
    let commands = projected
        .get_mut("commands")
        .and_then(Value::as_array_mut)
        .ok_or(capture_error("actual command list"))?;
    if commands.len() > 16 {
        return Err(capture_error("actual command bound16"));
    }
    for command in commands {
        if let Some(rows) = command.pointer_mut("/diagnostics/processes/rows") {
            spill_processes(rows, root)?;
        }
        if let Some(rows) = command.pointer_mut("/actual_observer_result/processes") {
            spill_processes(rows, root)?;
        }
    }
    Ok(projected)
}
fn persist_for(report: &Value, captured61: bool) {
    if captured61 {
        persist(
            &spill_report(report, std::path::Path::new("/evidence"))
                .expect("bounded owned lossless stderr capture"),
        );
    } else {
        persist(report);
    }
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
    run_peer(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Root-admitted actual captured61 owned local video+MP2 context only; no Main/default graph"]
async fn actual_captured61_dvr_gate_peer() {
    run_peer(true).await;
}
async fn run_peer(captured61: bool) {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    assert_eq!(
        std::env::var("DVR_NATIVE_MODE").as_deref(),
        Ok(if captured61 {
            "physical61-once"
        } else {
            "physical-once"
        })
    );
    let stdin = std::io::stdin();
    let mut stream = stdin.lock();
    let config = input(&mut stream);
    let mode = config["mode"].as_str().expect("fixed mode");
    if captured61 {
        assert!(matches!(mode, "captured61-normal" | "result-reply"));
        assert_eq!(config["captured_domain"], "Source61LocalVideoMp2");
    } else {
        assert!(MODES.contains(&mode));
        assert!(config.get("captured_domain").is_none());
    }
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
    let assembly = Assembly {
        store: store.clone(),
        broker: broker.into(),
        result: result.into(),
        cache: cache.into(),
        events: events.clone(),
        explicit_internal_ts_base: "http://origin:8080".into(),
        configured_plugin_directory: "/data/plugins".into(),
        media_uid: u32::try_from(config["media_uid"].as_u64().unwrap()).unwrap(),
    };
    let dvr = Arc::new(
        if captured61 {
            Dvr::connect_captured_61_local(assembly, Arc::new(())).await
        } else {
            Dvr::connect(assembly, Arc::new(())).await
        }
        .expect("explicit captured-domain inert assembly"),
    );
    let began = Instant::now();
    let caller: Arc<dyn Send + Sync> = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let operation = dvr.clone();
    let mut observer = Some(tokio::spawn(
        async move { operation.run_once(caller).await },
    ));
    let mut report = json!({"passed":false,"capture_only":true,"mode":mode,"database":database,"Source_task_execution":false,"Main_execution":false,"began_utc":chrono::Utc::now(),"commands":[],"same_original_caller_not_a_dummy":true,"event_connections_before":event_connections_before,"event_bytes_before":event_bytes_before});
    persist_for(&report, captured61);
    response(
        &json!({"stage":"started","mode":mode,"utc":chrono::Utc::now(),"monotonic_elapsed_seconds":began.elapsed().as_secs_f64()}),
    );
    let mut aborted = false;
    let mut observed_outcome: Option<Value> = None;
    let mut observed_join_error: Option<String> = None;
    for expected_id in 1_u64..=16 {
        let command = input(&mut stream);
        assert_eq!(command["id"].as_u64(), Some(expected_id));
        let action = command["action"].as_str().expect("known action");
        let mut value = json!({"id":expected_id,"action":action,"utc":chrono::Utc::now(),"monotonic_elapsed_seconds":began.elapsed().as_secs_f64()});
        match action {
            "sample" => {
                if observer.as_ref().is_some_and(|handle| handle.is_finished()) {
                    let handle = observer.take().expect("one actual completed observer");
                    match handle.await {
                        Ok(result) => {
                            let actual = outcome(&result);
                            value["actual_observer_result"] = actual.clone();
                            observed_outcome = Some(actual);
                        }
                        Err(error) => {
                            let actual = error.to_string();
                            value["actual_observer_join_error"] = json!(actual);
                            observed_join_error = Some(actual);
                        }
                    }
                }
            }
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
                if let Some(actual) = &observed_outcome {
                    value["actual_observer_result"] = actual.clone();
                }
                if let Some(actual) = &observed_join_error {
                    value["actual_observer_join_error"] = json!(actual);
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
        value["observer_finished"] = json!(
            observed_outcome.is_some()
                || observed_join_error.is_some()
                || observer.as_ref().is_some_and(|handle| handle.is_finished())
        );
        report["commands"]
            .as_array_mut()
            .unwrap()
            .push(value.clone());
        persist_for(&report, captured61);
        response(&value);
        if action == "finish" {
            // The controller must not finish an aborted normal observer while
            // its real capture is running. Known post-claim operational errors
            // intentionally retain the bounded uncertain original scope.
            if matches!(mode, "capture-observer-loss" | "captured61-normal") {
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
            persist_for(&report, captured61);
            return;
        }
    }
    panic!("finite physical control budget exhausted");
}

#[cfg(test)]
mod lossless_stderr_report_controls {
    use super::*;
    fn root() -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("owned-dvr-stderr-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&p).unwrap();
        p
    }
    fn rows(a: Vec<u8>, b: Vec<u8>) -> Value {
        json!([{"stderr":{"total_bytes":a.len(),"tail":a,"read_error":null}},{"stderr":{"total_bytes":b.len(),"tail":b,"read_error":null}}])
    }
    #[test]
    fn repeated_two_actual_tail_shapes_spill_once_under_unchanged_report_cap() {
        let p = root();
        let process = rows(vec![b'a'; STDERR_BLOB_CAP], vec![b'b'; STDERR_BLOB_CAP]);
        let commands:Vec<_>=(0..16).map(|id|json!({"id":id,"utc":"literal-clock-unchanged","diagnostics":{"processes":{"rows":process}}})).collect();
        let raw = json!({"commands":commands});
        assert!(serde_json::to_vec_pretty(&raw).unwrap().len() > 2 * 1024 * 1024);
        let projected = spill_report(&raw, &p).unwrap();
        assert!(serde_json::to_vec_pretty(&projected).unwrap().len() <= 2 * 1024 * 1024);
        assert_eq!(projected["commands"].as_array().unwrap().len(), 16);
        assert_eq!(
            std::fs::read_dir(p.join("native-dvr-stderr-by-sha256"))
                .unwrap()
                .count(),
            2
        );
        for (id, c) in projected["commands"].as_array().unwrap().iter().enumerate() {
            assert_eq!(c["id"], id);
            assert_eq!(c["utc"], "literal-clock-unchanged");
            for (slot, byte) in [b'a', b'b'].into_iter().enumerate() {
                let r = &c["diagnostics"]["processes"]["rows"][slot]["stderr"]["tail"]
                    ["$native_dvr_stderr_ref"];
                assert_eq!(
                    std::fs::read(p.join(r["path"].as_str().unwrap())).unwrap(),
                    vec![byte; STDERR_BLOB_CAP]
                );
            }
        }
        std::fs::remove_dir_all(p).unwrap();
    }
    #[test]
    fn third_immutable_tail_is_refused_not_capacity_increased() {
        let p = root();
        stderr_blob(&p, b"capture").unwrap();
        stderr_blob(&p, b"remux").unwrap();
        assert!(stderr_blob(&p, b"third").is_err());
        assert_eq!(
            std::fs::read_dir(p.join("native-dvr-stderr-by-sha256"))
                .unwrap()
                .count(),
            2
        );
        std::fs::remove_dir_all(p).unwrap();
    }
    #[test]
    fn existing_wrong_hash_content_cannot_be_reused() {
        let p = root();
        let r = stderr_blob(&p, b"capture").unwrap();
        std::fs::write(
            p.join(r["$native_dvr_stderr_ref"]["path"].as_str().unwrap()),
            b"changed",
        )
        .unwrap();
        assert!(stderr_blob(&p, b"capture").is_err());
        std::fs::remove_dir_all(p).unwrap();
    }
    #[test]
    fn symlink_is_retained_and_refused_without_following() {
        use std::os::unix::fs::symlink;
        let p = root();
        let r = stderr_blob(&p, b"capture").unwrap();
        let file = p.join(r["$native_dvr_stderr_ref"]["path"].as_str().unwrap());
        std::fs::remove_file(&file).unwrap();
        let target = p.join("outside-data");
        std::fs::write(&target, b"unmodified").unwrap();
        symlink(&target, &file).unwrap();
        assert!(stderr_blob(&p, b"capture").is_err());
        assert_eq!(std::fs::read(target).unwrap(), b"unmodified");
        std::fs::remove_dir_all(p).unwrap();
    }
    #[test]
    fn boolean_or_non_u8_tail_is_not_projected_as_bytes() {
        let p = root();
        for value in [json!(true), json!(256)] {
            let raw = json!({"commands":[{"diagnostics":{"processes":{"rows":[{"stderr":{"tail":[value]}}]}}}]});
            assert!(spill_report(&raw, &p).is_err());
        }
        std::fs::remove_dir_all(p).unwrap();
    }
    #[test]
    fn more_than_actual_stderr_tail_cap_is_refused() {
        let p = root();
        assert!(stderr_blob(&p, &vec![0; STDERR_BLOB_CAP + 1]).is_err());
        std::fs::remove_dir_all(p).unwrap();
    }
    #[test]
    fn more_than_two_process_slots_is_refused() {
        let p = root();
        let raw = json!({"commands":[{"diagnostics":{"processes":{"rows":[{},{},{}]}}}]});
        assert!(spill_report(&raw, &p).is_err());
        std::fs::remove_dir_all(p).unwrap();
    }
    #[test]
    fn command_bound16_is_not_raised() {
        let p = root();
        let raw = json!({"commands":vec![json!({});17]});
        assert!(spill_report(&raw, &p).is_err());
        std::fs::remove_dir_all(p).unwrap();
    }
    #[test]
    fn unknown_unicode_file_refuses_without_deletion_or_panicking() {
        let p = root();
        let folder = p.join("native-dvr-stderr-by-sha256");
        std::fs::create_dir(&folder).unwrap();
        let file = folder.join(format!("{}é.bin", "a".repeat(63)));
        std::fs::write(&file, b"retain").unwrap();
        assert!(stderr_blob(&p, b"capture").is_err());
        assert!(file.exists());
        std::fs::remove_dir_all(p).unwrap();
    }
    #[test]
    fn completed_outcome_processes_use_same_lossless_tail_references() {
        let p = root();
        let process = rows(b"capture".to_vec(), b"remux".to_vec());
        let raw = json!({"commands":[{"diagnostics":{"processes":{"rows":process}},"actual_observer_result":{"kind":"Completed","processes":process}}]});
        let projected = spill_report(&raw, &p).unwrap();
        assert_eq!(
            projected["commands"][0]["diagnostics"]["processes"]["rows"],
            projected["commands"][0]["actual_observer_result"]["processes"]
        );
        std::fs::remove_dir_all(p).unwrap();
    }
}
