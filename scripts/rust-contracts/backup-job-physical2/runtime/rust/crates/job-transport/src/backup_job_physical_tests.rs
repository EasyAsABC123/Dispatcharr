//! Test-only real BackupFiles→BackupApplication physical publish/read ownership.
//! Original Dispatcharr contributors retain application credit and licensing.
use super::BackupApplication;
use crate::worker::ArchiveHandler;
use chrono::{Datelike, Timelike, Utc};
use dispatcharr_backups::{Error as ArchiveError, SourceClock};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_store::{
    initialize_empty_database, AuthConfig, AuthService, BackupFiles, BackupJobApi,
    NotificationCache, SchemaInitialization, Store, StoreError,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::os::fd::AsRawFd;
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Read, Write},
    net::SocketAddr,
    os::unix::process::ExitStatusExt,
    path::{Path as FsPath, PathBuf},
    process::{Child, Command, Stdio},
    sync::{mpsc, Arc, Mutex, Weak},
    thread,
    time::{Duration, Instant, SystemTime},
};
use tokio::io::AsyncWriteExt;
const NOTICE: &str = "core_systemnotification";
const DISMISS: &str = "core_notificationdismissal";
const TABLES: [&str; 11] = [
    "accounts_user",
    "m3u_m3uaccount",
    "m3u_m3uaccountprofile",
    "vod_movie",
    "vod_m3umovierelation",
    "vod_series",
    "vod_episode",
    "vod_m3uepisoderelation",
    "core_coresettings",
    NOTICE,
    DISMISS,
];
fn hash(b: &[u8]) -> String {
    format!("{:x}", Sha256::digest(b))
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|b| format!("{b:02x}")).collect()
}
fn save(path: &FsPath, v: &Value) {
    let b = serde_json::to_vec_pretty(v).unwrap();
    assert!(b.len() <= 32 * 1024 * 1024);
    std::fs::write(path, b).unwrap();
}
fn owned(v: &str) {
    let s = v.strip_prefix("rust_probe_backup_jobs_physical_").unwrap();
    assert_eq!(s.len(), 10);
    assert!(s
        .bytes()
        .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)));
}
async fn snapshot(store: &Store) -> Value {
    let mut tables = BTreeMap::new();
    let mut sequences = BTreeMap::new();
    let mut total = 0;
    for table in TABLES {
        let rows:Value=sqlx::query_scalar(&format!("SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY id),'[]'::jsonb) FROM (SELECT * FROM {table} ORDER BY id LIMIT 10001) t")).fetch_one(store.pool()).await.unwrap();
        assert!(rows.as_array().unwrap().len() <= 10000);
        for row in rows.as_array().unwrap() {
            let n = serde_json::to_vec(row).unwrap().len();
            assert!(n <= 131072);
            total += n;
            assert!(total <= 8388608);
        }
        tables.insert(table, rows);
        let seq:Value=sqlx::query_scalar(&format!("SELECT jsonb_build_object('last_value',last_value,'is_called',is_called) FROM {table}_id_seq")).fetch_one(store.pool()).await.unwrap();
        sequences.insert(table, seq);
    }
    json!({"tables":tables,"sequences":sequences})
}
async fn credentials(store: &Store) -> Value {
    let rows:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY id),'[]'::jsonb) FROM (SELECT * FROM core_systemevent ORDER BY id LIMIT 10001) t").fetch_one(store.pool()).await.unwrap();
    assert!(rows.as_array().unwrap().len() <= 10000);
    assert!(serde_json::to_vec(&rows).unwrap().len() <= 8388608);
    let sequence:Value=sqlx::query_scalar("SELECT jsonb_build_object('last_value',last_value,'is_called',is_called) FROM core_systemevent_id_seq").fetch_one(store.pool()).await.unwrap();
    let users: Value = sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY id),'[]'::jsonb) FROM accounts_user t",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    json!({"accounts_user":users,"events":rows,"sequence":sequence})
}
async fn cache(url: &str) -> Value {
    let client = redis::Client::open(url).unwrap();
    let mut c = client.get_multiplexed_async_connection().await.unwrap();
    let mut cursor = 0u64;
    let mut rows = BTreeMap::new();
    let mut used = 0;
    let mut complete = false;
    for _ in 0..128 {
        let (next, keys): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("COUNT")
            .arg(100)
            .query_async(&mut c)
            .await
            .unwrap();
        for key in keys {
            assert!(key.len() <= 1024);
            let dump: Option<Vec<u8>> = redis::cmd("DUMP")
                .arg(&key)
                .query_async(&mut c)
                .await
                .unwrap();
            if let Some(dump) = dump {
                used += key.len() + dump.len();
                assert!(dump.len() <= 65536 && used <= 1048576 && rows.len() < 1000);
                let ttl: i64 = redis::cmd("PTTL")
                    .arg(&key)
                    .query_async(&mut c)
                    .await
                    .unwrap();
                rows.insert(hex(&key), json!({"dump_hex":hex(&dump),"pttl":ttl}));
            }
        }
        cursor = next;
        if cursor == 0 {
            complete = true;
            break;
        }
    }
    assert!(complete);
    json!({"raw_DUMP_PTTL":rows,"credential_Source_comparison":false})
}
struct Peer {
    child: Child,
    replies: mpsc::Receiver<Result<Value, String>>,
    reader: Option<thread::JoinHandle<()>>,
    next: usize,
    receipt: PathBuf,
    stopped: bool,
    starttime: Option<String>,
}
impl Peer {
    fn start(root: &FsPath, output: &FsPath, stages: &Value) -> (Self, u16) {
        let err = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(output.join("proxy.stderr"))
            .unwrap();
        let mut command = Command::new("/dispatcharrpy/bin/python");
        command
            .arg(root.join("controller/owned_resp_proxy.py"))
            .arg("--backend-port")
            .arg("6379")
            .arg("--contracts")
            .arg(root.join("inputs/resp-contracts.json"))
            .arg("--case")
            .arg(stages[0].as_str().unwrap());
        let mut child = command
            .arg("--evidence")
            .arg(output.join("proxy-events.jsonl"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(err)
            .spawn()
            .unwrap();
        let starttime = std::fs::read_to_string(format!("/proc/{}/stat", child.id()))
            .ok()
            .map(|s| {
                s.rsplit_once(')')
                    .unwrap()
                    .1
                    .split_whitespace()
                    .nth(19)
                    .unwrap()
                    .to_string()
            });
        assert!(starttime.is_some(), "ActualProxyStarttimeRequired");
        let stdout = child.stdout.take().unwrap();
        let (tx, replies) = mpsc::channel::<Result<Value, String>>();
        let stdout_path = output.join("proxy-control.stdout");
        let reader = thread::spawn(move || {
            let mut raw_log = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(stdout_path)
                .unwrap();
            let mut used = 0usize;
            let mut r = BufReader::new(stdout);
            loop {
                let mut line = Vec::new();
                let got = Read::take(Read::by_ref(&mut r), 4194305).read_until(b'\n', &mut line);
                match got {
                    Ok(0) => break,
                    Ok(_) => {
                        used += line.len();
                        if used > 33554432 {
                            let _ = tx.send(Err("ProxyRawStdout32MiBCapacity".into()));
                            break;
                        }
                        raw_log.write_all(&line).unwrap();
                        if line.len() > 4194304 || line.last() != Some(&b'\n') {
                            let _ = tx.send(Err("ProxyReplyCapacity".into()));
                            break;
                        }
                        let value = serde_json::from_slice(&line).map_err(|e| e.to_string());
                        if tx.send(value).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e.to_string()));
                        break;
                    }
                }
            }
        });
        let hello = replies
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(hello["ready"], true);
        let port = hello["port"].as_u64().unwrap().try_into().unwrap();
        (
            Self {
                child,
                replies,
                reader: Some(reader),
                next: 0,
                receipt: output.join("proxy-process.json"),
                stopped: false,
                starttime,
            },
            port,
        )
    }
    fn command(&mut self, action: &str, ordinal: Option<u64>) -> Value {
        let id = self.next;
        self.next += 1;
        assert!(self.next <= 6000);
        let b = json!({"id":id,"action":action,"ordinal":ordinal}).to_string();
        let input = self.child.stdin.as_mut().unwrap();
        writeln!(input, "{b}").unwrap();
        input.flush().unwrap();
        let reply = self
            .replies
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(reply["id"], id);
        reply["result"].clone()
    }
    fn stats(&mut self) -> Value {
        self.command("stats", None)
    }
    fn release(&mut self, ordinal: u64) {
        self.command("release", Some(ordinal));
    }
    fn finish(&mut self) -> Value {
        let stats = self.command("stop", None);
        self.child.stdin.take();
        let deadline = Instant::now() + Duration::from_secs(8);
        let status = loop {
            if let Some(v) = self.child.try_wait().unwrap() {
                break v;
            }
            assert!(Instant::now() < deadline, "ProxyProcessWait8");
            thread::sleep(Duration::from_millis(5));
        };
        let waited = self.child.wait().unwrap();
        assert_eq!(status.code(), waited.code());
        if let Some(r) = self.reader.take() {
            r.join().unwrap();
        }
        self.stopped = true;
        let stderr_path = self.receipt.with_file_name("proxy.stderr");
        let stderr_bytes = std::fs::metadata(&stderr_path).unwrap().len();
        assert!(stderr_bytes <= 1048576, "ProxyStderr1MiBCapacity");
        let stderr_sha = hash(&std::fs::read(stderr_path).unwrap());
        save(
            &self.receipt,
            &json!({"pid":self.child.id(),"exit":status.code(),"signal":status.signal(),"status_debug":format!("{status:?}"),"starttime":self.starttime,"wait":waited.code(),"stderr_bytes":stderr_bytes,"stderr_sha256":stderr_sha,"proc_PID_absent_after_wait":!std::path::Path::new(&format!("/proc/{}",self.child.id())).exists(),"PID_scope":"ELF container namespace; outer host Docker-top proof separate","stdout_reader_joined":true,"forced":false,"settled_utc":chrono::Utc::now()}),
        );
        assert!(status.success());
        stats
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        if !self.stopped {
            let _ = self.child.kill();
            self.child.stdin.take();
            let status = self.child.wait().ok();
            let joined = self.reader.take().map(|r| r.join().is_ok()).unwrap_or(true);
            save(
                &self.receipt,
                &json!({"pid":self.child.id(),"forced":true,"wait":status.as_ref().and_then(|s|s.code()),"signal":status.as_ref().and_then(|s|s.signal()),"status_debug":format!("{status:?}"),"starttime":self.starttime,"PID_scope":"ELF container namespace; outer host Docker-top proof separate","partial_failure_preserved":true,"proc_PID_absent_after_wait":!std::path::Path::new(&format!("/proc/{}",self.child.id())).exists(),"stdout_reader_joined":joined}),
            );
        }
    }
}

struct OwnUtcClock;
impl SourceClock for OwnUtcClock {
    fn filename_timestamp(&self) -> Result<String, ArchiveError> {
        Ok(Utc::now().format("%Y.%m.%d.%H.%M.%S").to_string())
    }
    fn created_at_utc(&self) -> Result<String, ArchiveError> {
        Ok(Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, false))
    }
    fn zip_timestamp(&self, time: SystemTime) -> Result<zip::DateTime, ArchiveError> {
        let dt: chrono::DateTime<Utc> = time.into();
        zip::DateTime::from_date_and_time(
            dt.year().try_into().map_err(|_| ArchiveError::Metadata)?,
            dt.month() as u8,
            dt.day() as u8,
            dt.hour() as u8,
            dt.minute() as u8,
            dt.second() as u8,
        )
        .map_err(|_| ArchiveError::Metadata)
    }
}
#[derive(Default)]
struct Observation {
    caller: Option<Weak<dyn Send + Sync>>,
    calls: usize,
    logical_done: bool,
    logical_result: Option<String>,
    logical_completed_utc: Option<String>,
}
#[derive(Clone)]
struct ObservedApi {
    real: BackupApplication,
    observation: Arc<Mutex<Observation>>,
}
impl ObservedApi {
    fn begin(&self, caller: &Arc<dyn Send + Sync>) {
        let mut state = self.observation.lock().unwrap();
        assert_eq!(state.calls, 0, "ExactlyOneRealAdapterCall");
        state.calls += 1;
        state.caller = Some(Arc::downgrade(caller));
    }
    fn completed<T>(&self, result: &Result<T, StoreError>) {
        let mut state = self.observation.lock().unwrap();
        state.logical_done = true;
        state.logical_result = Some(match result {
            Ok(_) => "Ok".into(),
            Err(e) => format!("Err({e:?})"),
        });
        state.logical_completed_utc = Some(Utc::now().to_rfc3339());
    }
    fn state(&self) -> Value {
        let state = self.observation.lock().unwrap();
        // No strong caller is stored or returned by this observer.
        let alive = state.caller.as_ref().is_some_and(|w| w.strong_count() != 0);
        json!({"calls":state.calls,"Weak_Some":alive,"weak_observer_stores_no_Arc":true,
            "logical_done":state.logical_done,"logical_result":state.logical_result,
            "logical_completed_utc":state.logical_completed_utc,
            "real_application_http_slots_available":self.real.inner.http.available_permits(),
            "real_jobs_slots_available":self.real.inner.jobs.available_slots()})
    }
}
impl BackupJobApi for ObservedApi {
    fn create(&self, caller: Arc<dyn Send + Sync>) -> BoxFuture<'_, Result<String, StoreError>> {
        self.begin(&caller);
        Box::pin(async move {
            let result = self.real.create(caller).await;
            self.completed(&result);
            result
        })
    }
    fn status<'a>(
        &'a self,
        task_id: &'a str,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'a, Result<Value, StoreError>> {
        self.begin(&caller);
        Box::pin(async move {
            let result = self.real.status(task_id, caller).await;
            self.completed(&result);
            result
        })
    }
    fn shutdown(&self, grace: Duration) -> BoxFuture<'_, bool> {
        self.real.shutdown(grace)
    }
}
async fn redis_value(url: &str, key: &str) -> Value {
    let client = redis::Client::open(url).unwrap();
    let mut c = client.get_multiplexed_async_connection().await.unwrap();
    let kind: String = redis::cmd("TYPE")
        .arg(key)
        .query_async(&mut c)
        .await
        .unwrap();
    let dump: Option<Vec<u8>> = redis::cmd("DUMP")
        .arg(key)
        .query_async(&mut c)
        .await
        .unwrap();
    let ttl: i64 = redis::cmd("PTTL")
        .arg(key)
        .query_async(&mut c)
        .await
        .unwrap();
    let value: Value = match kind.as_str() {
        "none" => Value::Null,
        "string" => {
            let b: Vec<u8> = redis::cmd("GET")
                .arg(key)
                .query_async(&mut c)
                .await
                .unwrap();
            assert!(b.len() <= 262144);
            json!({"value_hex":hex(&b)})
        }
        "list" => {
            let n: usize = redis::cmd("LLEN")
                .arg(key)
                .query_async(&mut c)
                .await
                .unwrap();
            assert!(n <= 16);
            let rows: Vec<Vec<u8>> = redis::cmd("LRANGE")
                .arg(key)
                .arg(0)
                .arg(-1)
                .query_async(&mut c)
                .await
                .unwrap();
            assert!(rows.iter().all(|r| r.len() <= 65536));
            json!({"list_hex":rows.iter().map(|b|hex(b)).collect::<Vec<_>>()})
        }
        "set" => {
            let n: usize = redis::cmd("SCARD")
                .arg(key)
                .query_async(&mut c)
                .await
                .unwrap();
            assert!(n <= 16);
            let mut rows: Vec<Vec<u8>> = redis::cmd("SMEMBERS")
                .arg(key)
                .query_async(&mut c)
                .await
                .unwrap();
            rows.sort();
            json!({"set_hex":rows.iter().map(|b|hex(b)).collect::<Vec<_>>()})
        }
        _ => panic!("UndeclaredBrokerBackendType:{kind}"),
    };
    assert!(dump.as_ref().is_none_or(|b| b.len() <= 262144));
    json!({"key":key,"type":kind,"DUMP_hex":dump.as_ref().map(|b|hex(b)),"pttl":ttl,"value":value,"consumed":false})
}
async fn redis_snapshot(url: &str, db: u8) -> Value {
    let client = redis::Client::open(url).unwrap();
    assert_eq!(client.get_connection_info().redis.db, i64::from(db));
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let mut cursor = 0u64;
    let mut rows = BTreeMap::new();
    let mut used = 0usize;
    let mut done = false;
    for _ in 0..128 {
        let (next, keys): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("COUNT")
            .arg(100)
            .query_async(&mut connection)
            .await
            .unwrap();
        cursor = next;
        for key in keys {
            assert!(key.len() <= 1024 && rows.len() < 1000);
            let kind: String = redis::cmd("TYPE")
                .arg(&key)
                .query_async(&mut connection)
                .await
                .unwrap();
            let dump: Option<Vec<u8>> = redis::cmd("DUMP")
                .arg(&key)
                .query_async(&mut connection)
                .await
                .unwrap();
            let ttl: i64 = redis::cmd("PTTL")
                .arg(&key)
                .query_async(&mut connection)
                .await
                .unwrap();
            let mut row = json!({"key_hex":hex(&key),"type":kind,"dump_hex":dump.as_ref().map(|v|hex(v)),"pttl":ttl});
            used += key.len() + dump.as_ref().map_or(0, Vec::len);
            assert!(used <= 8388608);
            if kind == "string" {
                let value: Option<Vec<u8>> = redis::cmd("GET")
                    .arg(&key)
                    .query_async(&mut connection)
                    .await
                    .unwrap();
                row["value_hex"] = json!(value.map(|v| hex(&v)));
            } else if kind == "list" {
                let count: u64 = redis::cmd("LLEN")
                    .arg(&key)
                    .query_async(&mut connection)
                    .await
                    .unwrap();
                assert!(count <= 1000);
                let values: Vec<Vec<u8>> = redis::cmd("LRANGE")
                    .arg(&key)
                    .arg(0)
                    .arg(-1)
                    .query_async(&mut connection)
                    .await
                    .unwrap();
                assert!(values.iter().map(Vec::len).sum::<usize>() <= 8388608);
                row["list_hex"] = json!(values.iter().map(|v| hex(v)).collect::<Vec<_>>());
            }
            if kind == "set" {
                let count: u64 = redis::cmd("SCARD")
                    .arg(&key)
                    .query_async(&mut connection)
                    .await
                    .unwrap();
                assert!(count <= 1000);
                let mut values: Vec<Vec<u8>> = redis::cmd("SMEMBERS")
                    .arg(&key)
                    .query_async(&mut connection)
                    .await
                    .unwrap();
                values.sort();
                assert!(values.iter().map(Vec::len).sum::<usize>() <= 8388608);
                row["set_hex"] = json!(values.iter().map(|v| hex(v)).collect::<Vec<_>>());
            }
            rows.insert(hex(&key), row);
        }
        if cursor == 0 {
            done = true;
            break;
        }
    }
    assert!(done);
    json!({"database":db,"capture_bytes":used,"rows":rows.into_values().collect::<Vec<_>>()})
}

async fn raw_state(
    store: &Store,
    cache_url: &str,
    broker: &str,
    backend: &str,
    task_key: &str,
) -> Value {
    json!({"SQL":snapshot(store).await,"credentials":credentials(store).await,"cache":cache(cache_url).await,
        "broker_queue":redis_value(broker,"celery").await,"binding":redis_value(broker,"_kombu.binding.celery").await,
        "backend":redis_value(backend,task_key).await,"all_broker":redis_snapshot(broker,2).await,"all_backend":redis_snapshot(backend,3).await})
}

fn same_inventory(before: &Value, after: &Value) {
    assert_eq!(before["database"], after["database"]);
    let left = before["rows"].as_array().unwrap();
    let right = after["rows"].as_array().unwrap();
    assert_eq!(left.len(), right.len());
    for (a, b) in left.iter().zip(right) {
        let mut a = a.clone();
        let mut b = b.clone();
        let x = a
            .as_object_mut()
            .unwrap()
            .remove("pttl")
            .unwrap()
            .as_i64()
            .unwrap();
        let y = b
            .as_object_mut()
            .unwrap()
            .remove("pttl")
            .unwrap()
            .as_i64()
            .unwrap();
        assert_eq!(a, b);
        assert!(x == y && x == -1 || x > 0 && y > 0 && y <= x);
    }
}

async fn held_reply(peer: &mut Peer) -> Value {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let value = peer.stats();
        assert_eq!(value["invalid"], json!([]));
        if value["seen"] == 1
            && value["rows"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r["kind"] == "backend-target-reply-read-held")
        {
            return value;
        }
        assert!(Instant::now() < deadline, "ActualLuaReplyReach15");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
async fn logical_completion(observed: &ObservedApi) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let value = observed.state();
        if value["logical_done"] == true {
            return value;
        }
        assert!(Instant::now() < deadline, "RealAdapterLogicalCompletion10");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
async fn original_caller_released(observed: &ObservedApi) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let value = observed.state();
        if value["Weak_Some"] == false
            && value["real_application_http_slots_available"] == 4
            && value["real_jobs_slots_available"] == 4
        {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "OriginalCallerAndActualSlotsDrain10"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
async fn run_case(number: u64) {
    assert_eq!(
        std::env::var("BACKUP_JOB_PHYSICAL_AUTHORITY").as_deref(),
        Ok("owned-backup-jobs-physical-only")
    );
    assert_eq!(
        std::env::var("BACKUP_JOB_PHYSICAL_CASE").unwrap(),
        number.to_string()
    );
    let database = std::env::var("POSTGRES_DB").unwrap();
    owned(&database);
    let root = PathBuf::from(std::env::var("BACKUP_JOB_PHYSICAL_ROOT").unwrap());
    let output = PathBuf::from(std::env::var("BACKUP_JOB_PHYSICAL_OUTPUT").unwrap());
    assert!(output.is_dir());
    let path = output.join("capture-private.json");
    assert!(!path.exists());
    let url = std::env::var("DATABASE_TEST_URL").unwrap();
    let check = Store::connect(&url, 1).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT current_database()")
            .fetch_one(check.pool())
            .await
            .unwrap(),
        database
    );
    check.pool().close().await;
    assert!(matches!(
        initialize_empty_database(&url).await.unwrap(),
        SchemaInitialization::Created { .. }
    ));
    let store = Store::connect(&url, 4).await.unwrap();
    let mut report = json!({"case":number,"passed":false,"capture_complete":false,"phase":"owned-seed","database":database,
        "production_changes":false,"Source_app_runs":0,"Main_runs":0,"worker_polls":0,"request_count":0,
        "weak_only_observer":true,"FD_loss_is_not_handler_abort":true,"initial_SQL":snapshot(&store).await});
    save(&path, &report);
    assert_eq!(
        report["initial_SQL"]["tables"]["accounts_user"],
        json!([]),
        "FreshOwnedActorTable"
    );
    let actor_key = format!("backup-physical56-key-{database}");
    sqlx::query("INSERT INTO accounts_user(id,password,username,first_name,last_name,email,user_level,is_active,is_staff,is_superuser,custom_properties,api_key,date_joined,stream_limit)VALUES(1,'!','owned-backup-physical','','','',10,true,false,false,'{}',$1,NOW(),0)").bind(&actor_key).execute(store.pool()).await.unwrap();
    sqlx::query("SELECT setval('accounts_user_id_seq',1,true)")
        .execute(store.pool())
        .await
        .unwrap();
    for (key, value) in [
        ("system_settings", json!({"time_zone":"UTC"})),
        ("network_access", json!({"UI":"127.0.0.0/8"})),
    ] {
        sqlx::query("INSERT INTO core_coresettings(key,name,value)VALUES($1,$1,$2)ON CONFLICT(key)DO UPDATE SET value=EXCLUDED.value").bind(key).bind(value).execute(store.pool()).await.unwrap();
    }
    let cache_url = "redis://:playback-fixture-only@127.0.0.1:6379/1";
    let broker = "redis://:playback-fixture-only@127.0.0.1:6379/2";
    let backend = "redis://:playback-fixture-only@127.0.0.1:6379/3";
    let task_id = "11111111-1111-4111-8111-111111111111";
    let task_key = format!("celery-tasks:celery-task-meta-{task_id}");
    assert_eq!(redis_value(broker, "celery").await["type"], "none");
    if number == 6 {
        let mut value: Value =
            serde_json::from_str(include_str!("backup_source16_vectors.json")).unwrap();
        value["actual_backend"]["task_id"] = json!(task_id);
        let client = redis::Client::open(backend).unwrap();
        let mut c = client.get_multiplexed_async_connection().await.unwrap();
        let ok: String = redis::cmd("SET")
            .arg(&task_key)
            .arg(serde_json::to_vec(&value["actual_backend"]).unwrap())
            .arg("EX")
            .arg(3600)
            .query_async(&mut c)
            .await
            .unwrap();
        assert_eq!(ok, "OK");
        report["Source16_result_seed_context"] = value["actual_backend"].clone();
    }
    let (mut peer, port) = Peer::start(
        &root,
        &output,
        &json!([if number == 5 { "publish" } else { "read" }]),
    );
    let routed = format!(
        "redis://:playback-fixture-only@127.0.0.1:{port}/{}",
        if number == 5 { 2 } else { 3 }
    );
    let backup_root = output.join("backup-root");
    let scratch = output.join("scratch");
    std::fs::create_dir(&backup_root).unwrap();
    std::fs::create_dir(&scratch).unwrap();
    let handler = ArchiveHandler::new_configured(
        dispatcharr_backups::PgConfig {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "dispatch".into(),
            database: database.clone(),
            password: Some("playback-fixture-only".into()),
            options: BTreeMap::new(),
        },
        dispatcharr_backups::Runtime {
            pg_dump: PathBuf::from("/usr/lib/postgresql/17/bin/pg_dump"),
            pg_restore: PathBuf::from("/usr/lib/postgresql/17/bin/pg_restore"),
            psql: PathBuf::from("/usr/bin/psql"),
        },
        backup_root.clone(),
        scratch,
        Arc::new(OwnUtcClock),
        dispatcharr_backups::Limits {
            database_bytes: 67108864,
            archive_bytes: 8388608,
            command_timeout: Duration::from_secs(90),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(crate::transport::Config::default().operations, 4);
    assert_eq!(
        crate::transport::Config::default().command_timeout,
        Duration::from_secs(2)
    );
    let application = BackupApplication::prepare(
        if number == 5 { &routed } else { broker },
        if number == 6 { &routed } else { backend },
        Arc::new(handler),
    )
    .await
    .unwrap();
    let observed = ObservedApi {
        real: application.clone(),
        observation: Arc::new(Mutex::new(Observation::default())),
    };
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("backup-physical-job-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(NotificationCache::connect(cache_url).await.unwrap());
    let files = BackupFiles::new(auth, backup_root, "backup-physical-job-fixture-only".into())
        .unwrap()
        .with_jobs(Arc::new(observed.clone()));
    let retained_files = files.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            files
                .router()
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async {
            let _ = stopped.await;
        })
        .await
    });
    report["before"] = raw_state(&store, cache_url, broker, backend, &task_key).await;
    report["phase"] = json!("request");
    report["request_count"] = json!(1);
    save(&path, &report);
    let start = Instant::now();
    if number == 5 {
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        let request=format!("POST /api/backups/create/ HTTP/1.1\r\nHost: localhost\r\nX-API-Key: {actor_key}\r\nContent-Type: application/json\r\nContent-Length: 1\r\nConnection: close\r\n\r\n{{");
        client.write_all(request.as_bytes()).await.unwrap();
        let held = held_reply(&mut peer).await;
        report["held"] = held.clone();
        report["before_FD_loss"] = observed.state();
        save(&path, &report);
        assert_eq!(
            report["before_FD_loss"]["logical_done"], false,
            "FDLossMustPrecedeActualLogicalCompletion"
        );
        let mut byte = [0u8; 1];
        let observed_read = client.try_read(&mut byte);
        report["pre_FD_loss_read"] = json!({"result":format!("{observed_read:?}"),"successful_HTTP_body_or_status_observed":false});
        assert!(matches!(observed_read,Err(ref e)if e.kind()==std::io::ErrorKind::WouldBlock));
        let fd = client.as_raw_fd();
        let old_link = std::fs::read_link(format!("/proc/self/fd/{fd}")).unwrap();
        client.shutdown().await.unwrap();
        drop(client);
        let after_link = std::fs::read_link(format!("/proc/self/fd/{fd}")).ok();
        report["actual_client_FD_receipt"] = json!({"fd":fd,"original_link":old_link.to_string_lossy(),"after_link":after_link.as_ref().map(|p|p.to_string_lossy().to_string()),"original_FD_absent_or_reused":after_link.as_ref()!=Some(&old_link),"namespace":"ELF container /proc/self"});
        assert_ne!(after_link.as_ref(), Some(&old_link));
        report["actual_client_FD_shutdown_and_drop"] = json!(true);
        report["handler_abort_claimed"] = json!(false);
        report["logical_completion"] = logical_completion(&observed).await;
    } else {
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap();
        let pending = tokio::spawn(async move {
            let response = client
                .get(format!("http://{address}/api/backups/status/{task_id}/"))
                .header("X-API-Key", actor_key)
                .send()
                .await
                .unwrap();
            let status = response.status().as_u16();
            let bytes = response.bytes().await.unwrap();
            (status, bytes)
        });
        let held = held_reply(&mut peer).await;
        report["held"] = held.clone();
        save(&path, &report);
        let (status, bytes) = tokio::time::timeout(Duration::from_secs(10), pending)
            .await
            .unwrap()
            .unwrap();
        assert!(bytes.len() <= 65536);
        report["HTTP_response_while_reply_held"] = json!({"status":status,"body_hex":hex(&bytes),"bytes":bytes.len(),"sha256":hash(&bytes),"Source_HTTP_error_parity_claimed":false});
        save(&path, &report);
        assert_eq!(
            status, 500,
            "PinnedNativeTransportTimeoutRendererPrediction"
        );
        report["logical_completion"] = logical_completion(&observed).await;
    }
    report["while_held_after_logical_completion"] = observed.state();
    report["held_raw_state"] = raw_state(&store, cache_url, broker, backend, &task_key).await;
    save(&path, &report);
    assert_eq!(
        report["while_held_after_logical_completion"]["Weak_Some"],
        true
    );
    assert_eq!(
        report["while_held_after_logical_completion"]["real_application_http_slots_available"],
        3
    );
    assert_eq!(
        report["while_held_after_logical_completion"]["real_jobs_slots_available"],
        3
    );
    assert_eq!(peer.stats()["seen"], 1);
    assert!(start.elapsed() <= Duration::from_secs(25), "DeclaredHold25");
    peer.release(0);
    report["after_physical_reply"] = original_caller_released(&observed).await;
    report["reply_after_release"] = peer.stats();
    report["after"] = raw_state(&store, cache_url, broker, backend, &task_key).await;
    save(&path, &report);
    assert_eq!(report["before"]["SQL"], report["after"]["SQL"]);
    assert_eq!(
        report["before"]["credentials"],
        report["after"]["credentials"]
    );
    if number == 5 {
        assert_eq!(
            report["held_raw_state"]["all_broker"]["rows"]
                .as_array()
                .unwrap()
                .len(),
            2,
            "OnlyQueueAndBindingPublished"
        );
        same_inventory(
            &report["held_raw_state"]["all_broker"],
            &report["after"]["all_broker"],
        );
        same_inventory(
            &report["before"]["all_backend"],
            &report["after"]["all_backend"],
        );
        let raw = report["held_raw_state"]["broker_queue"]["value"]["list_hex"]
            .as_array()
            .unwrap();
        assert_eq!(raw.len(), 1);
        let encoded = raw[0].as_str().unwrap();
        let bytes = (0..encoded.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&encoded[i..i + 2], 16).unwrap())
            .collect::<Vec<_>>();
        let id = crate::worker::create_identity(&bytes).unwrap();
        report["published_owned_task_id"] = json!(id.to_string());
        report["published_unconsumed_create_frame_sha256"] = json!(hash(&bytes));
        assert_eq!(
            report["held_raw_state"]["broker_queue"],
            report["after"]["broker_queue"]
        );
        assert_eq!(
            report["held_raw_state"]["binding"],
            report["after"]["binding"]
        );
    } else {
        same_inventory(
            &report["before"]["all_broker"],
            &report["after"]["all_broker"],
        );
        same_inventory(
            &report["before"]["all_backend"],
            &report["after"]["all_backend"],
        );
        assert_eq!(
            report["before"]["broker_queue"],
            report["after"]["broker_queue"]
        );
        assert_eq!(
            report["before"]["backend"]["DUMP_hex"],
            report["after"]["backend"]["DUMP_hex"]
        );
    }
    assert!(retained_files.shutdown(Duration::from_secs(10)).await);
    let _ = stop.send(());
    let waited = tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap();
    assert!(waited.is_ok());
    drop(retained_files);
    drop(observed);
    drop(application);
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let state = peer.stats();
        if state["connections"] == 0 {
            report["proxy_connections_absent_before_stop"] = state;
            break;
        }
        assert!(Instant::now() < until, "RealClientEndpointClose10");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    report["proxy_stop"] = peer.finish();
    assert_eq!(report["proxy_stop"]["listener_joined"], true);
    assert_eq!(report["proxy_stop"]["invalid"], json!([]));
    tokio::time::timeout(Duration::from_secs(10), store.pool().close())
        .await
        .unwrap();
    report["closed_pool_size"] = json!(store.pool().size());
    report["closed_pool_idle"] = json!(store.pool().num_idle());
    save(&path, &report);
    assert_eq!(store.pool().size(), 0);
    report["phase"] = json!("complete");
    report["capture_complete"] = json!(true);
    report["passed"] = json!(true);
    save(&path, &report);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Root-reviewed owned physical case5 controller; no Source or Main"]
async fn real_backup_publish_fd_loss_retains_original_caller_until_reply() {
    tokio::time::timeout(Duration::from_secs(90), run_case(5))
        .await
        .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Root-reviewed owned physical case6 controller; no Source or Main"]
async fn real_backup_status_logical_deadline_retains_original_caller_until_reply() {
    tokio::time::timeout(Duration::from_secs(90), run_case(6))
        .await
        .unwrap();
}
