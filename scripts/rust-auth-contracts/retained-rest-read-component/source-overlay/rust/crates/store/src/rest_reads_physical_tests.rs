//! Native-only actual retained REST SQL ErrorResponse/Ready probes.
//! Dispatcharr/SQLx original contributors retain application/algorithm credit.
use crate::{AuthConfig, AuthService, NotificationCache, Store};
use dispatcharr_contracts::PlaybackRequest;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{Connection, Row};
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Read, Write},
    os::unix::process::ExitStatusExt,
    path::{Path as FsPath, PathBuf},
    process::{Child, Command, Stdio},
    sync::{mpsc, Arc},
    thread,
    time::{Duration, Instant},
};
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
    let s = v.strip_prefix("rust_probe_rest_reads_").unwrap();
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
        command.arg(root.join("controller/owned_pg_proxy.py"));
        command.arg("--backend-port").arg("5432");
        command
            .arg("--contracts")
            .arg(root.join("inputs/pg-contracts.json"));
        command.arg("--stages").arg(stages.to_string());
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
            &json!({"pid":self.child.id(),"exit":status.code(),"signal":status.signal(),"status_debug":format!("{status:?}"),"starttime":self.starttime,"wait":waited.code(),"stderr_bytes":stderr_bytes,"stderr_sha256":stderr_sha,"proc_PID_absent_after_wait":!std::path::Path::new(&format!("/proc/{}",self.child.id())).exists(),"stdout_reader_joined":true,"forced":false,"settled_utc":chrono::Utc::now()}),
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
                &json!({"pid":self.child.id(),"forced":true,"wait":status.as_ref().and_then(|s|s.code()),"signal":status.as_ref().and_then(|s|s.signal()),"status_debug":format!("{status:?}"),"starttime":self.starttime,"partial_failure_preserved":true,"proc_PID_absent_after_wait":!std::path::Path::new(&format!("/proc/{}",self.child.id())).exists(),"stdout_reader_joined":joined}),
            );
        }
    }
}
async fn held(peer: &mut Peer, stage: &str) -> Value {
    let until = Instant::now() + Duration::from_secs(15);
    loop {
        let stats = peer.stats();
        assert_eq!(stats["errors"], json!([]));
        if stats["held"]["stage"] == stage {
            return stats;
        }
        assert!(Instant::now() < until, "ActualStageReach15:{stage}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn weak_alive(w: &std::sync::Weak<tokio::sync::OwnedSemaphorePermit>) -> bool {
    w.upgrade().is_some()
}
async fn read_target(observer: &Store, db: &str, sql: &str) -> Value {
    sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(t)),'[]'::jsonb) FROM (SELECT pid,state,wait_event_type,wait_event,query FROM pg_stat_activity WHERE datname=$1 AND query=$2 AND state='active') t").bind(db).bind(sql).fetch_one(observer.pool()).await.unwrap()
}
async fn locked(observer: &Store, db: &str, sql: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let rows = read_target(observer, db, sql).await;
            if rows.as_array().unwrap().len() == 1 && rows[0]["wait_event_type"] == "Lock" {
                break rows;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("ActualExactTargetLock15")
}
async fn run_case(case: u64) {
    assert_eq!(
        std::env::var("REST_READS_AUTHORITY").as_deref(),
        Ok("owned-native-rest-read-only")
    );
    let db = std::env::var("POSTGRES_DB").unwrap();
    owned(&db);
    let root = PathBuf::from(std::env::var("REST_READS_ROOT").unwrap());
    let out = PathBuf::from(std::env::var("REST_READS_OUTPUT").unwrap());
    let path = out.join("rest-reads-private.json");
    assert!(!path.exists());
    let url = std::env::var("DATABASE_TEST_URL").unwrap();
    let observer = Store::connect(&url, 2).await.unwrap();
    let mut subject_url = url::Url::parse(&url).unwrap();
    subject_url
        .query_pairs_mut()
        .append_pair("application_name", "rest-read-subject");
    let stage = if case == 1 {
        "api_key_error_ready"
    } else {
        "network_error_ready"
    };
    let (mut peer, port) = Peer::start(&root, &out, &json!([stage]));
    subject_url.set_port(Some(port)).unwrap();
    let subject = Store::connect(subject_url.as_str(), 4).await.unwrap();
    let cache_url = std::env::var("REST_READS_CACHE_URL").unwrap();
    let auth = AuthService::new(
        subject.clone(),
        AuthConfig::new("owned-rest-physical-signing-only".into()).unwrap(),
    )
    .with_notification_cache(NotificationCache::connect(&cache_url).await.unwrap());
    let contracts: Value =
        serde_json::from_slice(&std::fs::read(root.join("inputs/pg-contracts.json")).unwrap())
            .unwrap();
    let target = contracts[stage]["sql"].as_str().unwrap();
    let key = "owned-rest-read-key-not-production";
    let before = snapshot(&observer).await;
    let credentials_before = credentials(&observer).await;
    let cache_before = cache(&cache_url).await;
    // Warm exact statement on the actual sole idle subject connection BEFORE the
    // exclusive lock. A Parse/Describe failure cannot satisfy the Ready contract.
    let mut warm = subject.pool().acquire().await.unwrap();
    let prepared_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *warm)
        .await
        .unwrap();
    let user_sql = format!(
        "SELECT {} FROM accounts_user WHERE api_key=$1 LIMIT 2",
        super::USER_COLUMNS
    );
    let row = sqlx::query(&user_sql)
        .bind(key)
        .bind(65536i32)
        .fetch_one(&mut *warm)
        .await
        .unwrap();
    assert_eq!(row.try_get::<i64, _>("id").unwrap(), 800001);
    let row = sqlx::query(super::GROUP_SQL)
        .bind("network_access")
        .bind(65536i32)
        .fetch_one(&mut *warm)
        .await
        .unwrap();
    assert_eq!(row.try_get::<String, _>("raw").unwrap(), "{}");
    warm.ping().await.unwrap();
    warm.return_to_pool().await;
    let mut lock = observer.pool().begin().await.unwrap();
    sqlx::query(if case == 1 {
        "LOCK TABLE accounts_user IN ACCESS EXCLUSIVE MODE"
    } else {
        "LOCK TABLE core_coresettings IN ACCESS EXCLUSIVE MODE"
    })
    .execute(&mut *lock)
    .await
    .unwrap();
    let original = Arc::new(auth.user_agent_workers.clone().try_acquire_owned().unwrap());
    let weak = Arc::downgrade(&original);
    let scoped = auth.clone().with_request_writer_permit(original).unwrap();
    let reads = scoped
        .retained_rest_reads(super::RestReadLimits::default())
        .unwrap();
    drop(scoped);
    let request = PlaybackRequest {
        channel_id: String::new(),
        query: vec![],
        headers: vec![("x-api-key".into(), key.into())],
        peer_ip: "127.0.0.1".parse().unwrap(),
    };
    let (deadline_tx, deadline_rx) = tokio::sync::oneshot::channel();
    let caller = tokio::spawn(async move {
        if case == 1 {
            let _ = reads.authenticate(&request).await;
            false
        } else {
            let user = reads
                .authenticate(&request)
                .await
                .unwrap()
                .expect("ActualValidKeyBeforeNetwork");
            assert_eq!(user.user().id, 800001);
            let work = reads.ui_network_allowed(request.peer_ip, Some(&user));
            tokio::pin!(work);
            tokio::select! {_=&mut work=>panic!("NetworkReturnedBeforeHeldReady"),_=deadline_rx=>{assert!(tokio::time::timeout(Duration::from_millis(100),&mut work).await.is_err());true}}
        }
    });
    let mut report = json!({"capture_only":true,"capture_complete":false,"passed":false,"case":case,"database":db,"Source_runs":0,"Main_runs":0,"real_existing_writer_budget":4,"dummy_owner":false,"before":before,"credentials_before":credentials_before,"raw_cache_before":cache_before,"prepared_actual_backend_pid":prepared_pid,"pool_max_connections":4,"physical_scope":"two opt-in read stages only; failed-auth event SQL/runtime shutdown unqualified"});
    save(&path, &report);
    let rows = locked(&observer, &db, target).await;
    assert_eq!(rows[0]["pid"].as_i64(), Some(prepared_pid as i64));
    report["same_target_backend_before_cancel"] = rows;
    save(&path, &report);
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT pg_cancel_backend($1)")
            .bind(prepared_pid)
            .fetch_one(observer.pool())
            .await
            .unwrap()
    );
    let reached = held(&mut peer, stage).await;
    assert_eq!(
        reached["held"]["backend_pid"].as_i64(),
        Some(prepared_pid as i64)
    );
    report["held"] = reached;
    if case == 1 {
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        report["actual_direct_observer_abort"] = json!(true);
        drop(deadline_tx);
    } else {
        deadline_tx.send(()).unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(2), caller)
            .await
            .unwrap()
            .unwrap());
        report["actual_logical_deadline_returned_before_ready"] = json!(true);
    }
    let began = Instant::now();
    let mut samples = Vec::new();
    for n in 0..2 {
        if n == 1 {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let alive = weak_alive(&weak);
        let slots = auth.user_agent_workers.available_permits();
        let stats = peer.stats();
        assert_eq!(stats["held"]["stage"], stage);
        assert_eq!(
            stats["held"]["backend_pid"].as_i64(),
            Some(prepared_pid as i64)
        );
        let target_still: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=$1 AND pid=$2)",
        )
        .bind(&db)
        .bind(prepared_pid)
        .fetch_one(observer.pool())
        .await
        .unwrap();
        samples.push(json!({"elapsed_monotonic_seconds":began.elapsed().as_secs_f64(),"original_owner_weak_alive":alive,"writer_slots":slots,"same_backend_still_exists":target_still,"held":stats["held"]}));
        report["post_loss_or_deadline_held_observations"] = json!(samples);
        save(&path, &report);
        assert!(alive && target_still);
        assert_eq!(slots, 3);
    }
    peer.release(0);
    lock.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while weak_alive(&weak)
            || auth.user_agent_workers.available_permits() != 4
            || auth
                .notification_cache
                .as_ref()
                .unwrap()
                .physical_operations()
                != 0
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("OriginalSlotAndCachePhysicalSettle10");
    report["original_owner_weak_after_settlement"] = json!(weak_alive(&weak));
    report["writer_slots_after_settlement"] = json!(auth.user_agent_workers.available_permits());
    // Prove real post-error pool reuse, not only a counter observation.
    let reuse: i32 = sqlx::query_scalar("SELECT 424242::int")
        .fetch_one(subject.pool())
        .await
        .unwrap();
    assert_eq!(reuse, 424242);
    report["actual_pool_reuse_value"] = json!(reuse);
    report["after"] = snapshot(&observer).await;
    report["credentials_after"] = credentials(&observer).await;
    report["raw_cache_after"] = cache(&cache_url).await;
    assert_eq!(report["before"], report["after"]);
    assert_eq!(report["credentials_before"], report["credentials_after"]);
    assert_eq!(report["raw_cache_before"], report["raw_cache_after"]);
    save(&path, &report);
    tokio::time::timeout(Duration::from_secs(10), subject.pool().close())
        .await
        .expect("FirstClose10");
    assert_eq!(subject.pool().size(), 0);
    report["closed_pool_size"] = json!(subject.pool().size());
    report["closed_pool_idle_diagnostic"] = json!(subject.pool().num_idle());
    assert!(subject.pool().num_idle() <= 4);
    drop(auth);
    let drained = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let s = peer.stats();
            if s["connections"]
                .as_array()
                .unwrap()
                .iter()
                .all(|r| r["closed"] == true && r["up_joined"] == true)
            {
                break s;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("NaturalPhysicalProxyDrain10");
    report["proxy_naturally_drained_before_stop"] = drained;
    let stats = peer.finish();
    assert_eq!(stats["errors"], json!([]));
    assert_eq!(stats["listener_alive"], false);
    let events = stats["events"].as_array().unwrap();
    let starts: Vec<_> = events
        .iter()
        .filter(|r| r["kind"] == "ready-held")
        .collect();
    let ends: Vec<_> = events
        .iter()
        .filter(|r| r["kind"] == "ready-forwarded")
        .collect();
    assert_eq!(starts.len(), 1);
    assert_eq!(ends.len(), 1);
    let hold = ends[0]["monotonic"].as_f64().unwrap() - starts[0]["monotonic"].as_f64().unwrap();
    assert!((0.0..=25.0).contains(&hold));
    report["actual_target_hold_seconds"] = json!(hold);
    report["proxy_final"] = stats;
    let backends:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(t)),'[]'::jsonb) FROM (SELECT pid,state FROM pg_stat_activity WHERE datname=$1 AND application_name='rest-read-subject') t").bind(&db).fetch_one(observer.pool()).await.unwrap();
    assert_eq!(backends, json!([]));
    report["while_ELF_alive_subject_SQL_backends"] = backends;
    report["capture_complete"] = json!(true);
    report["passed"] = json!(true);
    save(&path, &report);
    observer.pool().close().await;
}
#[tokio::test]
#[ignore = "requires reviewed owned Native REST ErrorReady PG/Redis fixture"]
async fn real_retained_rest_read_error_ready_case() {
    let case: u64 = std::env::var("REST_READS_CASE").unwrap().parse().unwrap();
    assert!((1..=2).contains(&case));
    tokio::time::timeout(Duration::from_secs(90), run_case(case))
        .await
        .expect("FixedCase90");
}
