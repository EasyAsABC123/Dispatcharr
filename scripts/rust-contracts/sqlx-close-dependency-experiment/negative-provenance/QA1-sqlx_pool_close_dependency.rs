//! Private pinned SQLx0.8.6 first-close contract regression, no Main/API changes.
//! SQLx contributors retain source/algorithm credit under vendored MIT/Apache2.
//! Original Dispatcharr contributors retain application/schema credit.
use dispatcharr_store::{initialize_empty_database, SchemaInitialization, Store};
use futures_util::FutureExt;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{
    collections::BTreeMap,
    future::Future,
    io::{BufRead, BufReader, Read, Write},
    os::unix::process::ExitStatusExt,
    panic::AssertUnwindSafe,
    path::{Path as FsPath, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc,
    },
    task::Poll,
    thread,
    time::{Duration, Instant},
};
use tokio::sync::Notify;
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
    "core_systemnotification",
    "core_notificationdismissal",
];
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn save(path: &FsPath, value: &Value) {
    let bytes = serde_json::to_vec_pretty(value).unwrap();
    assert!(bytes.len() <= 33554432);
    std::fs::write(path, bytes).unwrap();
}
fn owned(name: &str) {
    let suffix = name.strip_prefix("rust_probe_sqlx_close_").unwrap();
    assert_eq!(suffix.len(), 10);
    assert!(suffix
        .bytes()
        .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v)));
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
        let mut child = Command::new("/dispatcharrpy/bin/python")
            .arg(root.join("controller/owned_pg_proxy.py"))
            .arg("--backend-port")
            .arg("5432")
            .arg("--contracts")
            .arg(root.join("inputs/gate-contracts.json"))
            .arg("--stages")
            .arg(stages.to_string())
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
async fn backend_state(observer: &Store, pid: i64) -> Value {
    let rows:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(t)),'[]'::jsonb) FROM (SELECT pid,state,xact_start,backend_start,wait_event_type,wait_event,query FROM pg_stat_activity WHERE pid=$1) t").bind(pid as i32).fetch_one(observer.pool()).await.unwrap();
    rows
}
fn client_sockets(port: u16) -> Value {
    let fds: Vec<_> = std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| {
            std::fs::read_link(entry.path()).ok().map(|link| {
                (
                    entry.file_name().to_string_lossy().to_string(),
                    link.to_string_lossy().to_string(),
                )
            })
        })
        .collect();
    let text = std::fs::read_to_string("/proc/self/net/tcp").unwrap();
    let mut rows = Vec::new();
    for line in text.lines().skip(1) {
        let fields: Vec<_> = line.split_whitespace().collect();
        assert!(fields.len() >= 10);
        if fields[2].rsplit(':').next() == Some(format!("{port:04X}").as_str()) {
            let inode = fields[9];
            let link = format!("socket:[{inode}]");
            for (fd, actual) in &fds {
                if actual == &link {
                    rows.push(json!({"fd":fd,"inode":inode,"link":link,"proc_tcp_line":line}));
                }
            }
        }
    }
    json!(rows)
}
async fn prior_endpoint_closed(
    observer: &Store,
    peer: &mut Peer,
    pid: i64,
    sockets: &Value,
) -> Value {
    assert_eq!(
        sockets.as_array().unwrap().len(),
        1,
        "ActualMax1SQLxSocketIdentity"
    );
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let links: Vec<_> = std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| std::fs::read_link(entry.path()).ok())
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        let gone = sockets
            .as_array()
            .unwrap()
            .iter()
            .all(|s| !links.iter().any(|l| Some(l.as_str()) == s["link"].as_str()));
        let backend = backend_state(observer, pid).await;
        if gone && backend == json!([]) {
            return json!({"actual_prior_client_FDs":sockets,"original_socket_inodes_absent":true,"actual_backend_absent":backend,"actual_after_fd_links":links,"relay":peer.stats(),"test_relay_join_not_yet_claimed":true});
        }
        assert!(
            Instant::now() < until,
            "NoPrematurePoolSettlementBeforeActualSocketAndBackendClose"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
async fn physical_absent(observer: &Store, peer: &mut Peer) -> Value {
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let stats = peer.stats();
        let mut backends = Vec::new();
        let mut gone = true;
        for row in stats["connections"].as_array().unwrap() {
            if let Some(pid) = row["backend_pid"].as_i64() {
                let state = backend_state(observer, pid).await;
                gone &= state == json!([]);
                backends.push(json!({"pid":pid,"state":state}));
            }
            gone &= row["closed"] == true && row["up_joined"] == true;
        }
        if gone {
            return json!({"proxy":stats,"backend_absence":backends});
        }
        assert!(Instant::now() < until, "ActualPhysicalClose10");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
async fn observe_first_close(
    pool: &PgPool,
    observer: &Store,
    peer: &mut Peer,
    port: u16,
    old_sockets: &Value,
    output: &FsPath,
) -> Value {
    let started = Instant::now();
    let started_utc = chrono::Utc::now();
    let initial = json!({"pool_size":pool.size(),"num_idle":pool.num_idle(),"is_closed":pool.is_closed(),"observed_utc":chrono::Utc::now()});
    let sql_after_first_close = snapshot(observer).await;
    let mut ledger = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output.join("post-first-close-observations.jsonl"))
        .unwrap();
    let mut used = 0usize;
    let mut count = 0usize;
    loop {
        count += 1;
        assert!(count <= 128, "PostCloseObservationCount128");
        let began_utc = chrono::Utc::now();
        let metadata_before =
            json!({"size":pool.size(),"num_idle":pool.num_idle(),"is_closed":pool.is_closed()});
        let current_sockets = client_sockets(port);
        let links: Vec<_> = std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|p| std::fs::read_link(p.path()).ok())
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        assert!(links.len() <= 1024, "OwnFDObservationCapacity");
        let relay = peer.stats();
        let mut backends = Vec::new();
        let mut backends_absent = !relay["connections"].as_array().unwrap().is_empty();
        for connection in relay["connections"].as_array().unwrap() {
            if let Some(pid) = connection["backend_pid"].as_i64() {
                let state = backend_state(observer, pid).await;
                backends_absent &= state == json!([]);
                backends.push(json!({"connection":connection["id"],"pid":pid,"state":state}));
            } else {
                backends_absent = false;
                backends
                    .push(json!({"connection":connection["id"],"pid":null,"state":"UNOBSERVED"}));
            }
        }
        let old_fds_absent = old_sockets
            .as_array()
            .unwrap()
            .iter()
            .all(|r| !links.iter().any(|v| Some(v.as_str()) == r["link"].as_str()));
        let directions_joined = relay["connections"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["closed"] == true && r["up_joined"] == true);
        let metadata_after =
            json!({"size":pool.size(),"num_idle":pool.num_idle(),"is_closed":pool.is_closed()});
        let accepted = metadata_after["size"] == 0
            && current_sockets == json!([])
            && old_fds_absent
            && backends_absent
            && directions_joined
            && started.elapsed() <= Duration::from_secs(10);
        let row = json!({"ordinal":count,"began_utc":began_utc,"completed_utc":chrono::Utc::now(),"elapsed_millis":started.elapsed().as_millis(),"metadata_before":metadata_before,"metadata_after":metadata_after,"old_SQLx_socket_identity":old_sockets,"current_SQLx_sockets":current_sockets,"actual_self_FD_links":links,"backends":backends,"relay":relay,"old_FDs_absent":old_fds_absent,"all_backend_PIDs_absent":backends_absent,"all_relay_directions_joined":directions_joined,"accepted":accepted});
        let raw = serde_json::to_vec(&row).unwrap();
        used += raw.len() + 1;
        assert!(used <= 33554432, "PostCloseLedger32MiBCapacity");
        ledger.write_all(&raw).unwrap();
        ledger.write_all(b"\n").unwrap();
        ledger.flush().unwrap();
        if accepted || started.elapsed() >= Duration::from_secs(10) {
            return json!({"passed":accepted,"window_seconds":10,"started_utc":started_utc,"completed_utc":chrono::Utc::now(),"observations":count,"initial_metadata_before_observer_await":initial,"SQL_after_first_close_return":sql_after_first_close,"terminal":row,"application_statement_retries":0,"additional_close_calls":0,"ledger":"post-first-close-observations.jsonl"});
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn run_case(root: &FsPath, output: &FsPath, url: &str, number: usize) -> Value {
    std::fs::create_dir(output).unwrap();
    let path = output.join("case.json");
    let mut report = json!({"id":number,"passed":false,"Source_Main_or_HTTP_runs":0,"max_connections":1,"subject_close_calls":0,"application_statement_retries":0});
    save(&path, &report);
    assert!(matches!(
        initialize_empty_database(url).await.unwrap(),
        SchemaInitialization::Created { .. }
    ));
    let observer = Store::connect(url, 1).await.unwrap();
    let database = sqlx::query_scalar::<_, String>("SELECT current_database()")
        .fetch_one(observer.pool())
        .await
        .unwrap();
    owned(&database);
    report["database"] = json!(database);
    let before = snapshot(&observer).await;
    report["before"] = before.clone();
    save(&path, &report);
    let (mut peer, port) = Peer::start(root, output, &json!(["dependency-backend-id-ready"]));
    let mut proxy_url = url::Url::parse(url).unwrap();
    proxy_url.set_port(Some(port)).unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let hook_entered = entered.clone();
    let hook_release = release.clone();
    let hook_calls = calls.clone();
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .acquire_timeout(Duration::from_secs(5))
        .idle_timeout(None)
        .max_lifetime(None)
        .after_release(move |_connection, _metadata| {
            let entered = hook_entered.clone();
            let release = hook_release.clone();
            let calls = hook_calls.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::AcqRel);
                if number == 3 {
                    entered.notify_one();
                    release.notified().await;
                }
                Ok(true)
            })
        })
        .connect_lazy(proxy_url.as_str())
        .unwrap();
    let mut connection = pool.acquire().await.unwrap();
    let query = tokio::spawn(async move {
        let pid = sqlx::query_scalar::<_, i64>("SELECT pg_backend_pid()::bigint")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        (connection, pid)
    });
    let gate = held(&mut peer, "dependency-backend-id-ready").await;
    let sockets = client_sockets(port);
    assert_eq!(sockets.as_array().unwrap().len(), 1);
    let pid = gate["held"]["backend_pid"].as_i64().unwrap();
    let backend = backend_state(&observer, pid).await;
    assert_eq!(backend.as_array().unwrap().len(), 1);
    report["held_actual_select_ready"] =
        json!({"proxy":gate,"actual_SQLx_socket":sockets,"actual_backend":backend});
    save(&path, &report);
    peer.release(0);
    let (connection, returned_pid) = tokio::time::timeout(Duration::from_secs(15), query)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(returned_pid, pid);
    report["actual_query_task_joined"] = json!(true);
    if number == 1 {
        drop(connection);
        let until = Instant::now() + Duration::from_secs(5);
        while pool.num_idle() != 1 {
            assert!(Instant::now() < until, "HealthyIdleReturn5");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(calls.load(Ordering::Acquire), 1);
        report["before_first_close"] = json!({"is_closed":pool.is_closed(),"size":pool.size(),"num_idle":pool.num_idle(),"after_release_calls":calls.load(Ordering::Acquire),"healthy_idle_proven":true});
        save(&path, &report);
        report["subject_close_calls"] = json!(1);
        pool.close().await;
    } else if number == 2 {
        assert_eq!(calls.load(Ordering::Acquire), 0);
        let mut closing = Box::pin(pool.close());
        assert!(pool.is_closed());
        let prematurely_complete = std::future::poll_fn(|cx| {
            Poll::Ready(matches!(closing.as_mut().poll(cx), Poll::Ready(())))
        })
        .await;
        assert!(!prematurely_complete);
        report["before_connection_drop"] = json!({"closed":pool.is_closed(),"first_close_polled_pending":true,"after_release_calls":calls.load(Ordering::Acquire)});
        save(&path, &report);
        drop(connection);
        report["subject_close_calls"] = json!(1);
        closing.as_mut().await;
        drop(closing);
        assert_eq!(calls.load(Ordering::Acquire), 0);
    } else {
        assert_eq!(number, 3);
        drop(connection);
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert!(!pool.is_closed());
        let mut closing = Box::pin(pool.close());
        assert!(pool.is_closed());
        let prematurely_complete = std::future::poll_fn(|cx| {
            Poll::Ready(matches!(closing.as_mut().poll(cx), Poll::Ready(())))
        })
        .await;
        assert!(!prematurely_complete);
        report["actual_after_release_barrier"] = json!({"hook_entered_after_initial_closed_check":true,"first_close_marks_closed_before_hook_release":true,"same_first_close_future_polled_pending":true,"after_release_calls":calls.load(Ordering::Acquire),"no_sleep_as_ordering_authority":true});
        save(&path, &report);
        report["subject_close_calls"] = json!(1);
        release.notify_one();
        closing.as_mut().await;
        drop(closing);
    }
    report["first_close_returned_utc"] = json!(chrono::Utc::now());
    report["after_release_calls"] = json!(calls.load(Ordering::Acquire));
    save(&path, &report);
    let observation =
        observe_first_close(&pool, &observer, &mut peer, port, &sockets, output).await;
    report["first_close_observation"] = observation.clone();
    let after = snapshot(&observer).await;
    report["after"] = after.clone();
    report["all11_tables_and_sequences_unchanged"] = json!(before == after);
    report["first_close_contract_passed"] = observation["passed"].clone();
    save(&path, &report);
    // Explicit last-handle DROP is failure cleanup, never a second close or a
    // reinterpretation of the first-close endpoint obligation above.
    drop(pool);
    report["cleanup_after_last_pool_handle_drop"] =
        prior_endpoint_closed(&observer, &mut peer, pid, &sockets).await;
    report["cleanup_relay_backend_absence"] = physical_absent(&observer, &mut peer).await;
    report["peer_finish_after_physical_cleanup"] = peer.finish();
    observer.pool().close().await;
    report["observer_pool_first_close_metadata"] = json!({"size":observer.pool().size(),"num_idle":observer.pool().num_idle(),"closed":observer.pool().is_closed(),"external_all_target_backend_absence_required_after_ELF_exit":true});
    report["passed"] = json!(observation["passed"] == true && before == after);
    report["pool_drop_cleanup_cannot_waive_first_close_negative"] = json!(true);
    save(&path, &report);
    report
}
#[tokio::test]
#[ignore = "reviewed pinned SQLx dependency-only owned PG fixture; no Main/API or default data"]
async fn first_close_three_cases() {
    assert_eq!(
        std::env::var("SQLX_CLOSE_DEPENDENCY_FIXTURE").as_deref(),
        Ok("owned-dependency-only")
    );
    let root = PathBuf::from(std::env::var("SQLX_CLOSE_ROOT").unwrap());
    let output = PathBuf::from(std::env::var("SQLX_CLOSE_OUTPUT").unwrap());
    assert!(output.is_dir());
    let variant = std::env::var("SQLX_CLOSE_VARIANT").unwrap();
    assert!(matches!(variant.as_str(), "baseline" | "candidate"));
    let path = output.join("dependency-report.json");
    assert!(!path.exists());
    let databases: Vec<String> =
        serde_json::from_str(&std::env::var("SQLX_CLOSE_DATABASES").unwrap()).unwrap();
    assert_eq!(databases.len(), 3);
    let unique: std::collections::BTreeSet<_> = databases.iter().collect();
    assert_eq!(unique.len(), 3);
    for name in &databases {
        owned(name);
    }
    let mut report = json!({"passed":false,"variant":variant,"Source_Main_API_runs":0,"case_order":[1,2,3],"cases":[],"scope":"SQLx0.8.6 documented first-close contract max1; no production Main shutdown claim"});
    save(&path, &report);
    for (number, database) in databases.iter().enumerate() {
        let url = format!("postgres://dispatch:playback-fixture-only@127.0.0.1:5432/{database}");
        let case = AssertUnwindSafe(run_case(
            &root,
            &output.join(format!("case-{}", number + 1)),
            &url,
            number + 1,
        ))
        .catch_unwind();
        match tokio::time::timeout(Duration::from_secs(90), case).await {
            Ok(Ok(value)) => report["cases"].as_array_mut().unwrap().push(value),
            Ok(Err(_)) => {
                report["infrastructure_or_case_panic"] = json!({"case":number+1,"remaining_cases":"UNEXECUTED","raw_panic_hook_retained":true});
                save(&path, &report);
                panic!("DependencyCasePanicNoRetry")
            }
            Err(_) => {
                report["infrastructure_timeout"] =
                    json!({"case":number+1,"seconds":90,"remaining_cases":"UNEXECUTED"});
                save(&path, &report);
                panic!("DependencyCaseTimeoutNoRetry")
            }
        }
        save(&path, &report);
    }
    assert_eq!(report["cases"].as_array().unwrap().len(), 3);
    let all = report["cases"]
        .as_array()
        .unwrap()
        .iter()
        .all(|r| r["passed"] == true);
    report["passed"] = json!(all);
    save(&path, &report);
    assert!(all, "FirstCloseContractNegativePreservedNoForcedGreen");
}
