//! Private test-only physical PostgreSQL witness. Original Dispatcharr and SQLx
//! contributors retain source/algorithm credit. Never enabled outside cfg(test).
use super::BackupFiles;
use crate::{
    initialize_empty_database, AuthConfig, AuthService, NotificationCache, SchemaInitialization,
    Store,
};
use axum::{body::Body, http::Request};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Read, Write},
    net::SocketAddr,
    os::{fd::AsRawFd, unix::process::ExitStatusExt},
    path::{Path as FsPath, PathBuf},
    process::{Child, Command, Stdio},
    sync::{mpsc, Arc},
    thread,
    time::{Duration, Instant},
};
use tokio::{io::AsyncWriteExt, net::TcpStream};
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
    let s = v.strip_prefix("rust_probe_backup_physical_").unwrap();
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
        let redis = stages[0] == "cache_system";
        let err = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(output.join("proxy.stderr"))
            .unwrap();
        let mut command = Command::new("/dispatcharrpy/bin/python");
        command.arg(root.join(if redis {
            "controller/owned_resp_proxy.py"
        } else {
            "controller/owned_pg_proxy.py"
        }));
        command
            .arg("--backend-port")
            .arg(if redis { "6379" } else { "5432" });
        command.arg("--contracts").arg(root.join(if redis {
            "inputs/resp-contracts.json"
        } else {
            "inputs/pg-contracts.json"
        }));
        if redis {
            command.arg("--case").arg("cache_system");
        } else {
            command.arg("--stages").arg(stages.to_string());
        }
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

// Owned test-only transport witness. A peer FD close does not imply that Hyper
// attempted a response write. Retain actual read/write/handler/drop observations.
#[derive(Default, serde::Serialize)]
struct HttpFate {
    accepted: usize,
    handler_started: usize,
    handler_cancelled: usize,
    response_statuses: Vec<u16>,
    bytes_read: usize,
    bytes_written: usize,
    read_eof: bool,
    write_errors: Vec<String>,
    socket_dropped: bool,
    capacity_exceeded: bool,
}
type SharedFate = Arc<std::sync::Mutex<HttpFate>>;
struct FateIo {
    stream: TcpStream,
    fate: SharedFate,
}
impl tokio::io::AsyncRead for FateIo {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let result = std::pin::Pin::new(&mut this.stream).poll_read(cx, buf);
        if let std::task::Poll::Ready(Ok(())) = &result {
            let n = buf.filled().len() - before;
            let mut fate = this.fate.lock().unwrap();
            fate.bytes_read += n;
            fate.read_eof |= n == 0;
            fate.capacity_exceeded |= fate.bytes_read > 65536;
        }
        result
    }
}
impl tokio::io::AsyncWrite for FateIo {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let result = std::pin::Pin::new(&mut this.stream).poll_write(cx, buf);
        match &result {
            std::task::Poll::Ready(Ok(n)) => {
                let mut fate = this.fate.lock().unwrap();
                fate.bytes_written += n;
                fate.capacity_exceeded |= fate.bytes_written > 65536;
            }
            std::task::Poll::Ready(Err(error)) => {
                let mut fate = this.fate.lock().unwrap();
                if fate.write_errors.len() < 8 {
                    fate.write_errors.push(format!("{:?}", error.kind()));
                } else {
                    fate.capacity_exceeded = true;
                }
            }
            std::task::Poll::Pending => {}
        }
        result
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}
impl Drop for FateIo {
    fn drop(&mut self) {
        self.fate.lock().unwrap().socket_dropped = true;
    }
}
struct FateListener {
    listener: tokio::net::TcpListener,
    fate: SharedFate,
}
impl axum::serve::Listener for FateListener {
    type Io = FateIo;
    type Addr = SocketAddr;
    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let (stream, address) = self.listener.accept().await.expect("OwnedHTTPAccept");
        self.fate.lock().unwrap().accepted += 1;
        (
            FateIo {
                stream,
                fate: self.fate.clone(),
            },
            address,
        )
    }
    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}
#[derive(Clone, Copy)]
struct FatePeer(SocketAddr);
impl axum::extract::connect_info::Connected<axum::serve::IncomingStream<'_, FateListener>>
    for FatePeer
{
    fn connect_info(stream: axum::serve::IncomingStream<'_, FateListener>) -> Self {
        Self(*stream.remote_addr())
    }
}
struct HandlerFate {
    fate: SharedFate,
    returned: bool,
}
impl Drop for HandlerFate {
    fn drop(&mut self) {
        if !self.returned {
            self.fate.lock().unwrap().handler_cancelled += 1;
        }
    }
}
fn held_duration(stats: &Value, case: u64) -> f64 {
    let (rows, start, end) = if case == 4 {
        (
            stats["rows"].as_array().unwrap(),
            "backend-target-reply-read-held",
            "backend-target-reply-released",
        )
    } else {
        (
            stats["events"].as_array().unwrap(),
            "ready-held",
            "ready-forwarded",
        )
    };
    let starts: Vec<_> = rows.iter().filter(|v| v["kind"] == start).collect();
    let ends: Vec<_> = rows.iter().filter(|v| v["kind"] == end).collect();
    assert_eq!(starts.len(), 1);
    assert_eq!(ends.len(), 1);
    let elapsed = ends[0]["monotonic"].as_f64().unwrap() - starts[0]["monotonic"].as_f64().unwrap();
    assert!(
        elapsed.is_finite() && (0.0..=25.0).contains(&elapsed),
        "ActualPhysicalHold25"
    );
    elapsed
}

// PRIVATE, UNEXECUTED. Cases1–4 only; caller abort and HTTP FD loss are distinct.
async fn run_callback_case() {
    assert_eq!(
        std::env::var("BACKUP_PHYSICAL_AUTHORITY").as_deref(),
        Ok("owned-native-only")
    );
    let case: u64 = std::env::var("BACKUP_PHYSICAL_CASE")
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=4).contains(&case));
    let database = std::env::var("POSTGRES_DB").unwrap();
    owned(&database);
    let root = PathBuf::from(std::env::var("BACKUP_PHYSICAL_INPUTS").unwrap());
    let out = PathBuf::from(std::env::var("BACKUP_PHYSICAL_OUTPUT").unwrap());
    assert!(out.is_dir());
    let report_path = out.join("callback-case.json");
    assert!(!report_path.exists());
    let url = std::env::var("DATABASE_URL").unwrap();
    assert!(matches!(
        initialize_empty_database(&url).await.unwrap(),
        SchemaInitialization::Created { .. }
    ));
    let observer = Store::connect(&url, 2).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT current_database()")
            .fetch_one(observer.pool())
            .await
            .unwrap(),
        database
    );
    sqlx::query("INSERT INTO core_coresettings(key,name,value)VALUES('system_settings','System',$1),('network_access','Network',$2) ON CONFLICT(key)DO UPDATE SET value=EXCLUDED.value")
        .bind(json!({"display_timezone":"UTC"})).bind(json!({"UI":"127.0.0.0/8"})).execute(observer.pool()).await.unwrap();
    let before = snapshot(&observer).await;
    let before_credentials = credentials(&observer).await;
    let stage = match case {
        1 | 3 => "system_ready",
        2 => "network_timeout_ready",
        4 => "cache_system",
        _ => unreachable!(),
    };
    let (mut peer, port) = Peer::start(&root, &out, &json!([stage]));
    let mut subject_url = url::Url::parse(&url).unwrap();
    if case != 4 {
        subject_url.set_port(Some(port)).unwrap();
        if case == 2 {
            subject_url
                .query_pairs_mut()
                .append_pair("options", "-c statement_timeout=2000");
        }
    }
    let subject = Store::connect(subject_url.as_str(), 4).await.unwrap();
    let mut auth = AuthService::new(
        subject.clone(),
        AuthConfig::new("backup-physical-only".into()).unwrap(),
    );
    if case == 2 || case == 4 {
        let mut cache_url = url::Url::parse(&std::env::var("BACKUP_CACHE_URL").unwrap()).unwrap();
        if case == 4 {
            cache_url.set_port(Some(port)).unwrap();
        }
        let cache = NotificationCache::connect(cache_url.as_str())
            .await
            .unwrap();
        if case == 2 {
            cache
                .settings_group("system_settings", || async {
                    Ok(json!({"display_timezone":"UTC"}))
                })
                .await
                .unwrap();
        }
        auth = auth.with_notification_cache(cache);
    }
    let files = BackupFiles::new(
        auth,
        PathBuf::from(format!("/data/backup-physical/{database}")),
        "backup-physical-only".into(),
    )
    .unwrap();
    // Read-only fixture setup: Parse/Describe under ACCESS EXCLUSIVE would
    // time out before Bind. Warm the exact statement on the actual sole idle
    // subject connection first, then require its real PID at the held reply.
    let prepared_network_backend = if case == 2 {
        let mut connection = subject.pool().acquire().await.unwrap();
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        let value: Option<Value> =
            sqlx::query_scalar("SELECT value FROM core_coresettings WHERE key=$1")
                .bind("network_access")
                .fetch_optional(&mut *connection)
                .await
                .unwrap();
        assert_eq!(value, Some(json!({"UI":"127.0.0.0/8"})));
        sqlx::Connection::ping(&mut *connection).await.unwrap();
        drop(connection);
        let idle_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert_eq!(
                subject.pool().size(),
                1,
                "ExactlyOnePreparedSubjectConnection"
            );
            if subject.pool().num_idle() == 1 {
                break;
            }
            assert!(
                Instant::now() < idle_deadline,
                "PreparedSubjectIdle5WithinCase90"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Some((pid, subject.pool().size(), subject.pool().num_idle()))
    } else {
        None
    };
    let mut lock = if case == 2 {
        let mut tx = observer.pool().begin().await.unwrap();
        sqlx::query("LOCK TABLE core_coresettings IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *tx)
            .await
            .unwrap();
        Some(tx)
    } else {
        None
    };
    let cache_before = cache(&std::env::var("BACKUP_CACHE_URL").unwrap()).await;
    let mut report = json!({"capture_complete":false,"passed":false,"capture_only":true,"case":case,"database":database,"physical_contract_unmeasured_before_capture":true,"before":before,"before_credentials":before_credentials,"Source_runs":0,"Main_runs":0});
    report["cache_before_request"] = cache_before;
    save(&report_path, &report);
    let request_path = if case == 2 {
        "/api/backups/"
    } else {
        "/api/backups/status/physical-owned-task/"
    };
    let address: SocketAddr = "127.0.0.1:32001".parse().unwrap();
    let mut server = None;
    let mut stop = None;
    let mut http_stream = None;
    let http_fate: SharedFate = Arc::new(std::sync::Mutex::new(HttpFate::default()));
    let mut caller = if case == 3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = listener.local_addr().unwrap();
        let handler_fate = http_fate.clone();
        let router = files.clone().router().layer(axum::middleware::from_fn(
            move |axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<FatePeer>,
                  mut request: Request<Body>,
                  next: axum::middleware::Next| {
                request
                    .extensions_mut()
                    .insert(axum::extract::ConnectInfo(peer.0));
                let fate = handler_fate.clone();
                async move {
                    fate.lock().unwrap().handler_started += 1;
                    let mut guard = HandlerFate {
                        fate: fate.clone(),
                        returned: false,
                    };
                    let response = next.run(request).await;
                    fate.lock()
                        .unwrap()
                        .response_statuses
                        .push(response.status().as_u16());
                    guard.returned = true;
                    response
                }
            },
        ));
        let listener = FateListener {
            listener,
            fate: http_fate.clone(),
        };
        let (signal, stopped) = tokio::sync::oneshot::channel();
        stop = Some(signal);
        server = Some(tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<FatePeer>(),
            )
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
        }));
        let mut stream = TcpStream::connect(bound).await.unwrap();
        stream
            .write_all(
                format!("GET {request_path} HTTP/1.1\r\nHost: owned\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        report["actual_HTTP_client_FD"] = json!(stream.as_raw_fd());
        http_stream = Some(stream);
        None
    } else {
        let request = Request::builder()
            .uri(request_path)
            .body(Body::empty())
            .unwrap();
        let runtime = files.clone();
        Some(tokio::spawn(async move {
            runtime.serve(address, request).await
        }))
    };
    let reached = if case == 4 {
        let until = Instant::now() + Duration::from_secs(15);
        loop {
            let state = peer.stats();
            assert_eq!(state["invalid"], json!([]));
            if state["seen"] == 1 {
                break state;
            }
            assert!(Instant::now() < until, "ActualRedisReplyReach15");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    } else {
        held(&mut peer, stage).await
    };
    report["actual_gate_reached"] = reached.clone();
    if let Some((pid, size, idle)) = prepared_network_backend {
        assert_eq!(
            reached["held"]["backend_pid"].as_i64(),
            Some(i64::from(pid))
        );
        report["actual_readonly_prepared_timeout_fixture"] = json!({"backend_pid":pid,"statement_prepared_before_lock":true,"held_reply_same_backend":true,"subject_pool_size_before_lock":size,"subject_pool_idle_before_lock":idle,"Source_business_request":false});
    }

    report["request_slots_while_held"] = json!(files.workers.available_permits());
    assert_eq!(files.workers.available_permits(), 3);
    let target_sockets = client_sockets(port);
    assert!(!target_sockets.as_array().unwrap().is_empty());
    report["actual_target_client_sockets"] = target_sockets.clone();
    if case == 1 || case == 2 {
        let handle = caller.take().unwrap();
        handle.abort();
        let joined = handle.await.unwrap_err();
        assert!(joined.is_cancelled());
        report["caller_JoinHandle_abort_consumed"] = json!(true);
        report["HTTP_FD_loss_claimed"] = json!(false);
    } else if case == 3 {
        let mut stream = http_stream.take().unwrap();
        stream.shutdown().await.unwrap();
        drop(stream);
        report["actual_HTTP_FD_shutdown_and_drop"] = json!(true);
        report["handler_abort_claimed"] = json!(false);
    } else {
        let response = tokio::time::timeout(Duration::from_secs(5), caller.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        report["logical_response_before_physical_reply"] =
            json!({"status":response.status().as_u16()});
        assert_eq!(response.status().as_u16(), 401);
        report["later_pipeline_write_arrival_claimed"] = json!(false);
    }
    assert_eq!(files.workers.available_permits(), 3);
    let early = files.shutdown(Duration::from_millis(30)).await;
    report["early_shutdown_returned"] = json!(early);
    assert!(!early);
    report["held_after_caller_loss_or_logical_completion"] = peer.stats();
    save(&report_path, &report);
    if let Some(tx) = lock.take() {
        tx.rollback().await.unwrap();
        report["owned_lock_released_before_Ready_forward"] = json!(true);
    }
    peer.release(0);
    let until = Instant::now() + Duration::from_secs(10);
    while files.workers.available_permits() != 4 {
        assert!(Instant::now() < until, "ExistingRequestSlotSettles10");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    report["final_shutdown_returned"] = json!(files.shutdown(Duration::from_secs(1)).await);
    assert_eq!(report["final_shutdown_returned"], true);
    if case != 4 {
        let pid = reached["held"]["backend_pid"].as_i64().unwrap();
        report["actual_target_endpoint_settlement"] =
            prior_endpoint_closed(&observer, &mut peer, pid, &target_sockets).await;
        assert_eq!(
            sqlx::query_scalar::<_, i32>("SELECT 1")
                .fetch_one(subject.pool())
                .await
                .unwrap(),
            1
        );
        report["same_SQL_pool_reuse_after_target_physical_close"] = json!(true);
    }
    if let Some(signal) = stop.take() {
        signal.send(()).unwrap();
    }
    if let Some(handle) = server.take() {
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        report["HTTP_server_graceful_wait0"] = json!(true);
        let fate = http_fate.lock().unwrap();
        report["actual_HTTP_response_write_fate"] = serde_json::to_value(&*fate).unwrap();
        assert_eq!(fate.accepted, 1);
        assert_eq!(fate.handler_started, 1);
        assert_eq!(fate.response_statuses.len() + fate.handler_cancelled, 1);
        assert!(fate.socket_dropped && !fate.capacity_exceeded);
        report["HTTP_write_failure_or_response_status_inferred_from_client_loss"] = json!(false);
    }
    report["cache_after_request"] = cache(&std::env::var("BACKUP_CACHE_URL").unwrap()).await;
    let cache_values = |value: &Value| {
        value["raw_DUMP_PTTL"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v["dump_hex"].clone()))
            .collect::<BTreeMap<_, _>>()
    };
    assert_eq!(
        cache_values(&report["cache_before_request"]),
        cache_values(&report["cache_after_request"])
    );
    report["after"] = snapshot(&observer).await;
    report["after_credentials"] = credentials(&observer).await;
    assert_eq!(report["before"], report["after"]);
    assert_eq!(report["before_credentials"], report["after_credentials"]);
    drop(files);
    subject.pool().close().await;
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let state = peer.stats();
        let done = if case == 4 {
            state["connections"] == 0
        } else {
            state["connections"]
                .as_array()
                .unwrap()
                .iter()
                .all(|r| r["closed"] == true && r["up_joined"] == true)
        };
        if done {
            report["before_proxy_stop_actual_connections_settled"] = state;
            break;
        }
        assert!(Instant::now() < until, "ActualRelayConnectionsSettle10");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    report["actual_physical_hold_seconds"] = json!(held_duration(&peer.stats(), case));
    report["proxy_final"] = peer.finish();
    report["capture_complete"] = json!(true);
    save(&report_path, &report);
    observer.pool().close().await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires sealed owned backup physical fixture"]
async fn backup_callbacks_actual_sql_and_cache_ownership() {
    tokio::time::timeout(Duration::from_secs(90), run_callback_case())
        .await
        .expect("FixedPhysicalCase90");
}
