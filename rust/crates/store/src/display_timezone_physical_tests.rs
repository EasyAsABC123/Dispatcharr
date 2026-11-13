//! Private test-only physical PostgreSQL witness. Original Dispatcharr and SQLx
//! contributors retain source/algorithm credit. Never enabled outside cfg(test).
use crate::{
    initialize_empty_database, AuthConfig, AuthService, NotificationCache, SchemaInitialization,
    Store,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;
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
    let s = v.strip_prefix("rust_probe_display_callback_").unwrap();
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
fn weak_alive(weak: &std::sync::Weak<tokio::sync::OwnedSemaphorePermit>) -> bool {
    // Upgrade only within this synchronous observation; no strong lease across await.
    weak.upgrade().is_some()
}
async fn leases_settled(
    auth: &AuthService,
    weak: &[std::sync::Weak<tokio::sync::OwnedSemaphorePermit>],
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while auth.user_agent_workers.available_permits() != 4
            || weak.iter().any(weak_alive)
            || auth
                .notification_cache
                .as_ref()
                .is_some_and(|c| c.physical_operations() != 0)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("ActualOriginalLeaseAndCacheSettlement10");
}
async fn read_target(observer: &Store, database: &str) -> Value {
    sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(t)),'[]'::jsonb) FROM (SELECT pid,state,wait_event_type,wait_event,query FROM pg_stat_activity WHERE datname=$1 AND query=$2 AND state='active') t")
        .bind(database).bind(super::SYSTEM_SQL).fetch_one(observer.pool()).await.unwrap()
}
async fn reach_locked(observer: &Store, database: &str, count: usize) -> Value {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let v = read_target(observer, database).await;
            if v.as_array().unwrap().len() == count
                && v.as_array()
                    .unwrap()
                    .iter()
                    .all(|r| r["wait_event_type"] == "Lock")
            {
                break v;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("ActualBoundTargetBackendLock15")
}
async fn run_case(case: u64) {
    assert_eq!(
        std::env::var("DISPLAY_CALLBACK_AUTHORITY").as_deref(),
        Ok("owned-native-callback-only")
    );
    let db = std::env::var("POSTGRES_DB").unwrap();
    owned(&db);
    let root = PathBuf::from(std::env::var("DISPLAY_CALLBACK_ROOT").unwrap());
    let out = PathBuf::from(std::env::var("DISPLAY_CALLBACK_OUTPUT").unwrap());
    assert!(out.is_dir());
    let path = out.join("callback-private.json");
    assert!(!path.exists());
    let url = std::env::var("DATABASE_TEST_URL").unwrap();
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
        db
    );
    sqlx::query("INSERT INTO core_coresettings(key,name,value)VALUES('system_settings','Owned callback system',$1)ON CONFLICT(key)DO UPDATE SET value=EXCLUDED.value").bind(json!({"time_zone":"Europe/London"})).execute(observer.pool()).await.unwrap();
    let before = snapshot(&observer).await;
    let credentials_before = credentials(&observer).await;
    let mut peer = None;
    let mut subject_url = url::Url::parse(&url).unwrap();
    subject_url
        .query_pairs_mut()
        .append_pair("application_name", "display-callback-subject");
    let mut redis_url =
        url::Url::parse(&std::env::var("DISPLAY_CALLBACK_CACHE_URL").unwrap()).unwrap();
    if case <= 4 {
        let stage = if case == 2 {
            "system_error_ready"
        } else if case >= 3 {
            "cache_system"
        } else {
            "system_ready"
        };
        let (p, port) = Peer::start(&root, &out, &json!([stage]));
        peer = Some(p);
        if case >= 3 {
            redis_url.set_port(Some(port)).unwrap();
        } else {
            subject_url.set_port(Some(port)).unwrap();
        }
    }
    let subject = Store::connect(subject_url.as_str(), 4).await.unwrap();
    let mut auth = AuthService::new(
        subject.clone(),
        AuthConfig::new("owned-callback-only".into()).unwrap(),
    );
    if (3..=4).contains(&case) {
        auth = auth.with_notification_cache(
            NotificationCache::connect(redis_url.as_str())
                .await
                .unwrap(),
        );
    }
    let prepared_pid = if case == 2 {
        // Prepare before ACCESS EXCLUSIVE: otherwise Parse fails before Bind.
        let mut c = subject.pool().acquire().await.unwrap();
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *c)
            .await
            .unwrap();
        let value = sqlx::query(super::SYSTEM_SQL)
            .bind("system_settings")
            .fetch_one(&mut *c)
            .await
            .unwrap();
        assert_eq!(
            value.try_get::<String, _>("value_text").unwrap(),
            r#"{"time_zone": "Europe/London"}"#
        );
        drop(c);
        tokio::time::timeout(Duration::from_secs(5), async {
            while subject.pool().num_idle() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        Some(pid)
    } else {
        None
    };
    let mut prepared_four_pids = Vec::new();
    if case == 5 {
        let mut warm = Vec::new();
        for _ in 0..4 {
            let mut c = subject.pool().acquire().await.unwrap();
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *c)
                .await
                .unwrap();
            let _ = sqlx::query(super::SYSTEM_SQL)
                .bind("system_settings")
                .fetch_one(&mut *c)
                .await
                .unwrap();
            prepared_four_pids.push(pid);
            warm.push(c);
        }
        drop(warm);
        tokio::time::timeout(Duration::from_secs(5), async {
            while subject.pool().num_idle() != 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    let mut lock = if case == 2 || case == 5 {
        let mut tx = observer.pool().begin().await.unwrap();
        sqlx::query("LOCK TABLE core_coresettings IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *tx)
            .await
            .unwrap();
        Some(tx)
    } else {
        None
    };
    let cache_before = cache(&std::env::var("DISPLAY_CALLBACK_CACHE_URL").unwrap()).await;
    let mut report = json!({"capture_complete":false,"passed":false,"capture_only":true,"case":case,"database":db,"Source_runs":0,"Main_runs":0,"before":before,"credentials_before":credentials_before,"raw_cache_before":cache_before,"IANA_ZoneInfo_catalogue_equivalent":false,"real_existing_writer_budget":4,"dummy_owner":false});
    save(&path, &report);
    if case <= 4 {
        let permit = Arc::new(auth.user_agent_workers.clone().try_acquire_owned().unwrap());
        let weak = Arc::downgrade(&permit);
        let scoped = auth.clone().with_request_writer_permit(permit).unwrap();
        let mut caller = None;
        let mut server = None;
        let mut signal = None;
        let mut http = None;
        let fate = Arc::new(std::sync::Mutex::new(HttpFate::default()));
        if case == 3 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let f = fate.clone();
            let io_fate = fate.clone();
            let once = Arc::new(std::sync::Mutex::new(Some(scoped)));
            let router = axum::Router::new().route(
                "/callback-owned-only",
                axum::routing::get(move || {
                    let auth = once
                        .lock()
                        .unwrap()
                        .take()
                        .expect("ExactlyOneActualHTTPCallback");
                    let f = f.clone();
                    async move {
                        f.lock().unwrap().handler_started += 1;
                        let mut guard = HandlerFate {
                            fate: f.clone(),
                            returned: false,
                        };
                        auth.request_started_display_timezone().await;
                        f.lock().unwrap().response_statuses.push(200);
                        guard.returned = true;
                        axum::http::StatusCode::OK
                    }
                }),
            );
            // Router captures an AuthService lease. Move to a one-shot handler
            // container so it cannot retain the original permit after request.
            let (stop, stopped) = tokio::sync::oneshot::channel();
            signal = Some(stop);
            server = Some(tokio::spawn(async move {
                axum::serve(
                    FateListener {
                        listener,
                        fate: io_fate,
                    },
                    router,
                )
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
            }));
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream.write_all(b"GET /callback-owned-only HTTP/1.1\r\nHost: owned\r\nConnection: close\r\n\r\n").await.unwrap();
            http = Some(stream);
        } else {
            caller = Some(tokio::spawn(async move {
                scoped.request_started_display_timezone().await;
            }));
        }
        if case == 2 {
            let locked = reach_locked(&observer, &db, 1).await;
            let pid = locked[0]["pid"].as_i64().unwrap() as i32;
            assert_eq!(Some(pid), prepared_pid);
            report["same_target_backend_before_cancel"] = locked;
            assert!(
                sqlx::query_scalar::<_, bool>("SELECT pg_cancel_backend($1)")
                    .bind(pid)
                    .fetch_one(observer.pool())
                    .await
                    .unwrap()
            );
        }
        let reached = if case >= 3 {
            tokio::time::timeout(Duration::from_secs(15), async {
                loop {
                    let s = peer.as_mut().unwrap().stats();
                    assert_eq!(s["invalid"], json!([]));
                    if s["seen"] == 1 {
                        break s;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap()
        } else {
            held(
                peer.as_mut().unwrap(),
                if case == 2 {
                    "system_error_ready"
                } else {
                    "system_ready"
                },
            )
            .await
        };
        report["held"] = reached;
        report["weak_original_owner_while_held"] = json!(weak_alive(&weak));
        report["writer_slots_while_held"] = json!(auth.user_agent_workers.available_permits());
        assert!(weak_alive(&weak));
        assert_eq!(auth.user_agent_workers.available_permits(), 3);
        save(&path, &report);
        if case == 1 {
            let t = caller.take().unwrap();
            t.abort();
            assert!(t.await.unwrap_err().is_cancelled());
            report["actual_observer_abort"] = json!(true);
        }
        if case == 3 {
            let mut stream = http.take().unwrap();
            report["actual_client_FD"] = json!(stream.as_raw_fd());
            stream.shutdown().await.unwrap();
            drop(stream);
            report["HTTP_FDloss_is_not_assumed_handler_abort"] = json!(true);
        }
        if case == 4 {
            tokio::time::timeout(Duration::from_secs(5), caller.take().unwrap())
                .await
                .unwrap()
                .unwrap();
            assert!(weak_alive(&weak));
            assert!(
                auth.notification_cache
                    .as_ref()
                    .unwrap()
                    .physical_operations()
                    > 0
            );
            report["logical_observer_returned_before_physical_reply"] = json!(true);
        }
        assert!(weak_alive(&weak));
        let observation_start = Instant::now();
        let mut sustained = Vec::new();
        for sample in 0..2 {
            if sample == 1 {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            let alive = weak_alive(&weak);
            let slots = auth.user_agent_workers.available_permits();
            let cache_operations = auth
                .notification_cache
                .as_ref()
                .map(|c| c.physical_operations())
                .unwrap_or(0);
            sustained.push(json!({"elapsed_monotonic_seconds":observation_start.elapsed().as_secs_f64(),"original_owner_weak_alive":alive,"writer_slots":slots,"actual_cache_physical_operations":cache_operations}));
            report["post_loss_or_logical_held_observations"] = json!(sustained);
            save(&path, &report);
            assert!(alive);
            assert_eq!(slots, 3);
            if case >= 3 {
                assert!(cache_operations > 0);
            }
        }
        peer.as_mut().unwrap().release(0);
        if let Some(tx) = lock.take() {
            tx.rollback().await.unwrap();
        }
        if let Some(c) = caller.take() {
            tokio::time::timeout(Duration::from_secs(5), c)
                .await
                .unwrap()
                .unwrap();
        }
        if let Some(s) = signal.take() {
            s.send(()).unwrap();
        }
        if let Some(s) = server.take() {
            tokio::time::timeout(Duration::from_secs(8), s)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        leases_settled(&auth, std::slice::from_ref(&weak)).await;
        report["weak_original_owner_after_settlement"] = json!(weak_alive(&weak));
        report["HTTP_fate"] = serde_json::to_value(&*fate.lock().unwrap()).unwrap();
        if case == 2 {
            assert_eq!(
                auth.display_timezone_name(),
                auth.display_timezone.environment
            );
            let before_stats = peer.as_mut().unwrap().stats();
            auth.request_started_display_timezone().await;
            let after_stats = peer.as_mut().unwrap().stats();
            assert_eq!(before_stats["events"], after_stats["events"]);
            report["error_preserved_zone_and_pretry_debounce"] = json!(true);
        }
        report["zone_after"] = json!(auth.display_timezone_name());
        if case != 2 {
            assert_eq!(auth.display_timezone_name(), "Europe/London");
        }
    } else if case == 5 {
        let mut weak = Vec::new();
        let mut actors = Vec::new();
        for _ in 0..4 {
            let p = Arc::new(auth.user_agent_workers.clone().try_acquire_owned().unwrap());
            weak.push(Arc::downgrade(&p));
            let a = auth.clone().with_request_writer_permit(p).unwrap();
            actors.push(tokio::spawn(async move {
                a.refresh_display_timezone(true).await;
            }));
        }
        let locked = reach_locked(&observer, &db, 4).await;
        let mut actual_pids: Vec<i32> = locked
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["pid"].as_i64().unwrap() as i32)
            .collect();
        actual_pids.sort();
        prepared_four_pids.sort();
        assert_eq!(actual_pids, prepared_four_pids);
        report["four_prepared_actual_backend_PIDs"] = json!(prepared_four_pids);
        assert_eq!(auth.user_agent_workers.available_permits(), 0);
        assert!(weak.iter().all(weak_alive));
        report["four_real_target_backends_held"] = locked;
        report["four_original_weak_alive"] = json!(true);
        save(&path, &report);
        lock.take().unwrap().rollback().await.unwrap();
        for a in actors {
            tokio::time::timeout(Duration::from_secs(5), a)
                .await
                .unwrap()
                .unwrap();
        }
        leases_settled(&auth, &weak).await;
        assert_eq!(auth.display_timezone_name(), "Europe/London");
        report["four_real_reused_tokens_settled"] = json!(true);
    } else {
        auth.set_display_timezone_on_settings_save(
            "system_settings",
            &json!({"time_zone":"Europe/Paris"}),
        );
        assert_eq!(auth.display_timezone_name(), "Europe/Paris");
        let checked = auth.display_timezone.state.lock().unwrap().checked;
        for value in [
            json!({"time_zone":"invalid zone"}),
            json!({"time_zone":false}),
            json!({}),
        ] {
            auth.set_display_timezone_on_settings_save("system_settings", &value);
            assert_eq!(auth.display_timezone_name(), "Europe/Paris");
            assert_eq!(auth.display_timezone.state.lock().unwrap().checked, checked);
        }
        auth.request_started_display_timezone().await;
        assert_eq!(auth.display_timezone_name(), "Europe/Paris");
        tokio::time::sleep(Duration::from_secs(5)).await;
        auth.request_started_display_timezone().await;
        assert_eq!(auth.display_timezone_name(), "Europe/London");
        report["setter_invalid_previous_zone_and_real_debounce"] = json!(true);
        report["exact_4999_5000_boundary_authority"] =
            json!("compiled paused-clock unit test; no wallclock exactness claim");
    }
    assert_eq!(snapshot(&observer).await, before);
    assert_eq!(credentials(&observer).await, credentials_before);
    report["after"] = snapshot(&observer).await;
    report["credentials_after"] = credentials(&observer).await;
    report["raw_cache_after"] = cache(&std::env::var("DISPLAY_CALLBACK_CACHE_URL").unwrap()).await;
    save(&path, &report);
    // While the ELF still lives, prove all subject backends physically gone.
    let close = subject.pool().close();
    tokio::time::timeout(Duration::from_secs(10), close)
        .await
        .expect("FirstPoolClose10");
    assert_eq!(subject.pool().size(), 0);
    report["closed_pool_size"] = json!(subject.pool().size());
    report["closed_pool_idle_diagnostic"] = json!(subject.pool().num_idle());
    report["writer_slots_after_settlement"] = json!(auth.user_agent_workers.available_permits());
    assert_eq!(auth.user_agent_workers.available_permits(), 4);
    drop(auth);
    if let Some(mut p) = peer {
        let drained = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let stats = p.stats();
                let settled = if case >= 3 {
                    stats["connections"] == 0
                } else {
                    stats["connections"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .all(|c| c["closed"] == true && c["up_joined"] == true)
                };
                if settled {
                    break stats;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("NaturalProxyConnectionDrainBeforeStop10");
        report["proxy_naturally_drained_before_stop"] = drained;
        let stats = p.finish();
        let (events, start, end) = if case >= 3 {
            (
                &stats["rows"],
                "backend-target-reply-read-held",
                "backend-target-reply-released",
            )
        } else {
            (&stats["events"], "ready-held", "ready-forwarded")
        };
        let starts: Vec<_> = events
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == start)
            .collect();
        let ends: Vec<_> = events
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == end)
            .collect();
        assert_eq!(starts.len(), 1);
        assert_eq!(ends.len(), 1);
        let held =
            ends[0]["monotonic"].as_f64().unwrap() - starts[0]["monotonic"].as_f64().unwrap();
        assert!(held.is_finite() && (0.0..=25.0).contains(&held));
        report["actual_target_hold_seconds"] = json!(held);
        if case >= 3 {
            assert_eq!(stats["invalid"], json!([]));
            assert_eq!(stats["listener_joined"], true);
        } else {
            assert_eq!(stats["errors"], json!([]));
            assert_eq!(stats["listener_alive"], false);
        }
        report["proxy_final"] = stats;
    }
    let own:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(t)),'[]'::jsonb) FROM (SELECT pid,state FROM pg_stat_activity WHERE datname=$1 AND application_name='display-callback-subject') t").bind(&db).fetch_one(observer.pool()).await.unwrap();
    assert_eq!(own, json!([]));
    report["while_ELF_alive_other_owned_SQL_backends"] = own;
    report["capture_complete"] = json!(true);
    report["passed"] = json!(true);
    save(&path, &report);
    observer.pool().close().await;
}
#[tokio::test]
#[ignore = "requires reviewed owned callback PG/Redis physical fixture"]
async fn real_callback_physical_case() {
    let case: u64 = std::env::var("DISPLAY_CALLBACK_CASE")
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=6).contains(&case));
    tokio::time::timeout(Duration::from_secs(90), run_case(case))
        .await
        .expect("FixedCase90");
}
