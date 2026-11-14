//! Four new actual unsigned-fetch ownership gates. Test-only; no Main/Source.
//! Original Dispatcharr and SQLx authors retain implementation/algorithm credit.
#![allow(clippy::too_many_arguments)]
use super::*;
use crate::{AuthConfig, NotificationCache, Store};
use futures_util::{FutureExt, StreamExt};
use sha2::{Digest, Sha256};
use sqlx::Connection;
use std::{
    collections::BTreeMap,
    future::IntoFuture,
    io::{BufRead, BufReader, Read, Write},
    os::unix::{io::AsRawFd, process::ExitStatusExt},
    path::{Path as FsPath, PathBuf},
    process::{Child, Command, Stdio},
    sync::{mpsc, Arc, Mutex, Weak},
    thread,
    time::{Duration, Instant},
};
use tower::ServiceExt;
const NOTICE: &str = "core_systemnotification";
const DISMISS: &str = "core_notificationdismissal";
const TABLES: [&str; 13] = [
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
    "plugins_pluginrepo",
    "plugins_pluginconfig",
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
    let s = v.strip_prefix("rust_probe_pluginrepo_").unwrap();
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
async fn cache_all(cache_url: &str) -> Value {
    let mut values = Vec::new();
    for database in 0..4 {
        let mut address = reqwest::Url::parse(cache_url).unwrap();
        address.set_path(&format!("/{database}"));
        values.push(json!({"database":database,"inventory":cache(address.as_str()).await}));
    }
    json!(values)
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
        assert!(
            starttime
                .as_ref()
                .is_some_and(|s| s.parse::<u64>().is_ok_and(|v| v > 0)),
            "ObservedProxyPIDStartBeforeReceipt"
        );
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

type OriginalWeak = Weak<tokio::sync::OwnedSemaphorePermit>;
type Admissions = Arc<Mutex<Vec<OriginalWeak>>>;
fn weak_alive(w: &OriginalWeak) -> bool {
    w.upgrade().is_some()
}
fn admitted(observed: &Admissions) -> Vec<bool> {
    observed.lock().unwrap().iter().map(weak_alive).collect()
}
fn request(method: &str, path: &str, key: Option<&str>, body: String) -> Request {
    let mut r = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(k) = key {
        r = r.header("x-api-key", k);
    }
    let mut r = r.body(axum::body::Body::from(body)).unwrap();
    r.extensions_mut().insert(ConnectInfo(
        "127.0.0.1:30000".parse::<SocketAddr>().unwrap(),
    ));
    r
}
async fn actual_frame(
    app: Router,
    request: Request,
    expected: u16,
    out: &FsPath,
    ordinal: usize,
) -> axum::body::Bytes {
    let response = app.oneshot(request).await.unwrap();
    let status = response.status().as_u16();
    let headers: Vec<_> = response
        .headers()
        .iter()
        .map(|(k, v)| (k.as_str().to_owned(), hex(v.as_bytes())))
        .collect();
    let path = out.join(format!("actual-response-frame-{ordinal}.json"));
    save(
        &path,
        &json!({"ordinal":ordinal,"status":status,"headers_value_hex":headers,"frame_observed":false,"capture_complete":false}),
    );
    let mut stream = response.into_body().into_data_stream();
    let original = stream.next().await.expect("ActualResponseFrame").unwrap();
    save(
        &path,
        &json!({"ordinal":ordinal,"status":status,"headers_value_hex":headers,"frame_observed":true,"frame_bytes":original.len(),"frame_hex":hex(&original),"frame_sha256":hash(&original),"scope":"one actual retained response data frame, not a copied replacement or TCP-body claim","capture_complete":true}),
    );
    assert_eq!(status, expected);
    assert!(!original.is_empty() && original.len() <= RESPONSE_LIMIT);
    let clone = original.clone();
    drop(original);
    drop(stream);
    clone
}

async fn settle(auth: &AuthService, observed: &Admissions) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while admitted(observed).iter().any(|v| *v)
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
    .expect("ActualOriginalFrameAndPhysicalOperationSettlement10");
}
async fn pool_finish(subject: &Store) -> Value {
    let reuse: i32 = sqlx::query_scalar("SELECT 424242::int")
        .fetch_one(subject.pool())
        .await
        .unwrap();
    assert_eq!(reuse, 424242);
    tokio::time::timeout(Duration::from_secs(10), subject.pool().close())
        .await
        .expect("FirstClose10");
    let size = subject.pool().size();
    let idle = subject.pool().num_idle();
    assert_eq!(size, 0);
    assert!(idle <= 4);
    json!({"actual_pool_reuse_value":reuse,"closed_pool_size":size,"closed_pool_idle_diagnostic":idle,"pool_max_connections":4,"after_ELF_backend_observer":"external controller; not this while-alive counter"})
}
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
    stream: tokio::net::TcpStream,
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
        let available = buf.remaining();
        let result = std::pin::Pin::new(&mut this.stream).poll_read(cx, buf);
        if let std::task::Poll::Ready(Ok(())) = &result {
            let n = buf.filled().len() - before;
            let mut fate = this.fate.lock().unwrap();
            fate.bytes_read += n;
            fate.read_eof |= n == 0 && available > 0;
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

fn source_key(reference: &Value) -> String {
    let users = reference["seed"]["users"].as_array().unwrap();
    let found: Vec<_> = users
        .iter()
        .filter(|u| u["id"] == 101 && u["user_level"] == 10 && u["custom_properties"] == json!({}))
        .collect();
    assert_eq!(found.len(), 1);
    found[0]["api_key"].as_str().unwrap().to_owned()
}
fn reference(root: &FsPath) -> Value {
    let raw = std::fs::read(root.join("inputs/Source24-private.json")).unwrap();
    assert!(raw.len() <= 67108864);
    assert_eq!(
        hash(&raw),
        std::env::var("PLUGIN_UNSIGNED_SOURCE_SHA256").unwrap()
    );
    let v: Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(v["actual_request_count"], 24);
    assert_eq!(v["force_authenticate_used"], false);
    v
}
type RequestCounter = Arc<std::sync::atomic::AtomicUsize>;
type Adapter = (AuthService, Router, Admissions, RequestCounter);
async fn adapter(subject: &Store, cache_url: &str, reference: &Value) -> Adapter {
    let auth = AuthService::new(
        subject.clone(),
        AuthConfig::new("owned-unsigned-remaining-signing-only".into()).unwrap(),
    )
    .with_notification_cache(NotificationCache::connect(cache_url).await.unwrap());
    let value = reference["seed"]["settings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["key"] == "system_settings")
        .unwrap()["value"]
        .clone();
    auth.set_display_timezone_on_settings_save("system_settings", &value);
    let admissions: Admissions = Arc::new(Mutex::new(Vec::new()));
    let observed = admissions.clone();
    let count: RequestCounter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = count.clone();
    let app = PluginRepositories::new(auth.clone())
        .with_unsigned_manifest_fetches()
        .observe_original_writer(Arc::new(move |weak| {
            let mut rows = observed.lock().unwrap();
            assert!(rows.len() < 8);
            rows.push(weak);
        }))
        .router()
        .layer(axum::middleware::from_fn(
            move |request: Request, next: axum::middleware::Next| {
                let count = counted.clone();
                async move {
                    let n = count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    assert!(n < 6);
                    next.run(request).await
                }
            },
        ));
    assert_eq!(auth.user_agent_workers.available_permits(), 4);
    (auth, app, admissions, count)
}
async fn cue(path: &FsPath, seconds: u64) {
    let end = Instant::now() + Duration::from_secs(seconds);
    while !path.is_file() {
        assert!(Instant::now() < end, "FiniteFileCue:{}", path.display());
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
async fn owner_samples(auth: &AuthService, observed: &Admissions) -> Value {
    let began = Instant::now();
    let mut samples = Vec::new();
    for index in 0..2 {
        if index == 1 {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let live = admitted(observed);
        let slots = auth.user_agent_workers.available_permits();
        assert_eq!(live, vec![true]);
        assert_eq!(slots, 3);
        samples.push(json!({"elapsed_seconds":began.elapsed().as_secs_f64(),"original_weak":live,"available_original_four_slots":slots,"physical_cache_operations":auth.notification_cache.as_ref().unwrap().physical_operations()}));
    }
    json!(samples)
}
async fn close_subject(
    subject: &Store,
    observer: &Store,
    auth: &AuthService,
    observed: &Admissions,
) -> Value {
    settle(auth, observed).await;
    let physical = pool_finish(subject).await;
    let rows:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(t)),'[]'::jsonb) FROM(SELECT pid,state,application_name FROM pg_stat_activity WHERE datname=current_database() AND application_name='unsigned-remaining-subject')t").fetch_one(observer.pool()).await.unwrap();
    assert_eq!(rows, json!([]));
    json!({"first_close":physical,"independent_while_ELF_subject_backend_rows":rows,"same_original_four_slots":auth.user_agent_workers.available_permits(),"original_weak_after_settlement":admitted(observed)})
}
async fn finish_report(
    out: &FsPath,
    subject: &Store,
    observer: &Store,
    auth: &AuthService,
    observed: &Admissions,
    mut report: Value,
) {
    report["physical_close"] = close_subject(subject, observer, auth, observed).await;
    report["after"] = snapshot(observer).await;
    report["credentials_after"] = credentials(observer).await;
    tokio::time::timeout(Duration::from_secs(10), observer.pool().close())
        .await
        .expect("FirstObserverClose10");
    assert_eq!(observer.pool().size(), 0);
    let proxy_pending = matches!(report["case"].as_u64(), Some(2 | 3));
    report["proxy_finish_pending"] = json!(proxy_pending);
    report["capture_complete"] = json!(!proxy_pending);
    report["passed"] = json!(!proxy_pending);
    report["completed_utc"] = json!(Utc::now());
    save(
        &out.join("unsigned-remaining-physical-private.json"),
        &report,
    );
    save(
        &out.join("pool-closed-ready.json"),
        &json!({"passed":true,"subject_pool_size":subject.pool().size(),"subject_pool_closed":subject.pool().is_closed(),"subject_pool_idle_diagnostic":subject.pool().num_idle(),"max":4,"independent_while_ELF_subject_backends":report["physical_close"]["independent_while_ELF_subject_backend_rows"]}),
    );
    cue(&out.join("backend-observer-done"), 15).await;
}
async fn frame_capacity_case(
    root: &FsPath,
    out: &FsPath,
    subject: &Store,
    observer: &Store,
    cache_url: &str,
    reference: &Value,
) {
    let (auth, app, observed, calls) = adapter(subject, cache_url, reference).await;
    observe_http(out, &observed, &auth);
    let key = source_key(reference);
    let before = snapshot(observer).await;
    let credentials_before = credentials(observer).await;
    let mut frames = Vec::new();
    let mut evidence = Vec::new();
    for index in 0..4 {
        let request = match index {
            0 | 1 => request(
                "POST",
                "/api/plugins/repos/preview/",
                Some(&key),
                json!({"url":format!("http://11.253.250.2:18080/physical/frame-{}.json",index+1)})
                    .to_string(),
            ),
            2 => request("POST", "/api/plugins/repos/preview/", None, "{}".into()),
            _ => {
                let mut r = request("POST", "/api/plugins/repos/preview/", None, "{}".into());
                r.headers_mut()
                    .insert("authorization", "Bearer too many pieces".parse().unwrap());
                r
            }
        };
        let frame = actual_frame(
            app.clone(),
            request,
            if index < 2 { 200 } else { 401 },
            out,
            index + 1,
        )
        .await;
        let live = admitted(&observed);
        let slots = auth.user_agent_workers.available_permits();
        assert_eq!(live, vec![true; index + 1]);
        assert_eq!(slots, 3 - index);
        evidence.push(json!({"ordinal":index+1,"actual_data_frame_bytes":frame.len(),"actual_data_frame_sha256":hash(&frame),"original_weak":live,"slots":slots}));
        frames.push(frame);
        save(
            &out.join("frames-partial.json"),
            &json!({"capture_complete":false,"actual_first_four_progress":evidence}),
        );
    }
    let before_fifth = snapshot(observer).await;
    let credentials_fifth_before = credentials(observer).await;
    let cache_before = cache_all(cache_url).await;
    let fifth = app
        .clone()
        .oneshot(request(
            "POST",
            "/api/plugins/repos/preview/",
            Some("owned-invalid-key-capacity-must-refuse-before-auth"),
            json!({"url":"http://11.253.250.2:18080/physical/forbidden-fifth.json"}).to_string(),
        ))
        .await
        .unwrap();
    let fifth_status = fifth.status().as_u16();
    let fifth_body = to_bytes(fifth.into_body(), RESPONSE_LIMIT).await.unwrap();
    assert_eq!(fifth_status, 503);
    let after_fifth = snapshot(observer).await;
    let credentials_fifth_after = credentials(observer).await;
    let cache_after = cache_all(cache_url).await;
    assert_eq!(before_fifth, after_fifth);
    assert_eq!(credentials_fifth_before, credentials_fifth_after);
    assert_eq!(admitted(&observed), vec![true; 4]);
    assert_eq!(auth.user_agent_workers.available_permits(), 0);
    let clone = frames[0].clone();
    drop(frames.remove(0));
    let clone_kept = json!({"actual_clone_bytes":clone.len(),"original_weak":admitted(&observed),"slots":auth.user_agent_workers.available_permits()});
    assert_eq!(admitted(&observed), vec![true; 4]);
    assert_eq!(auth.user_agent_workers.available_permits(), 0);
    drop(clone);
    assert_eq!(admitted(&observed), vec![false, true, true, true]);
    assert_eq!(auth.user_agent_workers.available_permits(), 1);
    let sixth = actual_frame(
        app.clone(),
        request(
            "POST",
            "/api/plugins/repos/preview/",
            Some(&key),
            json!({"url":"http://11.253.250.2:18080/physical/frame-reuse.json"}).to_string(),
        ),
        200,
        out,
        6,
    )
    .await;
    let reuse = json!({"body_hex":hex(&sixth),"body_sha256":hash(&sixth),"slots":auth.user_agent_workers.available_permits(),"original_weak":admitted(&observed)});
    assert_eq!(admitted(&observed), vec![false, true, true, true, true]);
    assert_eq!(auth.user_agent_workers.available_permits(), 0);
    drop(sixth);
    drop(frames);
    settle(&auth, &observed).await;
    assert_eq!(snapshot(observer).await, before);
    assert_eq!(credentials(observer).await, credentials_before);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 6);
    let report = json!({"capture_complete":false,"passed":false,"case":4,"actual_requests":calls.load(std::sync::atomic::Ordering::SeqCst),"actual_statuses":[200,200,401,401,503,200],"Source_requests":0,"Main":false,"before":before,"credentials_before":credentials_before,"actual_first_four":evidence,"fifth_status":fifth_status,"fifth_body_hex":hex(&fifth_body),"fifth_body_sha256":hash(&fifth_body),"fifth_SQL_before":before_fifth,"fifth_SQL_after":after_fifth,"fifth_credentials_before":credentials_fifth_before,"fifth_credentials_after":credentials_fifth_after,"fifth_raw_cache_before":cache_before,"fifth_raw_cache_after":cache_after,"clone_kept_after_original_drop":clone_kept,"sixth_reuse":reuse,"origin_contacts_expected":3,"Root_controller_raw_Origin_no_fifth_contact_required":true,"root_input_role":root});
    finish_report(out, subject, observer, &auth, &observed, report).await;
    *HTTP_OBSERVER.lock().unwrap() = None;
}
fn observe_http(out: &FsPath, observed: &Admissions, auth: &AuthService) {
    let path = out.to_path_buf();
    let weak = observed.clone();
    let workers = auth.user_agent_workers.clone();
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    *HTTP_OBSERVER.lock().unwrap() = Some(Arc::new(move |mut event| {
        let ordinal = count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        assert!(ordinal < 8);
        event["all_original_weak_at_event"] = json!(admitted(&weak));
        event["actual_four_slots_available_at_event"] = json!(workers.available_permits());
        save(
            &path.join(format!("unsigned-http-event-{ordinal}.json")),
            &event,
        );
    }));
}
async fn frontend_loss_case(
    root: &FsPath,
    out: &FsPath,
    subject: &Store,
    observer: &Store,
    cache_url: &str,
    reference: &Value,
) {
    let (auth, app, observed, calls) = adapter(subject, cache_url, reference).await;
    observe_http(out, &observed, &auth);
    let key = source_key(reference);
    let before = snapshot(observer).await;
    let credentials_before = credentials(observer).await;
    let before_cache = cache_all(cache_url).await;
    let fate: SharedFate = Arc::new(Mutex::new(HttpFate::default()));
    let f = fate.clone();
    let app = app
        .layer(axum::middleware::from_fn(
            move |request: Request, next: axum::middleware::Next| {
                let f = f.clone();
                async move {
                    f.lock().unwrap().handler_started += 1;
                    let mut guard = HandlerFate {
                        fate: f.clone(),
                        returned: false,
                    };
                    let response = next.run(request).await;
                    f.lock()
                        .unwrap()
                        .response_statuses
                        .push(response.status().as_u16());
                    guard.returned = true;
                    response
                }
            },
        ))
        .layer(axum::Extension(ConnectInfo(
            "127.0.0.1:30000".parse::<SocketAddr>().unwrap(),
        )));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(
        axum::serve(
            FateListener {
                listener,
                fate: fate.clone(),
            },
            app,
        )
        .with_graceful_shutdown(async move {
            let _ = rx.await;
        })
        .into_future(),
    );
    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    let fd = client.as_raw_fd();
    let original_link = std::fs::read_link(format!("/proc/self/fd/{fd}")).unwrap();
    let body =
        json!({"url":"http://11.253.250.2:18080/physical/frontend-fd-loss.json"}).to_string();
    let wire=format!("POST /api/plugins/repos/preview/ HTTP/1.1\r\nHost: owned\r\nx-api-key: {key}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
    tokio::io::AsyncWriteExt::write_all(&mut client, wire.as_bytes())
        .await
        .unwrap();
    cue(&out.join("origin-wire-held.json"), 15).await;
    assert_eq!(admitted(&observed), vec![true]);
    assert!(fate.lock().unwrap().response_statuses.is_empty());
    let shutdown_began = Utc::now();
    let actual_shutdown = tokio::io::AsyncWriteExt::shutdown(&mut client).await;
    let shutdown_error = actual_shutdown
        .as_ref()
        .err()
        .map(|e| format!("{:?}", e.kind()));
    drop(client);
    let after_link = std::fs::read_link(format!("/proc/self/fd/{fd}")).ok();
    assert_ne!(after_link.as_ref(), Some(&original_link));
    save(
        &out.join("actual-frontend-client-loss.json"),
        &json!({"fd":fd,"original_socket_link":original_link,"after_drop_link":after_link,"original_generation_absent":true,"PID":std::process::id(),"PID_namespace":std::fs::read_link("/proc/self/ns/pid").unwrap(),"actual_shutdown_result_ok":actual_shutdown.is_ok(),"actual_shutdown_error":shutdown_error,"began_utc":shutdown_began,"completed_utc":Utc::now(),"actual_request_wire_bytes":wire.len(),"actual_request_wire_sha256":hash(wire.as_bytes()),"FD_loss_implies_handler_abort":false}),
    );
    let samples = owner_samples(&auth, &observed).await;
    let held_fate = serde_json::to_value(&*fate.lock().unwrap()).unwrap();
    save(
        &out.join("frontend-held-observations.json"),
        &json!({"samples":samples,"actual_HTTP_IO_and_handler_fate_while_held":held_fate,"new_future_allowance":false}),
    );
    std::fs::write(
        out.join("origin-wire-release"),
        b"after actual clientFDdrop and two owner samples\n",
    )
    .unwrap();
    settle(&auth, &observed).await;
    tx.send(()).unwrap();
    let server_result = tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap();
    assert!(server_result.is_ok());
    let actual_fate = serde_json::to_value(&*fate.lock().unwrap()).unwrap();
    assert_eq!(actual_fate["accepted"], 1);
    assert_eq!(actual_fate["handler_started"], 1);
    assert_eq!(actual_fate["socket_dropped"], true);
    assert_eq!(actual_fate["capacity_exceeded"], false);
    let returned = actual_fate["response_statuses"].as_array().unwrap().len();
    let cancelled = actual_fate["handler_cancelled"].as_u64().unwrap();
    if returned == 1 {
        assert_eq!(
            actual_fate["response_statuses"],
            json!([200]),
            "ProducedResponseIsNotClientReceipt"
        );
    }
    assert_eq!(
        returned as u64 + cancelled,
        1,
        "ObserveExactlyOneActualHandlerFateNotInferFromFD"
    );
    assert_eq!(snapshot(observer).await, before);
    assert_eq!(credentials(observer).await, credentials_before);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    let report = json!({"capture_complete":false,"passed":false,"case":1,"actual_requests":calls.load(std::sync::atomic::Ordering::SeqCst),"Source_requests":0,"Main":false,"before":before,"credentials_before":credentials_before,"cache_before":before_cache,"cache_after":cache_all(cache_url).await,"actual_sustained_owner_samples":samples,"actual_HTTP_IO_handler_fate":actual_fate,"actual_server_exit0":true,"original_generation_scope":"one owned frontend clientTcpStream; no inferred handler cancellation/write error/received response","origin_contacts_expected":1,"root_input_role":root});
    finish_report(out, subject, observer, &auth, &observed, report).await;
    *HTTP_OBSERVER.lock().unwrap() = None;
}
async fn warm_fetch_statements(subject: &Store, observer: &Store, root: &FsPath) -> Value {
    let before = snapshot(observer).await;
    let mut c = subject.pool().acquire().await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *c)
        .await
        .unwrap();
    let measured:(String,i64)=sqlx::query_as("SELECT current_setting('track_activity_query_size'),pg_size_bytes(current_setting('track_activity_query_size'))").fetch_one(&mut *c).await.unwrap();
    assert!((100..=1048576).contains(&measured.1));
    let data: Value = serde_json::from_slice(
        &std::fs::read(root.join("inputs/refresh-origin-body.json")).unwrap(),
    )
    .unwrap();
    let t = chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let missing = 900000001i64;
    assert!(sqlx::query(TARGET_SQL)
        .bind(missing)
        .fetch_optional(&mut *c)
        .await
        .unwrap()
        .is_none());
    assert!(sqlx::query(SAVE)
        .bind(missing)
        .bind(text(inner(&data).get("registry_name")).unwrap())
        .bind(sqlx::types::Json(&data))
        .bind(t)
        .bind(t)
        .fetch_optional(&mut *c)
        .await
        .unwrap()
        .is_none());
    c.ping().await.unwrap();
    c.return_to_pool().await;
    let after = snapshot(observer).await;
    assert_eq!(before, after);
    json!({"actual_backend_pid":pid,"actual_setting":measured.0,"track_activity_query_size_bytes":measured.1,"observed_before_any_hold":true,"full_same_connection_warm_SQL":[TARGET_SQL,SAVE],"full_SQL_sha256":[hash(TARGET_SQL.as_bytes()),hash(SAVE.as_bytes())],"missing_pk":missing,"warm_before":before,"warm_after":after})
}
async fn actual_save_lock(observer: &Store, db: &str, warm: &Value) -> Value {
    let cap = warm["track_activity_query_size_bytes"].as_u64().unwrap() as usize;
    let limit = SAVE.len().min(cap - 1);
    let prefix = &SAVE[..limit];
    let pid = warm["actual_backend_pid"].as_i64().unwrap() as i32;
    let until = Instant::now() + Duration::from_secs(15);
    loop {
        let rows:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(t)),'[]'::jsonb) FROM(SELECT pid,datname,state,wait_event_type,wait_event,left(query,$3) AS observed_query_prefix FROM pg_stat_activity WHERE datname=$1 AND pid=$2 AND left(query,$3)=$4 AND wait_event_type='Lock')t").bind(db).bind(pid).bind(limit as i32).bind(prefix).fetch_one(observer.pool()).await.unwrap();
        if rows.as_array().unwrap().len() == 1 {
            return json!({"actual_rows":rows,"actual_track_activity_query_size_bytes":cap,"known_prefix":prefix,"known_prefix_bytes":limit,"full_SQL_sha256":hash(SAVE.as_bytes())});
        }
        assert!(Instant::now() < until, "ActualSameTargetSaveLock15");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
async fn sql_ready_case(
    case: u64,
    root: &FsPath,
    out: &FsPath,
    url: &str,
    observer: &Store,
    cache_url: &str,
    reference: &Value,
    db: &str,
) {
    assert!(matches!(case, 2 | 3));
    let stage = if case == 2 {
        "save_ready"
    } else {
        "save_57014_ready"
    };
    let (mut peer, port) = Peer::start(root, out, &json!([stage]));
    let mut proxy_url = reqwest::Url::parse(url).unwrap();
    proxy_url.set_port(Some(port)).unwrap();
    proxy_url
        .query_pairs_mut()
        .append_pair("application_name", "unsigned-remaining-subject");
    let subject = Store::connect(proxy_url.as_str(), 4).await.unwrap();
    let before = snapshot(observer).await;
    let credentials_before = credentials(observer).await;
    let before_cache = cache_all(cache_url).await;
    let warm = warm_fetch_statements(&subject, observer, root).await;
    let (auth, app, observed, calls) = adapter(&subject, cache_url, reference).await;
    observe_http(out, &observed, &auth);
    let key = source_key(reference);
    let mut row_lock = if case == 3 {
        let mut tx = observer.pool().begin().await.unwrap();
        let id: i64 =
            sqlx::query_scalar("SELECT id FROM plugins_pluginrepo WHERE id=801 FOR UPDATE")
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        assert_eq!(id, 801);
        Some(tx)
    } else {
        None
    };
    let began = Utc::now();
    let mut caller = Some(tokio::spawn(app.oneshot(request(
        "POST",
        "/api/plugins/repos/801/refresh/",
        Some(&key),
        String::new(),
    ))));
    let mut actual_lock = Value::Null;
    let mut actual_cancel = Value::Null;
    if case == 3 {
        actual_lock = actual_save_lock(observer, db, &warm).await;
        let pid = warm["actual_backend_pid"].as_i64().unwrap() as i32;
        let cancelled:bool=sqlx::query_scalar("SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid=$2 AND application_name='unsigned-remaining-subject'").bind(db).bind(pid).fetch_one(observer.pool()).await.unwrap();
        assert!(cancelled);
        actual_cancel = json!({"same_actual_backend_pid":pid,"pg_cancel_backend_result":cancelled,"Source_injected_SQL_error_HTTP_authority":false});
    }
    let first_held = held(&mut peer, stage).await;
    assert_eq!(
        first_held["held"]["backend_pid"],
        warm["actual_backend_pid"]
    );
    assert_eq!(first_held["held"]["sql"], SAVE);
    let held_sql = snapshot(observer).await;
    let held_credentials = credentials(observer).await;
    let report_path = out.join("sql-held-partial.json");
    save(
        &report_path,
        &json!({"capture_complete":false,"case":case,"before":before,"actual_warm":warm,"actual_target_lock":actual_lock,"actual_cancel":actual_cancel,"actual_held":first_held,"independent_SQL_while_Readyheld":held_sql}),
    );
    let disposition = Instant::now();
    let actual_fate = if case == 2 {
        let task = caller.take().unwrap();
        assert!(!task.is_finished());
        task.abort();
        let error = task.await.unwrap_err();
        assert!(error.is_cancelled());
        json!({"kind":"actual_observer_JoinHandle_abort","actual_join_cancelled":error.is_cancelled(),"join_error":error.to_string(),"elapsed_seconds":disposition.elapsed().as_secs_f64(),"HTTPFDloss_inferred":false})
    } else {
        assert!(!caller.as_ref().unwrap().is_finished());
        let timed =
            tokio::time::timeout(Duration::from_millis(100), caller.as_mut().unwrap()).await;
        assert!(timed.is_err());
        json!({"kind":"actual_borrowed100ms_observer_deadline","actual_timeout":true,"deadline_seconds":0.1,"elapsed_seconds":disposition.elapsed().as_secs_f64(),"actual_join_still_unfinished":!caller.as_ref().unwrap().is_finished(),"HTTPFDloss_inferred":false})
    };
    let samples = owner_samples(&auth, &observed).await;
    save(
        &out.join("SQL-sustained-owner-samples.json"),
        &json!({"case":case,"stage":stage,"actual_fate":actual_fate,"samples":samples,"same_gate_still_held":peer.stats()}),
    );
    peer.release(first_held["held"]["ordinal"].as_u64().unwrap());
    if let Some(tx) = row_lock.take() {
        tx.rollback().await.unwrap();
    }
    let mut actual_response = Value::Null;
    if let Some(task) = caller.take() {
        let response = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let status = response.status().as_u16();
        let headers: Vec<_> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_owned(), hex(v.as_bytes())))
            .collect();
        let body = to_bytes(response.into_body(), RESPONSE_LIMIT)
            .await
            .unwrap();
        assert_eq!(status, 503);
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"error":"Database unavailable"})
        );
        actual_response = json!({"status":status,"headers_value_hex":headers,"body_hex":hex(&body),"body_sha256":hash(&body),"body_bytes":body.len(),"Source_injected_SQL_error_HTTP_authority":false});
    }
    settle(&auth, &observed).await;
    let after = snapshot(observer).await;
    let after_credentials = credentials(observer).await;
    assert_eq!(after_credentials, credentials_before);
    if case == 3 {
        assert_eq!(before, held_sql);
        assert_eq!(before, after);
    } else {
        let before_repo = before["tables"]["plugins_pluginrepo"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["id"] == 801)
            .unwrap();
        let held_repo = held_sql["tables"]["plugins_pluginrepo"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["id"] == 801)
            .unwrap();
        assert_ne!(before_repo["cached_manifest"], held_repo["cached_manifest"]);
        assert_eq!(
            before["tables"]["plugins_pluginconfig"],
            held_sql["tables"]["plugins_pluginconfig"]
        );
        assert_ne!(
            before["tables"]["plugins_pluginconfig"],
            after["tables"]["plugins_pluginconfig"]
        );
        for table in TABLES {
            if !matches!(table, "plugins_pluginrepo" | "plugins_pluginconfig") {
                assert_eq!(before["tables"][table], after["tables"][table]);
            }
        }
        assert_eq!(before["sequences"], after["sequences"]);
    }
    let reused: i32 = sqlx::query_scalar("SELECT 424242::int")
        .fetch_one(subject.pool())
        .await
        .unwrap();
    assert_eq!(reused, 424242);
    let proxy_stats = peer.stats();
    assert_eq!(proxy_stats["errors"], json!([]));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    let report = json!({"capture_complete":false,"passed":false,"case":case,"actual_requests":calls.load(std::sync::atomic::Ordering::SeqCst),"Source_requests":0,"Main":false,"before":before,"credentials_before":credentials_before,"cache_before":before_cache,"cache_after":cache_all(cache_url).await,"request_began_utc":began,"request_completed_utc":Utc::now(),"actual_warm":warm,"actual_target_lock":actual_lock,"actual_cancel":actual_cancel,"actual_held":first_held,"independent_SQL_while_Readyheld":held_sql,"credentials_while_Readyheld":held_credentials,"actual_observer_fate":actual_fate,"actual_sustained_owner_samples":samples,"actual_response_after_Ready":actual_response,"actual_pool_reuse":reused,"actual_proxy_before_finish":proxy_stats,"origin_contacts_expected":1,"full_SQL_warm_equals_held_wire_required":true,"stat_activity_scope":"measured known prefix+exact backend; full SQL separately exact wire/warm"});
    // First close and independent while-alive backend observation precede relay stop.
    finish_report(out, &subject, observer, &auth, &observed, report).await;
    let stopped = peer.finish();
    save(&out.join("proxy-final-stats.json"), &stopped);
    let report_path = out.join("unsigned-remaining-physical-private.json");
    let mut final_report: Value =
        serde_json::from_slice(&std::fs::read(&report_path).unwrap()).unwrap();
    final_report["actual_proxy_final"] = stopped;
    final_report["proxy_finish_pending"] = json!(false);
    final_report["capture_complete"] = json!(true);
    final_report["passed"] = json!(true);
    save(&report_path, &final_report);
    *HTTP_OBSERVER.lock().unwrap() = None;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires exact reviewed unsigned remaining4 owned PG/Redis/HTTP context; never Main or Source"]
async fn actual_unsigned_remaining_ownership_peer() {
    assert_eq!(
        std::env::var("PLUGIN_UNSIGNED_REMAINING_AUTHORITY").as_deref(),
        Ok("owned-remaining-four-only")
    );
    let case: u64 = std::env::var("PLUGIN_UNSIGNED_REMAINING_CASE")
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=4).contains(&case));
    let root = PathBuf::from(std::env::var("PLUGIN_UNSIGNED_REMAINING_ROOT").unwrap());
    let out = PathBuf::from(std::env::var("PLUGIN_UNSIGNED_OUTPUT").unwrap());
    assert!(root.is_dir() && out.is_dir());
    let db = std::env::var("POSTGRES_DB").unwrap();
    owned(&db);
    let url = std::env::var("DATABASE_TEST_URL").unwrap();
    assert_eq!(reqwest::Url::parse(&url).unwrap().path(), format!("/{db}"));
    let cache_url = std::env::var("REDIS_TEST_URL").unwrap();
    assert!(cache_url.ends_with("/1"));
    let source = reference(&root);
    let mut direct = reqwest::Url::parse(&url).unwrap();
    direct
        .query_pairs_mut()
        .append_pair("application_name", "unsigned-remaining-observer");
    let observer = Store::connect(direct.as_str(), 4).await.unwrap();
    save(
        &out.join("remaining-peer-began.json"),
        &json!({"case":case,"source_requests":0,"Main":false,"Source_existing_state_only":true,"actual_began_utc":Utc::now(),"case_outer_seconds":90,"existing_original_request_writer_slots":4,"owned_database":db,"production_runtime_changed":false}),
    );
    let run = async {
        match case {
            1 | 4 => {
                let mut direct = reqwest::Url::parse(&url).unwrap();
                direct
                    .query_pairs_mut()
                    .append_pair("application_name", "unsigned-remaining-subject");
                let subject = Store::connect(direct.as_str(), 4).await.unwrap();
                if case == 1 {
                    frontend_loss_case(&root, &out, &subject, &observer, &cache_url, &source).await;
                } else {
                    frame_capacity_case(&root, &out, &subject, &observer, &cache_url, &source)
                        .await;
                }
            }
            2 | 3 => {
                sql_ready_case(case, &root, &out, &url, &observer, &cache_url, &source, &db).await
            }
            _ => unreachable!(),
        }
    };
    match tokio::time::timeout(
        Duration::from_secs(90),
        std::panic::AssertUnwindSafe(run).catch_unwind(),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(payload)) => {
            save(
                &out.join("remaining-peer-failure.json"),
                &json!({"case":case,"failure":"actual_unwind","passed":false,"capture_complete":false,"selected_later_cases":"no automatic replay"}),
            );
            std::panic::resume_unwind(payload);
        }
        Err(error) => {
            save(
                &out.join("remaining-peer-failure.json"),
                &json!({"case":case,"failure":"actual_outer90_timeout","detail":error.to_string(),"passed":false,"capture_complete":false}),
            );
            panic!("ActualRemainingCase90:{case}");
        }
    }
}
