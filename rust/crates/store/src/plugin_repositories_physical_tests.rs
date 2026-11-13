//! Test-only actual metadata SQL/response-frame ownership probes; no Main/Source.
//! Original Dispatcharr/SQLx authors retain source and algorithm credit.
#![allow(clippy::too_many_arguments)]
use super::*;
use crate::{AuthConfig, NotificationCache, Store};
use futures_util::StreamExt;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Connection;
use std::future::IntoFuture;
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Read, Write},
    os::unix::process::ExitStatusExt,
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
fn request(method: &str, path: &str, key: Option<&str>, body: &'static str) -> Request {
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
fn reference(root: &FsPath) -> Value {
    let b = std::fs::read(root.join("inputs/Source24-private.json")).unwrap();
    assert!(b.len() <= 67108864);
    assert_eq!(
        hash(&b),
        std::env::var("PLUGIN_REPO_SOURCE_SHA256").unwrap()
    );
    let v: Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["actual_request_count"], 24);
    assert_eq!(v["force_authenticate_used"], false);
    v
}
fn source_key(reference: &Value, level: i64) -> String {
    let users = reference["seed"]["users"].as_array().unwrap();
    let found: Vec<_> = users
        .iter()
        .filter(|u| u["user_level"] == level && u["id"].as_i64() != Some(104))
        .collect();
    assert_eq!(found.len(), 1);
    found[0]["api_key"].as_str().unwrap().to_owned()
}
fn adapter(
    subject: &Store,
    cache_url: &str,
    reference: &Value,
) -> impl std::future::Future<Output = (AuthService, Router, Admissions)> + Send {
    let subject = subject.clone();
    let cache_url = cache_url.to_owned();
    let reference = reference.clone();
    async move {
        let auth = AuthService::new(
            subject,
            AuthConfig::new("owned-metadata-physical-signing-only".into()).unwrap(),
        )
        .with_notification_cache(NotificationCache::connect(&cache_url).await.unwrap());
        let settings = reference["seed"]["settings"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["key"] == "system_settings")
            .unwrap()["value"]
            .clone();
        auth.set_display_timezone_on_settings_save("system_settings", &settings);
        let observed: Admissions = Arc::new(Mutex::new(Vec::new()));
        let record = observed.clone();
        let app = PluginRepositories::new(auth.clone())
            .observe_original_writer(Arc::new(move |w| {
                let mut rows = record.lock().unwrap();
                assert!(rows.len() < 8, "AtMostEightActualAdmissions");
                rows.push(w);
            }))
            .router();
        (auth, app, observed)
    }
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
async fn held_frames_case(
    root: &FsPath,
    out: &FsPath,
    subject: &Store,
    observer: &Store,
    cache_url: &str,
    reference: &Value,
) {
    let (auth, app, observed) = adapter(subject, cache_url, reference).await;
    let admin = source_key(reference, 10);
    let standard = source_key(reference, 1);
    let before = snapshot(observer).await;
    let auth_before = credentials(observer).await;
    let mut frames = Vec::new();
    let mut events = Vec::new();
    let progress = out.join("case4-partial-progress.json");
    save(
        &progress,
        &json!({"capture_complete":false,"before":before,"credentials_before":auth_before,"actual_frames":events}),
    );
    for (index, status) in [200, 200, 401, 401].into_iter().enumerate() {
        let r = match index {
            0 => request("GET", "/api/plugins/repos/", Some(&admin), ""),
            1 => request("GET", "/api/plugins/repos/", Some(&standard), ""),
            2 => request("GET", "/api/plugins/repos/", None, ""),
            _ => {
                let mut r = request("GET", "/api/plugins/repos/", None, "");
                r.headers_mut()
                    .insert("authorization", "Bearer too many pieces".parse().unwrap());
                r
            }
        };
        let frame = actual_frame(app.clone(), r, status, out, index + 1).await;
        events.push(json!({"request":index+1,"status":status,"actual_frame_bytes":frame.len(),"actual_frame_sha256":hash(&frame),"actual_frame_hex":hex(&frame),"original_weak":admitted(&observed),"available_slots":auth.user_agent_workers.available_permits()}));
        frames.push(frame);
        save(
            &progress,
            &json!({"capture_complete":false,"before":before,"credentials_before":auth_before,"actual_frames":events}),
        );
        assert_eq!(admitted(&observed), vec![true; index + 1]);
        assert_eq!(auth.user_agent_workers.available_permits(), 3 - index);
    }
    let fifth_sql_before = snapshot(observer).await;
    let fifth_auth_before = credentials(observer).await;
    let fifth_cache_before = cache(cache_url).await;
    // This would produce real failed-key history if auth ran. Capacity MUST win.
    let fifth = app
        .clone()
        .oneshot(request(
            "GET",
            "/api/plugins/repos/",
            Some("owned-invalid-fifth-preauth-no-history"),
            "",
        ))
        .await
        .unwrap();
    let fifth_status = fifth.status().as_u16();
    let fifth_headers: Vec<_> = fifth
        .headers()
        .iter()
        .map(|(k, v)| (k.as_str().to_owned(), hex(v.as_bytes())))
        .collect();
    let fifth_body = to_bytes(fifth.into_body(), RESPONSE_LIMIT).await.unwrap();
    save(
        &out.join("actual-response-fifth-refusal.json"),
        &json!({"status":fifth_status,"headers_value_hex":fifth_headers,"body_hex":hex(&fifth_body),"body_bytes":fifth_body.len(),"body_sha256":hash(&fifth_body)}),
    );
    assert_eq!(fifth_status, 503);
    let fifth_cache_after = cache(cache_url).await;
    let fifth_sql_after = snapshot(observer).await;
    let fifth_auth_after = credentials(observer).await;
    assert_eq!(fifth_sql_after, fifth_sql_before);
    assert_eq!(fifth_auth_after, fifth_auth_before);
    assert_eq!(admitted(&observed), vec![true; 4]);
    assert_eq!(auth.user_agent_workers.available_permits(), 0);
    let fifth_original_weak = admitted(&observed);
    let fifth_slots = auth.user_agent_workers.available_permits();
    // Compare finite TTL monotonicity outside Rust; retain literal before/after DUMPs.
    drop(frames.remove(0));
    assert_eq!(admitted(&observed), vec![false, true, true, true]);
    assert_eq!(auth.user_agent_workers.available_permits(), 1);
    let sixth = actual_frame(
        app.clone(),
        request("GET", "/api/plugins/repos/", Some(&admin), ""),
        200,
        out,
        6,
    )
    .await;
    assert_eq!(admitted(&observed), vec![false, true, true, true, true]);
    assert_eq!(auth.user_agent_workers.available_permits(), 0);
    let sixth_sha = hash(&sixth);
    let sixth_hex = hex(&sixth);
    let sixth_original_weak = admitted(&observed);
    let sixth_slots = auth.user_agent_workers.available_permits();
    drop(sixth);
    drop(frames);
    settle(&auth, &observed).await;
    assert_eq!(snapshot(observer).await, before);
    assert_eq!(credentials(observer).await, auth_before);
    let actual_subject_backends:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(t)),'[]'::jsonb) FROM(SELECT pid,state FROM pg_stat_activity WHERE datname=current_database() AND application_name='pluginrepo-physical-subject')t").fetch_one(observer.pool()).await.unwrap();
    let actual_pool = pool_finish(subject).await;
    let closed_subject_backends:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(t)),'[]'::jsonb) FROM(SELECT pid,state FROM pg_stat_activity WHERE datname=current_database() AND application_name='pluginrepo-physical-subject')t").fetch_one(observer.pool()).await.unwrap();
    assert_eq!(closed_subject_backends, json!([]));
    let report = json!({"capture_complete":true,"passed":true,"case":4,"actual_statuses":[200,200,401,401,503,200],"actual_first_four":events,"fifth_actual_body_hex":hex(&fifth_body),"fifth_SQL_before":fifth_sql_before,"fifth_SQL_after":fifth_sql_after,"fifth_credentials_before":fifth_auth_before,"fifth_credentials_after":fifth_auth_after,"fifth_original_weak":fifth_original_weak,"fifth_slots":fifth_slots,"fifth_raw_cache_before":fifth_cache_before,"fifth_raw_cache_after":fifth_cache_after,"sixth_actual_frame_sha256":sixth_sha,"sixth_actual_frame_hex":sixth_hex,"sixth_original_weak":sixth_original_weak,"sixth_slots":sixth_slots,"original_weak_after_settlement":admitted(&observed),"writer_slots_after_settlement":auth.user_agent_workers.available_permits(),"before":before,"after":snapshot(observer).await,"credentials_before":auth_before,"credentials_after":credentials(observer).await,"pool":actual_pool,"actual_subject_backends_before_close":actual_subject_backends,"while_ELF_alive_subject_backends":closed_subject_backends,"Main":false,"Source_requests":0,"root_input_path":root});
    save(&out.join("metadata-physical-private.json"), &report);
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
const CHILD_SQL: &str =
    "UPDATE plugins_pluginconfig SET source_repo_id=NULL WHERE source_repo_id=$1";
const PARENT_SQL: &str = "DELETE FROM plugins_pluginrepo WHERE id=$1";
async fn warm_statements(subject: &Store, observer: &Store) -> Value {
    let before = snapshot(observer).await;
    let mut c = subject.pool().acquire().await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *c)
        .await
        .unwrap();
    let measured:(String,i64)=sqlx::query_as("SELECT current_setting('track_activity_query_size'),pg_size_bytes(current_setting('track_activity_query_size'))")
        .fetch_one(&mut *c).await.unwrap();
    assert!((100..=1048576).contains(&measured.1));
    let missing = 900000001i64;
    let t = DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    assert!(sqlx::query(TARGET_SQL)
        .bind(missing)
        .fetch_optional(&mut *c)
        .await
        .unwrap()
        .is_none());
    assert!(sqlx::query(UPDATE_SQL)
        .bind(missing)
        .bind(Some("warm-only-not-a-real-row"))
        .bind(None::<bool>)
        .bind(t)
        .fetch_optional(&mut *c)
        .await
        .unwrap()
        .is_none());
    let mut tx = c.begin().await.unwrap();
    assert_eq!(
        sqlx::query(CHILD_SQL)
            .bind(missing)
            .execute(&mut *tx)
            .await
            .unwrap()
            .rows_affected(),
        0
    );
    assert_eq!(
        sqlx::query(PARENT_SQL)
            .bind(missing)
            .execute(&mut *tx)
            .await
            .unwrap()
            .rows_affected(),
        0
    );
    tx.commit().await.unwrap();
    c.begin().await.unwrap().rollback().await.unwrap();
    c.ping().await.unwrap();
    c.return_to_pool().await;
    assert_eq!(snapshot(observer).await, before);
    json!({"actual_backend_pid":pid,"track_activity_query_size_setting":measured.0,"track_activity_query_size_bytes":measured.1,"observed_before_any_hold":true,"same_connection_exact_warm_SQL":[TARGET_SQL,UPDATE_SQL,CHILD_SQL,PARENT_SQL,"COMMIT","ROLLBACK"],"full_warm_SQL_sha256":[hash(TARGET_SQL.as_bytes()),hash(UPDATE_SQL.as_bytes()),hash(CHILD_SQL.as_bytes()),hash(PARENT_SQL.as_bytes()),hash(b"COMMIT"),hash(b"ROLLBACK")],"warm_missing_pk":missing,"warm_before":before,"warm_after":snapshot(observer).await})
}
async fn lock_target(observer: &Store, db: &str, sql: &str, warm: &Value) -> Value {
    let cap = warm["track_activity_query_size_bytes"].as_u64().unwrap() as usize;
    assert!(sql.is_ascii());
    let prefix = &sql[..sql.len().min(cap - 1)];
    tokio::time::timeout(Duration::from_secs(15),async{
        loop {
            let rows:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(t)),'[]'::jsonb) FROM (SELECT pid,state,wait_event_type,wait_event,query FROM pg_stat_activity WHERE datname=$1 AND pid=$2 AND left(query,$3)=$4 AND state='active' AND wait_event_type='Lock')t")
                .bind(db).bind(warm["actual_backend_pid"].as_i64().unwrap()as i32).bind(prefix.len()as i32).bind(prefix).fetch_one(observer.pool()).await.unwrap();
            if rows.as_array().unwrap().len()==1{return json!({"rows":rows,"known_SQL_prefix":prefix,"known_prefix_bytes":prefix.len(),"full_SQL_sha256":hash(sql.as_bytes()),"actual_setting":warm["track_activity_query_size_setting"]});}
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("SamePreparedBackendActualLock15")
}
async fn samples(peer: &mut Peer, stage: &str, auth: &AuthService, observed: &Admissions) -> Value {
    let start = Instant::now();
    let mut rows = Vec::new();
    for n in 0..2 {
        if n == 1 {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let stats = peer.stats();
        assert_eq!(stats["held"]["stage"], stage);
        let weak = admitted(observed);
        assert_eq!(weak, vec![true]);
        assert_eq!(auth.user_agent_workers.available_permits(), 3);
        rows.push(json!({"elapsed_monotonic_seconds":start.elapsed().as_secs_f64(),"original_weak_alive":weak,"writer_slots":auth.user_agent_workers.available_permits(),"actual_held":stats["held"]}));
    }
    json!(rows)
}
fn same_except_repo_delta(before: &Value, after: &Value, case: u64) {
    assert_eq!(before["sequences"], after["sequences"]);
    for table in TABLES {
        if table != "plugins_pluginrepo" && table != "plugins_pluginconfig" {
            assert_eq!(
                before["tables"][table], after["tables"][table],
                "ForeignTable:{table}"
            );
        }
    }
    let mut expected = before.clone();
    if case == 1 {
        let target = expected["tables"]["plugins_pluginrepo"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|r| r["id"] == 801)
            .unwrap();
        target["public_key"] = json!("owned-physical-update");
        let saved = after["tables"]["plugins_pluginrepo"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["id"] == 801)
            .unwrap();
        assert_ne!(target["updated_at"], saved["updated_at"]);
        target["updated_at"] = saved["updated_at"].clone();
    } else if case == 2 {
        expected["tables"]["plugins_pluginrepo"]
            .as_array_mut()
            .unwrap()
            .retain(|r| r["id"] != 801);
        for r in expected["tables"]["plugins_pluginconfig"]
            .as_array_mut()
            .unwrap()
        {
            if r["source_repo_id"] == 801 {
                r["source_repo_id"] = Value::Null;
            }
        }
    }
    assert_eq!(&expected, after, "ExactOnlyMeasuredMetadataDelta");
}
async fn sql_case(
    case: u64,
    root: &FsPath,
    out: &FsPath,
    url: &str,
    observer: &Store,
    cache_url: &str,
    reference: &Value,
    db: &str,
) {
    let stages = match case {
        1 => json!(["update_ready"]),
        2 => json!(["child_ready", "commit_ready"]),
        3 => json!(["child_ready", "parent_error_ready", "rollback_ready"]),
        _ => unreachable!(),
    };
    let (mut peer, port) = Peer::start(root, out, &stages);
    let mut proxy_url = url::Url::parse(url).unwrap();
    proxy_url.set_port(Some(port)).unwrap();
    proxy_url
        .query_pairs_mut()
        .append_pair("application_name", "pluginrepo-physical-subject");
    let subject = Store::connect(proxy_url.as_str(), 4).await.unwrap();
    let before = snapshot(observer).await;
    let credentials_before = credentials(observer).await;
    let cache_before = cache(cache_url).await;
    let warm = warm_statements(&subject, observer).await;
    let (auth, app, observed) = adapter(&subject, cache_url, reference).await;
    let admin = source_key(reference, 10);
    let mut parent_lock = if case == 3 {
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
    let path = out.join("metadata-physical-private.json");
    let mut report = json!({"capture_complete":false,"passed":false,"case":case,"Main":false,"Source_requests":0,"before":before,"credentials_before":credentials_before,"cache_before":cache_before,"actual_warm_and_pg_setting":warm,"physical_stat_activity_scope":"known actual setting prefix AND same prepared backend; fullSQL equality separately relay-verified"});
    save(&path, &report);
    let request_began = Utc::now();
    report["request_began_utc"] = json!(request_began);
    let fate: SharedFate = Arc::new(Mutex::new(HttpFate::default()));
    let mut server = None;
    let mut stop = None;
    let mut http = None;
    let mut caller = if case == 2 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let f = fate.clone();
        let router = app
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
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        stop = Some(tx);
        server = Some(tokio::spawn(
            axum::serve(
                FateListener {
                    listener,
                    fate: fate.clone(),
                },
                router,
            )
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .into_future(),
        ));
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut client,format!("DELETE /api/plugins/repos/801/ HTTP/1.1\r\nHost: owned\r\nx-api-key: {admin}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
        http = Some(client);
        None
    } else {
        let r = if case == 1 {
            request(
                "PUT",
                "/api/plugins/repos/801/",
                Some(&admin),
                "{\"public_key\":\"owned-physical-update\"}",
            )
        } else {
            request("DELETE", "/api/plugins/repos/801/", Some(&admin), "")
        };
        Some(tokio::spawn(async move {
            let response = app.oneshot(r).await.unwrap();
            let status = response.status().as_u16();
            let body = to_bytes(response.into_body(), RESPONSE_LIMIT)
                .await
                .unwrap();
            (status, body)
        }))
    };
    let first = if case == 1 {
        "update_ready"
    } else {
        "child_ready"
    };
    let reached = held(&mut peer, first).await;
    report["first_held"] = reached;
    assert_eq!(
        report["first_held"]["held"]["backend_pid"],
        warm["actual_backend_pid"]
    );
    if case == 1 {
        let task = caller.take().unwrap();
        let returned_before_ready = task.is_finished();
        assert!(!returned_before_ready);
        report["observer_returned_before_update_ready_release"] = json!(returned_before_ready);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        report["actual_direct_observer_abort"] = json!(true);
        let visible = snapshot(observer).await;
        same_except_repo_delta(&before, &visible, 1);
        report["while_update_ready_held_committed_snapshot"] = visible;
        report["post_observer_loss_samples"] = samples(&mut peer, first, &auth, &observed).await;
        peer.release(0);
    } else if case == 2 {
        let child_visible = snapshot(observer).await;
        assert_eq!(child_visible, before, "ChildSetNullUncommittedAndInvisible");
        report["while_child_ready_held_uncommitted_snapshot"] = child_visible;
        report["while_child_ready_held_HTTP_fate"] =
            serde_json::to_value(&*fate.lock().unwrap()).unwrap();
        assert!(fate.lock().unwrap().response_statuses.is_empty());
        let mut client = http.take().unwrap();
        tokio::io::AsyncWriteExt::shutdown(&mut client)
            .await
            .unwrap();
        drop(client);
        report["actual_client_fd_shutdown_drop"] = json!(true);
        report["client_loss_implies_handler_abort"] = json!(false);
        report["post_socket_loss_samples"] = samples(&mut peer, first, &auth, &observed).await;
        peer.release(0);
        let commit = held(&mut peer, "commit_ready").await;
        report["commit_held"] = commit;
        report["while_commit_ready_held_HTTP_fate"] =
            serde_json::to_value(&*fate.lock().unwrap()).unwrap();
        assert!(fate.lock().unwrap().response_statuses.is_empty());
        let visible = snapshot(observer).await;
        same_except_repo_delta(&before, &visible, 2);
        report["while_commit_ready_held_committed_snapshot"] = visible;
        report["commit_held_samples"] = samples(&mut peer, "commit_ready", &auth, &observed).await;
        peer.release(1);
    } else {
        assert_eq!(snapshot(observer).await, before);
        peer.release(0);
        let locked = lock_target(observer, db, PARENT_SQL, &warm).await;
        report["actual_parent_lock_before_cancel"] = locked;
        let pid = warm["actual_backend_pid"].as_i64().unwrap() as i32;
        assert!(
            sqlx::query_scalar::<_, bool>("SELECT pg_cancel_backend($1)")
                .bind(pid)
                .fetch_one(observer.pool())
                .await
                .unwrap()
        );
        report["parent_57014_held"] = held(&mut peer, "parent_error_ready").await;
        assert!(!caller.as_ref().unwrap().is_finished());
        report["error_ready_samples"] =
            samples(&mut peer, "parent_error_ready", &auth, &observed).await;
        peer.release(1);
        report["rollback_held"] = held(&mut peer, "rollback_ready").await;
        let returned_before_ready = caller.as_ref().unwrap().is_finished();
        assert!(!returned_before_ready);
        report["observer_returned_before_rollback_ready_release"] = json!(returned_before_ready);
        let rollback_visible = snapshot(observer).await;
        assert_eq!(rollback_visible, before);
        report["while_rollback_ready_held_snapshot"] = rollback_visible;
        report["rollback_ready_samples"] =
            samples(&mut peer, "rollback_ready", &auth, &observed).await;
        peer.release(2);
        parent_lock.take().unwrap().rollback().await.unwrap();
        let (status, body) = tokio::time::timeout(Duration::from_secs(10), caller.take().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status, 503);
        report["actual_error_response"] = json!({"status":status,"body_hex":hex(&body),"sha256":hash(&body),"Source_injected_error_HTTP_authority":false});
    }
    settle(&auth, &observed).await;
    if let Some(tx) = stop {
        tx.send(()).unwrap();
    }
    if let Some(job) = server {
        tokio::time::timeout(Duration::from_secs(10), job)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    report["http_fate"] = serde_json::to_value(&*fate.lock().unwrap()).unwrap();
    assert!(!fate.lock().unwrap().capacity_exceeded);
    let after = snapshot(observer).await;
    same_except_repo_delta(&before, &after, case);
    assert_eq!(credentials(observer).await, credentials_before);
    report["after"] = after;
    report["credentials_after"] = credentials(observer).await;
    report["cache_after"] = cache(cache_url).await;
    report["original_weak_after_settlement"] = json!(admitted(&observed));
    report["writer_slots_after_settlement"] = json!(auth.user_agent_workers.available_permits());
    report["pool"] = pool_finish(&subject).await;
    drop(auth);
    let drained = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let stats = peer.stats();
            if stats["connections"]
                .as_array()
                .unwrap()
                .iter()
                .all(|c| c["closed"] == true && c["up_joined"] == true)
            {
                break stats;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("NaturalProxyDrainBeforeStop10");
    let pids: Vec<i32> = drained["connections"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|c| c["backend_pid"].as_i64().map(|v| v as i32))
        .collect();
    assert!(!pids.is_empty());
    let backends:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(t)),'[]'::jsonb) FROM(SELECT pid,state,query FROM pg_stat_activity WHERE datname=$1 AND pid=ANY($2))t").bind(db).bind(pids).fetch_one(observer.pool()).await.unwrap();
    assert_eq!(backends, json!([]));
    report["while_ELF_alive_subject_backends"] = backends;
    report["relay_naturally_drained_before_stop"] = drained;
    let stats = peer.finish();
    report["actual_relay_terminal"] = stats;
    report["request_completed_utc"] = json!(Utc::now());
    report["capture_complete"] = json!(true);
    report["passed"] = json!(true);
    save(&path, &report);
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires reviewed owned metadata4 controller, no default app"]
async fn metadata_physical_exact_one_case() {
    assert_eq!(
        std::env::var("PLUGIN_REPO_PHYSICAL").as_deref(),
        Ok("owned-metadata-four-only")
    );
    let case: u64 = std::env::var("PLUGIN_REPO_PHYSICAL_CASE")
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=4).contains(&case));
    let db = std::env::var("POSTGRES_DB").unwrap();
    owned(&db);
    let root = PathBuf::from(std::env::var("PLUGIN_REPO_PHYSICAL_ROOT").unwrap());
    let out = PathBuf::from(std::env::var("PLUGIN_REPO_PHYSICAL_OUTPUT").unwrap());
    assert!(out.is_dir());
    assert!(!out.join("metadata-physical-private.json").exists());
    let reference = reference(&root);
    let url = std::env::var("DATABASE_TEST_URL").unwrap();
    assert_eq!(url::Url::parse(&url).unwrap().path(), format!("/{db}"));
    let cache_url = std::env::var("REDIS_TEST_URL").unwrap();
    assert!(cache_url.ends_with("/1"));
    let observer = Store::connect(&url, 2).await.unwrap();
    tokio::time::timeout(Duration::from_secs(90), async {
        if case == 4 {
            let mut subject_url = url::Url::parse(&url).unwrap();
            subject_url
                .query_pairs_mut()
                .append_pair("application_name", "pluginrepo-physical-subject");
            let subject = Store::connect(subject_url.as_str(), 4).await.unwrap();
            held_frames_case(&root, &out, &subject, &observer, &cache_url, &reference).await;
        } else {
            sql_case(
                case, &root, &out, &url, &observer, &cache_url, &reference, &db,
            )
            .await;
        }
    })
    .await
    .expect("FixedCase90");
    observer.pool().close().await;
}
