//! Actual SQL/Redis/Channels physical ownership; copied fixture only.
//! Original Dispatcharr/framework authors retain code/algorithm credit.
use crate::{
    logo_scanner::Roots,
    logo_task::Clock,
    scanner_files::Service,
    scanner_files_worker::{Run, Worker},
    transport::{Config, ScannerFilesPorts, ScannerRoute},
};
use sqlx::Connection;
use std::{
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    task::{JoinHandle, JoinSet},
};
fn reply_frame<'a, R: tokio::io::AsyncRead + Unpin + Send + 'a>(
    reader: &'a mut R,
    budget: &'a mut usize,
    depth: usize,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<Vec<u8>>> + Send + 'a>> {
    Box::pin(async move {
        let invalid =
            || std::io::Error::new(std::io::ErrorKind::InvalidData, "Owned RESP2 reply bound");
        if depth > 8 {
            return Err(invalid());
        }
        let mut line = Vec::new();
        loop {
            if *budget == 0 || line.len() >= 1024 {
                return Err(invalid());
            }
            let byte = reader.read_u8().await?;
            *budget -= 1;
            line.push(byte);
            if byte == b'\n' {
                break;
            }
        }
        if !line.ends_with(b"\r\n") || line.len() < 3 {
            return Err(invalid());
        }
        match line[0] {
            b'+' | b'-' | b':' => Ok(line),
            b'$' => {
                let size = std::str::from_utf8(&line[1..line.len() - 2])
                    .map_err(|_| invalid())?
                    .parse::<i64>()
                    .map_err(|_| invalid())?;
                if size == -1 {
                    return Ok(line);
                }
                if size < 0 || size as usize > budget.saturating_sub(2) {
                    return Err(invalid());
                }
                let mut bytes = vec![0; size as usize + 2];
                reader.read_exact(&mut bytes).await?;
                *budget -= bytes.len();
                if !bytes.ends_with(b"\r\n") {
                    return Err(invalid());
                }
                line.extend(bytes);
                Ok(line)
            }
            b'*' => {
                let count = std::str::from_utf8(&line[1..line.len() - 2])
                    .map_err(|_| invalid())?
                    .parse::<i64>()
                    .map_err(|_| invalid())?;
                if count == -1 {
                    return Ok(line);
                }
                if !(0..=128).contains(&count) {
                    return Err(invalid());
                }
                for _ in 0..count {
                    line.extend(reply_frame(reader, budget, depth + 1).await?);
                }
                Ok(line)
            }
            _ => Err(invalid()),
        }
    })
}
#[derive(Clone, Copy)]
enum Target {
    Child,
    Marker,
    Script,
}
struct Proxy {
    base: String,
    hold: watch::Sender<bool>,
    discard: watch::Sender<bool>,
    blocked: watch::Receiver<bool>,
    task: JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort()
    }
}
async fn proxy(target: Target) -> Proxy {
    let mut listener = None;
    for port in 61000..61128 {
        if let Ok(value) = TcpListener::bind(("127.0.0.1", port)).await {
            listener = Some(value);
            break;
        }
    }
    let listener = listener.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (hold, held) = watch::channel(false);
    let (discard, lost) = watch::channel(false);
    let (blocked, observed) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                      incoming=listener.accept()=>{let(client,_)=incoming.unwrap();let mut held=held.clone();let lost=lost.clone();let blocked=blocked.clone();tasks.spawn(async move{
                       let server=TcpStream::connect(("127.0.0.1",6379)).await.unwrap();let(mut cr,mut cw)=client.into_split();let(sr,mut sw)=server.into_split();let(tx,mut rx)=mpsc::channel(32);
                       let outgoing:std::pin::Pin<Box<dyn std::future::Future<Output=std::io::Result<()>>+Send>>=Box::pin(async move{loop{let mut budget=524288;let frame=reply_frame(&mut cr,&mut budget,0).await?;let value=redis::parse_redis_value(&frame).map_err(|_|std::io::Error::from(std::io::ErrorKind::InvalidData))?;let args:Vec<Vec<u8>>=redis::from_redis_value(&value).map_err(|_|std::io::Error::from(std::io::ErrorKind::InvalidData))?;let matched=match target{Target::Child=>args.first().is_some_and(|v|v.eq_ignore_ascii_case(b"LPUSH"))&&args.get(1).is_some_and(|v|v==b"celery"),Target::Marker=>args.first().is_some_and(|v|v.eq_ignore_ascii_case(b"SET"))&&args.get(1).is_some_and(|v|v.starts_with(b"processed_file:")),Target::Script=>args.first().is_some_and(|v|v.eq_ignore_ascii_case(b"EVALSHA"))&&args.get(3).is_some_and(|v|v.starts_with(b"celery-tasks:celery-task-meta-")||v.starts_with(b"asgispecific."))};tx.send(matched).await.map_err(|_|std::io::Error::from(std::io::ErrorKind::BrokenPipe))?;sw.write_all(&frame).await?;}});
                       let incoming=async move{let mut sr=tokio::io::BufReader::new(sr);loop{let Some(matched)=rx.recv().await else{return Ok::<(),std::io::Error>(())};let mut budget=524288;let frame=reply_frame(&mut sr,&mut budget,0).await?;let actual=matched&&!frame.starts_with(b"-NOSCRIPT");if actual&&*lost.borrow(){blocked.send_replace(true);return Err(std::io::Error::from(std::io::ErrorKind::ConnectionReset))}
            if actual&&*held.borrow(){blocked.send_replace(true);held.wait_for(|v|!*v).await.map_err(|_|std::io::Error::from(std::io::ErrorKind::BrokenPipe))?;}cw.write_all(&frame).await?;}};
                       let _:Result<((),()),std::io::Error>=tokio::try_join!(outgoing,incoming);
                      });},_=tasks.join_next(),if !tasks.is_empty()=>{}
                     }
        }
    });
    Proxy {
        base: format!("redis://:playback-fixture-only@127.0.0.1:{port}"),
        hold,
        discard,
        blocked: observed,
        task,
    }
}
const CACHE: &str = "redis://:playback-fixture-only@localhost:6379/0";
const BROKER: &str = "redis://:playback-fixture-only@localhost:6379/8";
const RESULT: &str = "redis://:playback-fixture-only@localhost:6379/9";
struct Fixed;
impl Clock for Fixed {
    fn now(&self) -> f64 {
        1700000000.125
    }
}
struct Caller;
struct Fixture {
    worker: Worker,
    pool: sqlx::PgPool,
    file: String,
    id: uuid::Uuid,
}
impl Fixture {
    async fn new(broker: &str, result: &str, cache: &str, sink: &str) -> Self {
        assert_eq!(
            std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
            Ok("playback-fixture-only")
        );
        assert!(std::env::var("POSTGRES_DB")
            .unwrap()
            .starts_with("rust_probe_backup_jobs_"));
        crate::default_peer::validate_fixture_redis();
        let id =
            uuid::Uuid::parse_str(&std::env::var("SCANNER_RESOURCE_TASK_ID").unwrap()).unwrap();
        let file = "/data/m3us/rust-owned-branch-resource.m3u".to_string();
        let f = std::fs::File::create(&file).unwrap();
        f.set_times(std::fs::FileTimes::new().set_modified(
            std::time::UNIX_EPOCH + Duration::from_secs(1699999993) + Duration::from_millis(0),
        ))
        .unwrap();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&std::env::var("DATABASE_URL").unwrap())
            .await
            .unwrap();
        let ports = ScannerFilesPorts::connect(
            broker,
            result,
            cache,
            ScannerRoute::OwnedFixtureParent,
            Config::default(),
        )
        .await
        .unwrap();
        let sink = Arc::new(
            dispatcharr_realtime::RealtimeBus::connect(sink, Default::default())
                .await
                .unwrap(),
        );
        let service = Service::new(
            pool.clone(),
            ports,
            sink,
            Roots {
                m3u: "/data/m3us".into(),
                epg: "/data/epgs".into(),
                logos: "/data/logos".into(),
            },
        )
        .unwrap();
        // Worker and domain use clones of the SAME concrete ports capability.
        // The port clone is acquired through the prepared service's private accessor.
        let ports = service.resource_ports();
        let worker = Worker::new(service, ports, Arc::new(Fixed));
        Self {
            worker,
            pool,
            file,
            id,
        }
    }
    async fn rows(&self) -> i64 {
        sqlx::query_scalar("SELECT count(*)::bigint FROM m3u_m3uaccount WHERE file_path=$1")
            .bind(&self.file)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }
    async fn cleanup(self) {
        assert!(self.worker.shutdown(Duration::from_secs(5)).await);
        self.pool.close().await;
    }
}
async fn redis(db: i32) -> redis::aio::MultiplexedConnection {
    redis::Client::open(format!(
        "redis://:playback-fixture-only@localhost:6379/{db}"
    ))
    .unwrap()
    .get_multiplexed_async_connection()
    .await
    .unwrap()
}
async fn metadata(id: uuid::Uuid) -> Option<Vec<u8>> {
    redis::cmd("GET")
        .arg(format!("celery-tasks:celery-task-meta-{id}"))
        .query_async(&mut redis(9).await)
        .await
        .unwrap()
}
async fn own_children(id: uuid::Uuid) -> Vec<serde_json::Value> {
    let raw: Vec<Vec<u8>> = redis::cmd("LRANGE")
        .arg("celery")
        .arg(0)
        .arg(127)
        .query_async(&mut redis(8).await)
        .await
        .unwrap();
    raw.into_iter()
        .map(|v| serde_json::from_slice::<serde_json::Value>(&v).unwrap())
        .filter(|v| v["headers"]["parent_id"] == id.to_string())
        .collect()
}
async fn wait_blocked(proxy: &Proxy) {
    let mut rx = proxy.blocked.clone();
    tokio::time::timeout(Duration::from_secs(5), rx.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
}
async fn abort_held(
    f: &Fixture,
    proxy: &Proxy,
    task: JoinHandle<Result<Run, crate::scanner_files::Error>>,
    weak: Weak<Caller>,
) {
    wait_blocked(proxy).await;
    task.abort();
    let _ = task.await;
    assert!(weak.upgrade().is_some());
    assert_eq!(f.worker.available_slots(), 0);
    assert!(!f.worker.shutdown(Duration::from_millis(20)).await);
    proxy.hold.send_replace(false);
    assert!(f.worker.shutdown(Duration::from_secs(5)).await);
    assert!(weak.upgrade().is_none());
}
fn start(
    f: &Fixture,
) -> (
    JoinHandle<Result<Run, crate::scanner_files::Error>>,
    Weak<Caller>,
) {
    let caller = Arc::new(Caller);
    let weak = Arc::downgrade(&caller);
    let worker = f.worker.clone();
    (
        tokio::spawn(async move { worker.run_once(caller).await }),
        weak,
    )
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn a_sql_lock_after_early_ack_keeps_caller_until_rollback_close() {
    let f = Fixture::new(BROKER, RESULT, CACHE, CACHE).await;
    let mut c = sqlx::PgConnection::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    let mut tx = c.begin().await.unwrap();
    sqlx::query("LOCK TABLE m3u_m3uaccount IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await
        .unwrap();
    let (task, weak) = start(&f);
    tokio::time::timeout(Duration::from_secs(5),async{loop{sqlx::query("SELECT pg_stat_clear_snapshot()").execute(&mut*tx).await.unwrap();let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%m3u_m3uaccount%')").fetch_one(&mut*tx).await.unwrap();if blocked{break}tokio::time::sleep(Duration::from_millis(5)).await}}).await.unwrap();
    let n: i64 = redis::cmd("LLEN")
        .arg("rust-owned-scanner-parents")
        .query_async(&mut redis(8).await)
        .await
        .unwrap();
    assert_eq!(n, 0);
    assert!(metadata(f.id).await.is_none());
    task.abort();
    let _ = task.await;
    assert!(weak.upgrade().is_some());
    assert!(!f.worker.shutdown(Duration::from_millis(20)).await);
    tx.commit().await.unwrap();
    c.close().await.unwrap();
    assert!(f.worker.shutdown(Duration::from_secs(5)).await);
    assert!(weak.upgrade().is_none());
    assert_eq!(f.rows().await, 1);
    f.cleanup().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn b_actual_child_lpush_reply_is_before_model_commit_and_retains_abort() {
    let p = proxy(Target::Child).await;
    let f = Fixture::new(&format!("{}/8", p.base), RESULT, CACHE, CACHE).await;
    p.hold.send_replace(true);
    let (task, weak) = start(&f);
    wait_blocked(&p).await;
    assert_eq!(f.rows().await, 0);
    let n: i64 = redis::cmd("LLEN")
        .arg("celery")
        .query_async(&mut redis(8).await)
        .await
        .unwrap();
    assert!(n >= 1);
    assert_eq!(own_children(f.id).await.len(), 1);
    assert!(metadata(f.id).await.is_none());
    abort_held(&f, &p, task, weak).await;
    assert_eq!(f.rows().await, 1);
    assert!(metadata(f.id).await.is_some());
    f.cleanup().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn c_actual_marker_set_reply_after_commit_retains_abort() {
    let p = proxy(Target::Marker).await;
    let f = Fixture::new(BROKER, RESULT, &format!("{}/0", p.base), CACHE).await;
    p.hold.send_replace(true);
    let (task, weak) = start(&f);
    wait_blocked(&p).await;
    assert_eq!(f.rows().await, 1);
    assert!(metadata(f.id).await.is_none());
    abort_held(&f, &p, task, weak).await;
    assert!(metadata(f.id).await.is_some());
    f.cleanup().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn d_actual_channels_reply_retains_abort_after_sql_and_children() {
    let p = proxy(Target::Script).await;
    let f = Fixture::new(BROKER, RESULT, CACHE, &format!("{}/0", p.base)).await;
    p.hold.send_replace(true);
    let (task, weak) = start(&f);
    wait_blocked(&p).await;
    assert_eq!(f.rows().await, 1);
    let delivered: i64 = redis::cmd("ZCARD")
        .arg("asgispecific.rust_owned_scanner!")
        .query_async(&mut redis(0).await)
        .await
        .unwrap();
    assert!(delivered > 0);
    assert!(metadata(f.id).await.is_none());
    abort_held(&f, &p, task, weak).await;
    assert!(metadata(f.id).await.is_some());
    f.cleanup().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn e_lost_committed_marker_reply_is_operational_without_guessed_terminal() {
    let p = proxy(Target::Marker).await;
    let f = Fixture::new(BROKER, RESULT, &format!("{}/0", p.base), CACHE).await;
    p.discard.send_replace(true);
    let answer = f.worker.run_once(Arc::new(())).await;
    assert!(answer.is_err());
    assert_eq!(f.rows().await, 1);
    assert_eq!(own_children(f.id).await.len(), 1);
    let marker: Option<Vec<u8>> = redis::cmd("GET")
        .arg(format!("processed_file:{}", f.file))
        .query_async(&mut redis(0).await)
        .await
        .unwrap();
    assert!(marker.is_some());
    assert!(metadata(f.id).await.is_none());
    assert!(!f.worker.first_completed());
    f.cleanup().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn f_committed_result_reply_past_timeout_keeps_caller_and_admission() {
    let p = proxy(Target::Script).await;
    let f = Fixture::new(BROKER, &format!("{}/9", p.base), CACHE, CACHE).await;
    p.hold.send_replace(true);
    let (task, weak) = start(&f);
    wait_blocked(&p).await;
    assert!(metadata(f.id).await.is_some());
    tokio::time::sleep(Duration::from_millis(2200)).await;
    abort_held(&f, &p, task, weak).await;
    assert_eq!(f.worker.available_slots(), 1);
    f.cleanup().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn g_lost_child_reply_rolls_back_model_but_preserves_real_orphan_child() {
    let p = proxy(Target::Child).await;
    let f = Fixture::new(&format!("{}/8", p.base), RESULT, CACHE, CACHE).await;
    p.discard.send_replace(true);
    let answer = f.worker.run_once(Arc::new(())).await;
    assert!(answer.is_err());
    assert_eq!(f.rows().await, 0);
    assert_eq!(own_children(f.id).await.len(), 1);
    assert!(metadata(f.id).await.is_none());
    assert!(!f.worker.first_completed());
    f.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn h_unknown_actual_source_producer_frame_retained_before_ack() {
    let f = Fixture::new(BROKER, RESULT, CACHE, CACHE).await;
    let before: Vec<Vec<u8>> = redis::cmd("LRANGE")
        .arg("rust-owned-scanner-parents")
        .arg(0)
        .arg(2)
        .query_async(&mut redis(8).await)
        .await
        .unwrap();
    assert_eq!(before.len(), 1);
    assert_eq!(
        f.worker.run_once(Arc::new(())).await.unwrap(),
        Run::UnsupportedHead
    );
    let after: Vec<Vec<u8>> = redis::cmd("LRANGE")
        .arg("rust-owned-scanner-parents")
        .arg(0)
        .arg(2)
        .query_async(&mut redis(8).await)
        .await
        .unwrap();
    assert_eq!(before, after);
    assert!(metadata(f.id).await.is_none());
    assert_eq!(f.rows().await, 0);
    assert!(!f.worker.first_completed());
    f.cleanup().await;
}
