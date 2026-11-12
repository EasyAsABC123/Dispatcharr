//! Private copied-DB logo scanner physical controls. Not a task consumer.
use super::{
    logo_scanner::{Error, Limits, Roots, Scanner},
    logo_scanner_effects::RetainedEffects,
};
use dispatcharr_contracts::{BoxFuture, UpdateEventError, UpdateEventSink};
use dispatcharr_output_store::transport::{RedisTransport, TransportConfig};
use sqlx::Connection;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
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
struct Proxy {
    base: String,
    hold: watch::Sender<bool>,
    blocked: watch::Receiver<bool>,
    discard: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn proxy() -> Proxy {
    let raw = std::env::var("CELERY_BROKER_URL").expect("Explicit owned fixture Redis required");
    let client = redis::Client::open(raw).unwrap();
    let info = client.get_connection_info();
    assert_eq!(
        info.redis.password.as_deref(),
        Some("playback-fixture-only")
    );
    let address = match &info.addr {
        redis::ConnectionAddr::Tcp(host, port)
            if matches!(host.as_str(), "localhost" | "127.0.0.1") =>
        {
            format!("{host}:{port}")
        }
        _ => panic!("Owned loopback fixture required"),
    };
    let mut bound = None;
    for port in 61000..61128 {
        if let Ok(value) = TcpListener::bind(("127.0.0.1", port)).await {
            bound = Some(value);
            break;
        }
    }
    let listener = bound.expect("Proxy port bound");
    let port = listener.local_addr().unwrap().port();
    let (hold, receiver) = watch::channel(false);
    let (discard, discarded) = watch::channel(false);
    let (blocked, observed) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted=listener.accept()=>{
                    let(client,_)=accepted.unwrap();let address=address.clone();let mut hold=receiver.clone();let blocked=blocked.clone();let discard=discarded.clone();
                    connections.spawn(async move{
                        let server=TcpStream::connect(address).await.unwrap();
                        let(mut cr,mut cw)=client.into_split();let(sr,mut sw)=server.into_split();
                        let incoming=async move{
                            let mut sr=tokio::io::BufReader::new(sr);
                            loop{
                                let mut budget=524288;let buffer=reply_frame(&mut sr,&mut budget,0).await?;
                                if *discard.borrow(){blocked.send_replace(true);return Err(std::io::Error::from(std::io::ErrorKind::ConnectionReset));}
                                if *hold.borrow(){
                                    blocked.send_replace(true);
                                    if hold.wait_for(|paused|!*paused).await.is_err(){return Ok::<(),std::io::Error>(());}
                                }
                                cw.write_all(&buffer).await?;
                            }
                        };
                        let _=tokio::try_join!(async{tokio::io::copy(&mut cr,&mut sw).await.map(|_|())},incoming);
                    });
                },
                _=connections.join_next(),if !connections.is_empty()=>{},
            }
        }
    });
    Proxy {
        base: format!("redis://:playback-fixture-only@127.0.0.1:{port}"),
        hold,
        blocked: observed,
        discard,
        task,
    }
}

struct QuietSink;
impl UpdateEventSink for QuietSink {
    fn send<'a>(
        &'a self,
        _group: &'a str,
        _event: serde_json::Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async { Ok(()) })
    }
}
async fn setup(cache_url: &str, sink: Arc<dyn UpdateEventSink>) -> (Scanner, sqlx::PgPool, String) {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    assert!(std::env::var("POSTGRES_DB")
        .unwrap()
        .starts_with("rust_probe_backup_jobs_"));
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    sqlx::query("UPDATE epg_epgsource SET is_active=false")
        .execute(&pool)
        .await
        .unwrap();
    let path = format!(
        "/data/logos/rust-owned-physical-{}.png",
        uuid::Uuid::new_v4()
    );
    let file = std::fs::File::create(&path).unwrap();
    file.set_times(
        std::fs::FileTimes::new()
            .set_modified(std::time::UNIX_EPOCH + Duration::from_millis(1699999992125)),
    )
    .unwrap();
    let cache = RedisTransport::connect(
        cache_url,
        TransportConfig {
            max_operations: 2,
            max_queued_operations: 2,
            command_timeout: Duration::from_millis(50),
        },
    )
    .await
    .unwrap();
    let scanner = Scanner::new(
        Arc::new(RetainedEffects::new(pool.clone(), cache, sink)),
        Roots {
            m3u: "/data/m3us".into(),
            epg: "/data/epgs".into(),
            logos: "/data/logos".into(),
        },
        Limits::default(),
    )
    .unwrap();
    (scanner, pool, path)
}
async fn cleanup(pool: sqlx::PgPool, path: String) {
    sqlx::query("DELETE FROM dispatcharr_channels_logo WHERE url=$1")
        .bind(&path)
        .execute(&pool)
        .await
        .unwrap();
    std::fs::remove_file(path).unwrap();
    pool.close().await;
}
async fn wait(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await
        }
    })
    .await
    .unwrap()
}
const CACHE: &str = "redis://:playback-fixture-only@localhost:6379/0";
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn actual_sql_lock_caller_abort_retains_admission_until_reply() {
    let (scanner, pool, path) = setup(CACHE, Arc::new(QuietSink)).await;
    let mut blocker = sqlx::PgConnection::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    let mut transaction = blocker.begin().await.unwrap();
    sqlx::query("LOCK TABLE dispatcharr_channels_logo IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *transaction)
        .await
        .unwrap();
    let owner = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let copied = scanner.clone();
    let task = tokio::spawn(async move { copied.scan(1700000000.125, owner).await });
    tokio::time::timeout(Duration::from_secs(5),async{loop{sqlx::query("SELECT pg_stat_clear_snapshot()").execute(&mut *transaction).await.unwrap();let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT id::bigint FROM dispatcharr_channels_logo%')").fetch_one(&mut *transaction).await.unwrap();if blocked{break}tokio::time::sleep(Duration::from_millis(5)).await}}).await.unwrap();
    assert!(matches!(
        scanner.scan(1700000000.125, Arc::new(())).await,
        Err(Error::Capacity)
    ));
    task.abort();
    let _ = task.await;
    assert!(weak.upgrade().is_some());
    assert!(!scanner.shutdown(Duration::from_millis(20)).await);
    transaction.rollback().await.unwrap();
    blocker.close().await.unwrap();
    assert!(scanner.shutdown(Duration::from_secs(5)).await);
    assert!(weak.upgrade().is_none());
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_logo WHERE url=$1)")
            .bind(&path)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(exists);
    cleanup(pool, path).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn actual_cache_reply_timeout_caller_abort_waits_before_operational_result() {
    let p = proxy().await;
    let (scanner, pool, path) = setup(&format!("{}/0", p.base), Arc::new(QuietSink)).await;
    p.hold.send_replace(true);
    let owner = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let copied = scanner.clone();
    let task = tokio::spawn(async move { copied.scan(1700000000.125, owner).await });
    wait(|| *p.blocked.borrow()).await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(!task.is_finished());
    task.abort();
    let _ = task.await;
    assert!(weak.upgrade().is_some());
    assert!(!scanner.shutdown(Duration::from_millis(20)).await);
    p.hold.send_replace(false);
    assert!(scanner.shutdown(Duration::from_secs(5)).await);
    assert!(weak.upgrade().is_none());
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_logo WHERE url=$1)")
            .bind(&path)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!exists);
    cleanup(pool, path).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn actual_channels_reply_held_after_commits_retains_caller_until_delivery_settles() {
    let p = proxy().await;
    let bus = dispatcharr_realtime::RealtimeBus::connect(
        &format!("{}/0", p.base),
        dispatcharr_realtime::BusConfig {
            command_timeout: Duration::from_millis(50),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let (scanner, pool, path) = setup(CACHE, Arc::new(bus)).await;
    p.hold.send_replace(true);
    let owner = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let copied = scanner.clone();
    let task = tokio::spawn(async move { copied.scan(1700000000.125, owner).await });
    wait(|| *p.blocked.borrow()).await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(!task.is_finished());
    task.abort();
    let _ = task.await;
    assert!(weak.upgrade().is_some());
    assert!(!scanner.shutdown(Duration::from_millis(20)).await);
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_logo WHERE url=$1)")
            .bind(&path)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(exists);
    p.hold.send_replace(false);
    assert!(scanner.shutdown(Duration::from_secs(5)).await);
    assert!(weak.upgrade().is_none());
    cleanup(pool, path).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn lost_cache_reply_remains_operational_without_guessed_logo_commit() {
    let p = proxy().await;
    let (scanner, pool, path) = setup(&format!("{}/0", p.base), Arc::new(QuietSink)).await;
    p.discard.send_replace(true);
    let answer = tokio::time::timeout(
        Duration::from_secs(5),
        scanner.scan(1700000000.125, Arc::new(())),
    )
    .await
    .unwrap();
    assert!(matches!(answer, Err(Error::Operational)));
    assert!(scanner.shutdown(Duration::from_secs(5)).await);
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_logo WHERE url=$1)")
            .bind(&path)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!exists);
    cleanup(pool, path).await;
}
