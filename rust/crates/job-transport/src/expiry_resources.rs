//! Actual paused broker/result replies for cleanup-only expiry. Original
//! Dispatcharr/Celery authors retain credit; proxy derives accepted physical QA.
use crate::{
    cleanup_expiry::{CleanupExpiration, Revocation},
    decode::{self, State},
    default_domains::{Domains, Error as DomainError, KnownDomains},
    default_protocol::{Kind, Message, Normal},
    default_worker::DefaultWorker,
    transport::{Config, Jobs},
};
use dispatcharr_contracts::BoxFuture;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Weak,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};
use uuid::Uuid;
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
    skip: Arc<AtomicUsize>,
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
    let (blocked, observed) = watch::channel(false);
    let skip = Arc::new(AtomicUsize::new(0));
    let skipped = skip.clone();
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted=listener.accept()=>{
                    let(client,_)=accepted.unwrap();let address=address.clone();let mut hold=receiver.clone();let blocked=blocked.clone();let skip=skipped.clone();
                    connections.spawn(async move{
                        let server=TcpStream::connect(address).await.unwrap();
                        let(mut cr,mut cw)=client.into_split();let(sr,mut sw)=server.into_split();
                        let incoming=async move{
                            let mut sr=tokio::io::BufReader::new(sr);
                            loop{
                                let mut budget=524288;let buffer=reply_frame(&mut sr,&mut budget,0).await?;
                                let skip_this=*hold.borrow()&&skip.fetch_update(Ordering::SeqCst,Ordering::SeqCst,|value|value.checked_sub(1)).is_ok();
                                if *hold.borrow()&&!skip_this{
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
        skip,
        task,
    }
}

struct Caller;
struct Actual {
    domains: KnownDomains,
    calls: Arc<AtomicUsize>,
}
impl Domains for Actual {
    fn preflight(
        &self,
        kind: Kind,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), DomainError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.domains.preflight(kind, owner)
    }
    fn execute(
        &self,
        kind: Kind,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Normal, DomainError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.domains.execute(kind, owner)
    }
}
async fn direct(db: i64) -> redis::aio::MultiplexedConnection {
    let raw = std::env::var(if db == 8 {
        "CELERY_BROKER_URL"
    } else {
        "CELERY_RESULT_BACKEND"
    })
    .unwrap();
    redis::Client::open(raw)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}
async fn clear() {
    crate::default_peer::validate_fixture_redis();
    for db in [8, 9] {
        let mut c = direct(db).await;
        let _: String = redis::cmd("FLUSHDB").query_async(&mut c).await.unwrap();
    }
}
fn message(deadline: chrono::DateTime<chrono::Utc>) -> Message {
    Message {
        task_id: Uuid::new_v4(),
        kind: Kind::RedisBackendCleanup,
        expires: Some(
            CleanupExpiration::new(
                chrono::DateTime::from_timestamp_micros(deadline.timestamp_micros()).unwrap(),
                0,
            )
            .unwrap(),
        ),
    }
}
fn actual() -> (Arc<Actual>, Arc<AtomicUsize>, sqlx::PgPool) {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy(&std::env::var("DATABASE_URL").unwrap())
        .unwrap();
    let validated =
        KnownDomains::copied_fixture(pool.clone(), &std::env::var("POSTGRES_DB").unwrap()).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    (
        Arc::new(Actual {
            domains: validated,
            calls: calls.clone(),
        }),
        calls,
        pool,
    )
}
async fn raw(id: Uuid) -> Vec<u8> {
    let mut c = direct(9).await;
    redis::cmd("GET")
        .arg(format!("celery-tasks:celery-task-meta-{id}"))
        .query_async(&mut c)
        .await
        .unwrap()
}
async fn held(worker: &DefaultWorker, jobs: &Jobs, weak: &Weak<Caller>) {
    assert!(!worker.shutdown(Duration::from_millis(25)).await);
    assert!(weak.upgrade().is_some());
    assert_eq!(jobs.available_slots(), 0);
    assert!(jobs.shutdown(Duration::from_millis(25)).await.is_err());
}
async fn released(worker: &DefaultWorker, jobs: &Jobs, weak: &Weak<Caller>) {
    assert!(worker.shutdown(Duration::from_secs(3)).await);
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    let end = tokio::time::Instant::now() + Duration::from_secs(3);
    while weak.upgrade().is_some() {
        assert!(tokio::time::Instant::now() < end);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn revoked_result_logical_timeout_retains_receipt_and_warm_restores() {
    clear().await;
    let proxy = proxy().await;
    let jobs = Jobs::connect_defaults(
        &std::env::var("CELERY_BROKER_URL").unwrap(),
        &format!("{}/9", proxy.base),
        Config {
            operations: 1,
            command_timeout: Duration::from_millis(50),
            max_queued_jobs: 64,
        },
    )
    .await
    .unwrap();
    let warm = message(chrono::Utc::now() - chrono::Duration::minutes(1));
    jobs.write_default_revoked(&warm, Revocation::Expired, Arc::new(()))
        .await
        .unwrap();
    let m = message(chrono::Utc::now() - chrono::Duration::minutes(1));
    jobs.publish_default(&m, Arc::new(())).await.unwrap();
    let (actual, calls, pool) = actual();
    let worker = DefaultWorker::new(jobs.clone(), actual).unwrap();
    proxy.hold.send_replace(true);
    let caller = Arc::new(Caller);
    let weak = Arc::downgrade(&caller);
    let own = worker.clone();
    let task = tokio::spawn(async move { own.run_once(caller).await });
    let mut blocked = proxy.blocked.clone();
    tokio::time::timeout(Duration::from_secs(3), blocked.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
    task.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;
    held(&worker, &jobs, &weak).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let decoded = decode::decode(&raw(m.task_id).await, m.task_id).unwrap();
    assert_eq!(decoded.state, State::Revoked);
    assert_eq!(
        decoded.result["exc_message"],
        serde_json::json!(["expired"])
    );
    proxy.hold.send_replace(false);
    released(&worker, &jobs, &weak).await;
    let mut broker = direct(8).await;
    let queued: Vec<Vec<u8>> = redis::cmd("LRANGE")
        .arg("celery")
        .arg(0)
        .arg(8)
        .query_async(&mut broker)
        .await
        .unwrap();
    assert_eq!(queued.len(), 1);
    let restored: serde_json::Value = serde_json::from_slice(&queued[0]).unwrap();
    assert_eq!(restored["headers"]["redelivered"], true);
    let count: i64 = redis::cmd("HLEN")
        .arg("unacked")
        .query_async(&mut broker)
        .await
        .unwrap();
    assert_eq!(count, 0);
    pool.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn expiry_after_actual_held_peek_skips_body_and_retains_owner() {
    clear().await;
    let proxy = proxy().await;
    let jobs = Jobs::connect_defaults(
        &format!("{}/8", proxy.base),
        &std::env::var("CELERY_RESULT_BACKEND").unwrap(),
        Config {
            operations: 1,
            command_timeout: Duration::from_secs(2),
            max_queued_jobs: 64,
        },
    )
    .await
    .unwrap();
    assert!(jobs.peek_default(Arc::new(())).await.unwrap().is_none());
    let m = message(chrono::Utc::now() + chrono::Duration::milliseconds(100));
    jobs.publish_default(&m, Arc::new(())).await.unwrap();
    let (actual, calls, pool) = actual();
    let worker = DefaultWorker::new(jobs.clone(), actual).unwrap();
    proxy.hold.send_replace(true);
    let caller = Arc::new(Caller);
    let weak = Arc::downgrade(&caller);
    let own = worker.clone();
    let task = tokio::spawn(async move { own.run_once(caller).await });
    let mut blocked = proxy.blocked.clone();
    tokio::time::timeout(Duration::from_secs(3), blocked.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    task.abort();
    assert!(!worker.shutdown(Duration::from_millis(25)).await);
    assert!(weak.upgrade().is_some());
    proxy.hold.send_replace(false);
    assert!(worker.shutdown(Duration::from_secs(3)).await);
    let value = decode::decode(&raw(m.task_id).await, m.task_id).unwrap();
    assert_eq!(value.state, State::Revoked);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    let end = tokio::time::Instant::now() + Duration::from_secs(3);
    while weak.upgrade().is_some() {
        assert!(tokio::time::Instant::now() < end);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn revoked_committed_reply_precedes_ack_and_preserves_foreign_tuple() {
    clear().await;
    let proxy = proxy().await;
    let jobs = Jobs::connect_defaults(
        &std::env::var("CELERY_BROKER_URL").unwrap(),
        &format!("{}/9", proxy.base),
        Config {
            operations: 1,
            command_timeout: Duration::from_secs(2),
            max_queued_jobs: 64,
        },
    )
    .await
    .unwrap();
    let warm = message(chrono::Utc::now() - chrono::Duration::minutes(1));
    jobs.write_default_revoked(&warm, Revocation::Expired, Arc::new(()))
        .await
        .unwrap();
    let mut broker = direct(8).await;
    let foreign = Uuid::new_v4().to_string();
    let _: i64 = redis::cmd("HSET")
        .arg("unacked")
        .arg(&foreign)
        .arg("foreign-opaque-preserved")
        .query_async(&mut broker)
        .await
        .unwrap();
    let _: i64 = redis::cmd("ZADD")
        .arg("unacked_index")
        .arg(123)
        .arg(&foreign)
        .query_async(&mut broker)
        .await
        .unwrap();
    for key in ["unacked", "unacked_index"] {
        let _: i64 = redis::cmd("EXPIRE")
            .arg(key)
            .arg(600)
            .query_async(&mut broker)
            .await
            .unwrap();
    }
    let m = message(chrono::Utc::now() - chrono::Duration::minutes(1));
    jobs.publish_default(&m, Arc::new(())).await.unwrap();
    let (actual, calls, pool) = actual();
    let worker = DefaultWorker::new(jobs.clone(), actual).unwrap();
    proxy.hold.send_replace(true);
    let caller = Arc::new(Caller);
    let weak = Arc::downgrade(&caller);
    let own = worker.clone();
    let task = tokio::spawn(async move { own.run_once(caller).await });
    let mut blocked = proxy.blocked.clone();
    tokio::time::timeout(Duration::from_secs(3), blocked.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
    let count: i64 = redis::cmd("HLEN")
        .arg("unacked")
        .query_async(&mut broker)
        .await
        .unwrap();
    assert_eq!(count, 2);
    let value = decode::decode(&raw(m.task_id).await, m.task_id).unwrap();
    assert_eq!(value.state, State::Revoked);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    task.abort();
    assert!(!worker.shutdown(Duration::from_millis(25)).await);
    assert!(weak.upgrade().is_some());
    assert!(jobs.shutdown(Duration::from_millis(25)).await.is_err());
    proxy.hold.send_replace(false);
    released(&worker, &jobs, &weak).await;
    let count: i64 = redis::cmd("HLEN")
        .arg("unacked")
        .query_async(&mut broker)
        .await
        .unwrap();
    let stored: String = redis::cmd("HGET")
        .arg("unacked")
        .arg(&foreign)
        .query_async(&mut broker)
        .await
        .unwrap();
    let score: f64 = redis::cmd("ZSCORE")
        .arg("unacked_index")
        .arg(&foreign)
        .query_async(&mut broker)
        .await
        .unwrap();
    assert_eq!(
        (count, stored.as_str(), score),
        (1, "foreign-opaque-preserved", 123.)
    );
    for key in ["unacked", "unacked_index"] {
        let ttl: i64 = redis::cmd("TTL")
            .arg(key)
            .query_async(&mut broker)
            .await
            .unwrap();
        assert!((550..=600).contains(&ttl));
    }
    pool.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn normal_unexpired_body_result_complete_while_early_ack_reply_is_held() {
    clear().await;
    let proxy = proxy().await;
    let jobs = Jobs::connect_defaults(
        &format!("{}/8", proxy.base),
        &std::env::var("CELERY_RESULT_BACKEND").unwrap(),
        Config {
            operations: 2,
            command_timeout: Duration::from_secs(2),
            max_queued_jobs: 64,
        },
    )
    .await
    .unwrap();
    let (actual, calls, pool) = actual();
    let worker = DefaultWorker::new(jobs.clone(), actual).unwrap();
    // Warm the real PEEK/RESERVE/ACK scripts, not an assumed response count.
    let mut warm = message(chrono::Utc::now() + chrono::Duration::hours(1));
    warm.expires.as_mut().unwrap().transport_millis = 3_600_000;
    jobs.publish_default(&warm, Arc::new(())).await.unwrap();
    assert!(matches!(
        worker.run_once(Arc::new(())).await.unwrap(),
        crate::default_worker::Run::Completed(_)
    ));
    calls.store(0, Ordering::SeqCst);
    let mut m = message(chrono::Utc::now() + chrono::Duration::seconds(1));
    m.expires.as_mut().unwrap().transport_millis = 1000;
    jobs.publish_default(&m, Arc::new(())).await.unwrap();
    proxy.skip.store(2, Ordering::SeqCst);
    proxy.hold.send_replace(true);
    let caller = Arc::new(Caller);
    let weak = Arc::downgrade(&caller);
    let own = worker.clone();
    let task = tokio::spawn(async move { own.run_once(caller).await });
    let mut blocked = proxy.blocked.clone();
    tokio::time::timeout(Duration::from_secs(3), blocked.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1250)).await;
    let value = decode::decode(&raw(m.task_id).await, m.task_id).unwrap();
    assert_eq!(value.state, State::Success);
    assert!(value.result.is_null());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let mut broker = direct(8).await;
    let count: i64 = redis::cmd("HLEN")
        .arg("unacked")
        .query_async(&mut broker)
        .await
        .unwrap();
    assert_eq!(count, 0);
    task.abort();
    assert!(!worker.shutdown(Duration::from_millis(25)).await);
    assert!(weak.upgrade().is_some());
    assert_eq!(jobs.available_slots(), 1);
    assert!(jobs.shutdown(Duration::from_millis(25)).await.is_err());
    proxy.hold.send_replace(false);
    released(&worker, &jobs, &weak).await;
    pool.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn one_credit_unexpired_scope_is_refused_before_pop() {
    clear().await;
    let jobs = Jobs::connect_defaults(
        &std::env::var("CELERY_BROKER_URL").unwrap(),
        &std::env::var("CELERY_RESULT_BACKEND").unwrap(),
        Config {
            operations: 1,
            command_timeout: Duration::from_secs(2),
            max_queued_jobs: 64,
        },
    )
    .await
    .unwrap();
    let (actual, calls, pool) = actual();
    let worker = DefaultWorker::new(jobs.clone(), actual).unwrap();
    let mut m = message(chrono::Utc::now() + chrono::Duration::hours(1));
    m.expires.as_mut().unwrap().transport_millis = 3_600_000;
    jobs.publish_default(&m, Arc::new(())).await.unwrap();
    let mut broker = direct(8).await;
    let before: Vec<u8> = redis::cmd("LINDEX")
        .arg("celery")
        .arg(-1)
        .query_async(&mut broker)
        .await
        .unwrap();
    assert!(matches!(
        worker.run_once(Arc::new(())).await.unwrap(),
        crate::default_worker::Run::Unsupported(Kind::RedisBackendCleanup)
    ));
    let after: Vec<u8> = redis::cmd("LINDEX")
        .arg("celery")
        .arg(-1)
        .query_async(&mut broker)
        .await
        .unwrap();
    assert_eq!(before, after);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(jobs.read(m.task_id, Arc::new(())).await.unwrap().is_none());
    assert!(worker.shutdown(Duration::from_secs(3)).await);
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    pool.close().await;
}
#[tokio::test]
async fn reply_barrier_counts_complete_frames_and_rejects_oversized_frames() {
    let (mut writer, reader) = tokio::io::duplex(64);
    let bytes = b"*2\r\n:1\r\n$3\r\nabc\r\n:2\r\n".to_vec();
    let expected = bytes.clone();
    let send = tokio::spawn(async move {
        for byte in bytes {
            writer.write_all(&[byte]).await.unwrap();
            tokio::task::yield_now().await;
        }
    });
    let mut reader = tokio::io::BufReader::new(reader);
    let mut budget = 512;
    let first = reply_frame(&mut reader, &mut budget, 0).await.unwrap();
    let second = reply_frame(&mut reader, &mut budget, 0).await.unwrap();
    assert_eq!([first, second].concat(), expected);
    send.await.unwrap();
    for bad in [
        b"$524289\r\n".as_slice(),
        b"*129\r\n".as_slice(),
        b"!wrong\r\n".as_slice(),
    ] {
        let mut reader = bad;
        let mut budget = 524288;
        assert_eq!(
            reply_frame(&mut reader, &mut budget, 0)
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidData
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn receipt_pool_cap_origin_and_replacement_fences() {
    clear().await;
    let broker_url = std::env::var("CELERY_BROKER_URL").unwrap();
    let result_url = std::env::var("CELERY_RESULT_BACKEND").unwrap();
    let config = Config {
        operations: 2,
        command_timeout: Duration::from_secs(2),
        max_queued_jobs: 64,
    };
    let jobs = Jobs::connect_defaults(&broker_url, &result_url, config.clone())
        .await
        .unwrap();
    let other = Jobs::connect_defaults(&broker_url, &result_url, config)
        .await
        .unwrap();
    let mut receipts = Vec::new();
    let mut callers = Vec::new();
    for _ in 0..2 {
        let m = message(chrono::Utc::now() - chrono::Duration::minutes(1));
        jobs.publish_default(&m, Arc::new(())).await.unwrap();
        let caller = Arc::new(Caller);
        callers.push(Arc::downgrade(&caller));
        let candidate = jobs.peek_default(caller).await.unwrap().unwrap();
        receipts.push(jobs.reserve_cleanup(candidate).await.unwrap());
    }
    let m = message(chrono::Utc::now() - chrono::Duration::minutes(1));
    jobs.publish_default(&m, Arc::new(())).await.unwrap();
    let candidate = jobs.peek_default(Arc::new(())).await.unwrap().unwrap();
    assert!(matches!(
        jobs.reserve_cleanup(candidate).await,
        Err(crate::transport::Error::Capacity)
    ));
    let mut broker = direct(8).await;
    let queued: i64 = redis::cmd("LLEN")
        .arg("celery")
        .query_async(&mut broker)
        .await
        .unwrap();
    let pending: i64 = redis::cmd("HLEN")
        .arg("unacked")
        .query_async(&mut broker)
        .await
        .unwrap();
    assert_eq!((queued, pending), (1, 2));
    assert_eq!(jobs.available_slots(), 2);
    assert!(callers.iter().all(|v| v.upgrade().is_some()));
    assert!(matches!(
        other.ack_cleanup(&receipts[0]).await,
        Err(crate::transport::Error::Configuration)
    ));
    assert!(matches!(
        other.restore_cleanup(&receipts[0]).await,
        Err(crate::transport::Error::Configuration)
    ));
    assert_eq!(other.available_slots(), 2);
    assert!(jobs.shutdown(Duration::from_millis(25)).await.is_err());
    let pairs: Vec<(String, Vec<u8>)> = redis::cmd("HGETALL")
        .arg("unacked")
        .query_async(&mut broker)
        .await
        .unwrap();
    let target = receipts[1].message().task_id.to_string();
    let (tag, tuple) = pairs
        .into_iter()
        .find(|(_, v)| {
            serde_json::from_slice::<serde_json::Value>(v).unwrap()[0]["headers"]["id"] == target
        })
        .unwrap();
    let foreign = vec![b'x'; tuple.len()];
    let _: i64 = redis::cmd("HSET")
        .arg("unacked")
        .arg(&tag)
        .arg(&foreign)
        .query_async(&mut broker)
        .await
        .unwrap();
    assert!(!jobs.ack_cleanup(&receipts[1]).await.unwrap());
    assert!(!jobs.restore_cleanup(&receipts[1]).await.unwrap());
    let unchanged: Vec<u8> = redis::cmd("HGET")
        .arg("unacked")
        .arg(&tag)
        .query_async(&mut broker)
        .await
        .unwrap();
    assert_eq!(foreign, unchanged);
    assert!(jobs.restore_cleanup(&receipts[0]).await.unwrap());
    drop(receipts);
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    assert!(callers.iter().all(|v| v.upgrade().is_none()));
    // Shared Lua bounds reject an oversized foreign tuple before destructive POP.
    let huge = Uuid::new_v4().to_string();
    let _: i64 = redis::cmd("HSET")
        .arg("unacked")
        .arg(&huge)
        .arg(vec![b'z'; 131073])
        .query_async(&mut broker)
        .await
        .unwrap();
    let _: i64 = redis::cmd("ZADD")
        .arg("unacked_index")
        .arg(123)
        .arg(&huge)
        .query_async(&mut broker)
        .await
        .unwrap();
    let candidate = other.peek_default(Arc::new(())).await.unwrap().unwrap();
    let before: i64 = redis::cmd("LLEN")
        .arg("celery")
        .query_async(&mut broker)
        .await
        .unwrap();
    assert!(matches!(
        other.reserve_cleanup(candidate).await,
        Err(crate::transport::Error::Corrupt)
    ));
    let after: i64 = redis::cmd("LLEN")
        .arg("celery")
        .query_async(&mut broker)
        .await
        .unwrap();
    assert_eq!(before, after);
    let size: i64 = redis::cmd("HSTRLEN")
        .arg("unacked")
        .arg(&huge)
        .query_async(&mut broker)
        .await
        .unwrap();
    assert_eq!(size, 131073);
    other.shutdown(Duration::from_secs(3)).await.unwrap();
}
