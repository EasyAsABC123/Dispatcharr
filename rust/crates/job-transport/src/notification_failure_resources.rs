//! Actual SQL→typedFAILURE result physical controls; copied fixture only.
//! RESP2 proxy derives accepted cleanup QA; no new Source lifecycle inference.
//! Original Dispatcharr/Django/Celery/Kombu/Redis authors retain credit.
use crate::{
    decode::State,
    default_domains::KnownDomains,
    default_protocol::{Kind, Message, Normal},
    default_worker::DefaultWorker,
    transport::{Config, Jobs},
};
use sqlx::postgres::PgPoolOptions;
use std::{
    sync::{Arc, Weak},
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

struct Caller;
async fn direct(db: i64) -> redis::aio::MultiplexedConnection {
    redis::Client::open(
        std::env::var(if db == 8 {
            "CELERY_BROKER_URL"
        } else {
            "CELERY_RESULT_BACKEND"
        })
        .unwrap(),
    )
    .unwrap()
    .get_multiplexed_async_connection()
    .await
    .unwrap()
}
async fn prepare(code: &str) -> sqlx::PgPool {
    assert!(matches!(code, "P0001" | "P0002"));
    crate::default_peer::validate_fixture_redis();
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let db = std::env::var("POSTGRES_DB").unwrap();
    assert!(db.starts_with("rust_probe_backup_jobs_"));
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    KnownDomains::copied_fixture(pool.clone(), &db).unwrap();
    for db in [8, 9] {
        let _: String = redis::cmd("FLUSHDB")
            .query_async(&mut direct(db).await)
            .await
            .unwrap();
    }
    sqlx::query(
        "DROP TRIGGER IF EXISTS rust_notification_failure_resource ON core_systemnotification",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("DROP FUNCTION IF EXISTS rust_notification_failure_resource()")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE m3u_m3uaccountprofile SET is_active=false")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM core_notificationdismissal WHERE notification_id IN(SELECT id FROM core_systemnotification WHERE notification_key LIKE 'xc-exp-%')").execute(&pool).await.unwrap();
    sqlx::query("DELETE FROM core_systemnotification WHERE notification_key LIKE 'xc-exp-%'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO core_systemnotification(notification_key,notification_type,priority,source,title,message,action_data,is_active,admin_only,created_at,updated_at) VALUES('xc-exp-warning-owned-failure','warning','normal','system','Owned','Owned','{}',true,true,now(),now())").execute(&pool).await.unwrap();
    sqlx::query(&format!("CREATE FUNCTION rust_notification_failure_resource() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned physical notification failure' USING ERRCODE='{code}'; END $$")).execute(&pool).await.unwrap();
    sqlx::query("CREATE TRIGGER rust_notification_failure_resource BEFORE DELETE ON core_systemnotification FOR EACH ROW EXECUTE FUNCTION rust_notification_failure_resource()").execute(&pool).await.unwrap();
    pool
}
async fn cleanup(pool: sqlx::PgPool) {
    sqlx::query("DROP TRIGGER rust_notification_failure_resource ON core_systemnotification")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION rust_notification_failure_resource()")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
}
async fn jobs(result: &str, timeout: Duration) -> Jobs {
    Jobs::connect_defaults(
        &std::env::var("CELERY_BROKER_URL").unwrap(),
        result,
        Config {
            operations: 1,
            command_timeout: timeout,
            max_queued_jobs: 64,
        },
    )
    .await
    .unwrap()
}
async fn worker(jobs: &Jobs, pool: sqlx::PgPool) -> DefaultWorker {
    let bus = dispatcharr_realtime::RealtimeBus::connect(
        "redis://:playback-fixture-only@localhost:6379/0",
        dispatcharr_realtime::BusConfig::default(),
    )
    .await
    .unwrap();
    DefaultWorker::new(
        jobs.clone(),
        Arc::new(KnownDomains::with_notifications(pool, Arc::new(bus))),
    )
    .unwrap()
}
fn message(kind: Kind) -> Message {
    Message {
        task_id: Uuid::new_v4(),
        kind,
        expires: None,
    }
}
async fn raw(id: Uuid) -> Option<Vec<u8>> {
    redis::cmd("GET")
        .arg(format!("celery-tasks:celery-task-meta-{id}"))
        .query_async(&mut direct(9).await)
        .await
        .unwrap()
}
async fn warm(jobs: &Jobs) {
    jobs.write_default_success(
        &message(Kind::RedisBackendCleanup),
        Normal::Null,
        Arc::new(()),
    )
    .await
    .unwrap();
}
async fn assert_failure(id: Uuid) {
    let value = crate::decode::decode(&raw(id).await.unwrap(), id).unwrap();
    assert_eq!(value.state, State::Failure);
    assert_eq!(value.result["exc_module"], "django.db.utils");
    assert_eq!(value.result["exc_type"], "ProgrammingError");
    assert!(value
        .traceback
        .unwrap()
        .starts_with("Native Dispatcharr account expiration task diagnostic"));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn actual_failure_reply_cancel_and_timeout_retain_job_until_physical_settlement() {
    let pool = prepare("P0001").await;
    let proxy = proxy().await;
    let jobs = jobs(&format!("{}/9", proxy.base), Duration::from_millis(50)).await;
    warm(&jobs).await;
    let m = message(Kind::InactiveAccountNotifications);
    jobs.publish_default(&m, Arc::new(())).await.unwrap();
    let worker = worker(&jobs, pool.clone()).await;
    proxy.hold.send_replace(true);
    let caller = Arc::new(Caller);
    let weak: Weak<Caller> = Arc::downgrade(&caller);
    let own = worker.clone();
    let task = tokio::spawn(async move { own.run_once(caller).await });
    let mut blocked = proxy.blocked.clone();
    tokio::time::timeout(Duration::from_secs(3), blocked.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
    task.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_failure(m.task_id).await;
    assert!(!worker.shutdown(Duration::from_millis(25)).await);
    assert_eq!(jobs.available_slots(), 0);
    assert!(weak.upgrade().is_some());
    assert!(jobs.shutdown(Duration::from_millis(25)).await.is_err());
    proxy.hold.send_replace(false);
    assert!(worker.shutdown(Duration::from_secs(3)).await);
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    assert!(weak.upgrade().is_none());
    cleanup(pool).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn committed_failure_lost_reply_is_operational_never_guessed_completed() {
    let pool = prepare("P0001").await;
    let proxy = proxy().await;
    let jobs = jobs(&format!("{}/9", proxy.base), Duration::from_secs(2)).await;
    warm(&jobs).await;
    let m = message(Kind::InactiveAccountNotifications);
    jobs.publish_default(&m, Arc::new(())).await.unwrap();
    let worker = worker(&jobs, pool.clone()).await;
    proxy.discard.send_replace(true);
    let result = worker.run_once(Arc::new(())).await;
    assert!(result.is_err());
    assert_failure(m.task_id).await;
    assert!(worker.shutdown(Duration::from_secs(3)).await);
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    cleanup(pool).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn backend_denial_stays_operational_and_writes_no_terminal_record() {
    let pool = prepare("P0001").await;
    let name = format!("rust-notification-denied-{}", Uuid::new_v4().simple());
    let mut control = direct(9).await;
    let _: String = redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&name)
        .arg("on")
        .arg(">playback-fixture-only")
        .arg("~celery-tasks:*")
        .arg("&celery-tasks:*")
        .arg("+@all")
        .arg("-setex")
        .query_async(&mut control)
        .await
        .unwrap();
    let result = format!("redis://{name}:playback-fixture-only@localhost:6379/9");
    let jobs = jobs(&result, Duration::from_secs(2)).await;
    let m = message(Kind::InactiveAccountNotifications);
    jobs.publish_default(&m, Arc::new(())).await.unwrap();
    let worker = worker(&jobs, pool.clone()).await;
    assert!(worker.run_once(Arc::new(())).await.is_err());
    assert!(raw(m.task_id).await.is_none());
    assert!(worker.shutdown(Duration::from_secs(3)).await);
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    let _: i64 = redis::cmd("ACL")
        .arg("DELUSER")
        .arg(name)
        .query_async(&mut control)
        .await
        .unwrap();
    cleanup(pool).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn unobserved_sqlstate_is_not_rewritten_as_programming_error() {
    let pool = prepare("P0002").await;
    let jobs = jobs(
        &std::env::var("CELERY_RESULT_BACKEND").unwrap(),
        Duration::from_secs(2),
    )
    .await;
    let m = message(Kind::InactiveAccountNotifications);
    jobs.publish_default(&m, Arc::new(())).await.unwrap();
    let worker = worker(&jobs, pool.clone()).await;
    assert!(worker.run_once(Arc::new(())).await.is_err());
    assert!(raw(m.task_id).await.is_none());
    assert!(worker.shutdown(Duration::from_secs(3)).await);
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    cleanup(pool).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn known_failed_task_poller_continues_to_next_normal_family() {
    let pool = prepare("P0001").await;
    let jobs = jobs(
        &std::env::var("CELERY_RESULT_BACKEND").unwrap(),
        Duration::from_secs(2),
    )
    .await;
    let failed = message(Kind::InactiveAccountNotifications);
    let next = message(Kind::RedisBackendCleanup);
    jobs.publish_default(&failed, Arc::new(())).await.unwrap();
    jobs.publish_default(&next, Arc::new(())).await.unwrap();
    let worker = worker(&jobs, pool.clone()).await;
    let app =
        crate::default_application::DefaultApplication::prepare(worker, Duration::from_millis(25))
            .unwrap();
    app.start().unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while raw(next.task_id).await.is_none() {
        assert!(tokio::time::Instant::now() < deadline);
        assert!(!matches!(
            app.state(),
            crate::default_application::State::Fault(_)
        ));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_failure(failed.task_id).await;
    assert_eq!(
        crate::decode::decode(&raw(next.task_id).await.unwrap(), next.task_id)
            .unwrap()
            .state,
        State::Success
    );
    assert!(app.shutdown(Duration::from_secs(3)).await.drained);
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    cleanup(pool).await;
}
