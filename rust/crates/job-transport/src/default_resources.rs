//! Actual fixture Redis/SQL ownership controls; fake domains are explicitly
//! resource stubs, not evidence of additional Source task semantics.
//! Original Dispatcharr authors retain application/algorithm credit/licenses.
use crate::{
    default_application::{DefaultApplication, State},
    default_domains::{Domains, Error as DomainError, KnownDomains},
    default_protocol::{Kind, Message, Normal},
    default_worker::{DefaultWorker, Run},
    transport::{Config, Error, Jobs},
};
use dispatcharr_contracts::BoxFuture;
use sqlx::{postgres::PgPoolOptions, Connection};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use uuid::Uuid;
struct Owner(Arc<AtomicBool>);
impl Drop for Owner {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
fn owner() -> (Arc<dyn Send + Sync>, Arc<AtomicBool>) {
    let flag = Arc::new(AtomicBool::new(false));
    (Arc::new(Owner(flag.clone())), flag)
}
async fn wait(test: impl Fn() -> bool) {
    let end = tokio::time::Instant::now() + Duration::from_secs(3);
    while !test() {
        assert!(tokio::time::Instant::now() < end, "Owned control deadline");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
async fn connection(key: &str) -> redis::aio::MultiplexedConnection {
    redis::Client::open(std::env::var(key).unwrap())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}
async fn flush() {
    for key in ["CELERY_BROKER_URL", "CELERY_RESULT_BACKEND"] {
        let mut c = connection(key).await;
        let _: String = redis::cmd("FLUSHDB").query_async(&mut c).await.unwrap();
    }
}
async fn pause() {
    let mut c = connection("CELERY_BROKER_URL").await;
    let _: String = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(500)
        .query_async(&mut c)
        .await
        .unwrap();
}
async fn jobs(timeout: Duration) -> Jobs {
    Jobs::connect_fixture(
        &std::env::var("CELERY_BROKER_URL").unwrap(),
        &std::env::var("CELERY_RESULT_BACKEND").unwrap(),
        &std::env::var("POSTGRES_DB").unwrap(),
        Config {
            operations: 1,
            command_timeout: timeout,
            max_queued_jobs: 64,
        },
    )
    .await
    .unwrap()
}
fn message(kind: Kind) -> Message {
    Message {
        task_id: Uuid::new_v4(),
        kind,
        expires: None,
    }
}
async fn publish(jobs: &Jobs, m: &Message) {
    jobs.publish_default(m, Arc::new(())).await.unwrap();
}
async fn metadata(id: Uuid) -> Option<Vec<u8>> {
    let mut c = connection("CELERY_RESULT_BACKEND").await;
    redis::cmd("GET")
        .arg(format!("celery-tasks:celery-task-meta-{id}"))
        .query_async(&mut c)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn shared_candidate_budget_origin_and_claim_race() {
    flush().await;
    let a = jobs(Duration::from_secs(2)).await;
    let b = jobs(Duration::from_secs(2)).await;
    let m = message(Kind::RedisBackendCleanup);
    publish(&a, &m).await;
    let (guard, dropped) = owner();
    let candidate = a.peek_default(guard).await.unwrap().unwrap();
    assert_eq!(a.available_slots(), 0);
    assert!(!dropped.load(Ordering::SeqCst));
    assert!(matches!(
        a.peek_default(Arc::new(())).await,
        Err(Error::Capacity)
    ));
    assert!(matches!(
        b.claim_default(candidate).await,
        Err(Error::Configuration)
    ));
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(a.available_slots(), 1);
    assert_eq!(b.available_slots(), 1);
    let candidate = a.peek_default(Arc::new(())).await.unwrap().unwrap();
    let mut control = connection("CELERY_BROKER_URL").await;
    let _: Option<Vec<u8>> = redis::cmd("RPOP")
        .arg("celery")
        .query_async(&mut control)
        .await
        .unwrap();
    assert!(matches!(
        a.claim_default(candidate).await,
        Err(Error::ClaimRace)
    ));
    a.shutdown(Duration::from_secs(2)).await.unwrap();
    b.shutdown(Duration::from_secs(2)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn actual_peek_claim_reply_cancellation_keeps_shared_permits() {
    for claim in [false, true] {
        flush().await;
        let j = jobs(Duration::from_millis(50)).await;
        let m = message(Kind::RedisBackendCleanup);
        publish(&j, &m).await;
        let (guard, dropped) = owner();
        let candidate = if claim {
            Some(j.peek_default(guard.clone()).await.unwrap().unwrap())
        } else {
            None
        };
        pause().await;
        let owned = j.clone();
        let task = tokio::spawn(async move {
            if let Some(candidate) = candidate {
                drop(guard);
                let _ = owned.claim_default(candidate).await;
            } else {
                let _ = owned.peek_default(guard).await;
            }
        });
        tokio::time::sleep(Duration::from_millis(15)).await;
        task.abort();
        let _ = task.await;
        assert_eq!(j.available_slots(), 0);
        assert!(!dropped.load(Ordering::SeqCst));
        assert!(j.shutdown(Duration::from_millis(25)).await.is_err());
        assert!(!dropped.load(Ordering::SeqCst));
        wait(|| dropped.load(Ordering::SeqCst)).await;
        assert_eq!(j.available_slots(), 1);
        j.shutdown(Duration::from_secs(2)).await.unwrap();
        let mut control = connection("CELERY_BROKER_URL").await;
        let count: i64 = redis::cmd("LLEN")
            .arg("celery")
            .query_async(&mut control)
            .await
            .unwrap();
        assert_eq!(count, if claim { 0 } else { 1 });
        assert!(metadata(m.task_id).await.is_none());
    }
}
struct Stub {
    calls: Arc<AtomicUsize>,
    pause_result: bool,
    fault: bool,
}
impl Domains for Stub {
    fn preflight(
        &self,
        _kind: Kind,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), DomainError>> {
        Box::pin(async move {
            let _owner = owner;
            if self.fault {
                Err(DomainError::Panic)
            } else {
                Ok(())
            }
        })
    }
    fn execute(
        &self,
        kind: Kind,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Normal, DomainError>> {
        Box::pin(async move {
            let _owner = owner;
            if self.pause_result {
                pause().await;
            }
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(match kind {
                Kind::RedisBackendCleanup | Kind::InactiveAccountNotifications => Normal::Null,
                Kind::RecurringMaintenance => Normal::Zero,
                Kind::LogoScanner => return Err(DomainError::Unsupported),
            })
        })
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn actual_result_reply_cancel_and_same_id_reexec_guard() {
    flush().await;
    let j = jobs(Duration::from_millis(50)).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let w = DefaultWorker::new_fixture(
        j.clone(),
        Arc::new(Stub {
            calls: calls.clone(),
            pause_result: true,
            fault: false,
        }),
    )
    .unwrap();
    let m = message(Kind::RedisBackendCleanup);
    publish(&j, &m).await;
    let (guard, dropped) = owner();
    let worker = w.clone();
    let task = tokio::spawn(async move { worker.run_once(guard).await });
    wait(|| calls.load(Ordering::SeqCst) == 1 && j.available_slots() == 0).await;
    task.abort();
    let _ = task.await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(!dropped.load(Ordering::SeqCst));
    assert!(!w.shutdown(Duration::from_millis(25)).await);
    assert!(j.shutdown(Duration::from_millis(25)).await.is_err());
    wait(|| dropped.load(Ordering::SeqCst)).await;
    assert!(w.shutdown(Duration::from_secs(2)).await);
    j.shutdown(Duration::from_secs(2)).await.unwrap();
    let previous = metadata(m.task_id).await.unwrap();
    let j = jobs(Duration::from_secs(2)).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let worker = DefaultWorker::new_fixture(
        j.clone(),
        Arc::new(Stub {
            calls: calls.clone(),
            pause_result: false,
            fault: false,
        }),
    )
    .unwrap();
    for _ in 0..2 {
        publish(&j, &m).await;
        assert!(
            matches!(worker.run_once(Arc::new(())).await.unwrap(),Run::AlreadySucceeded(id)if id==m.task_id)
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(metadata(m.task_id).await.unwrap(), previous);
    assert!(worker.shutdown(Duration::from_secs(2)).await);
    j.shutdown(Duration::from_secs(2)).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn actual_sql_reply_caller_retention_and_poller_fault_visibility() {
    flush().await;
    let url = std::env::var("DATABASE_URL").unwrap();
    let database = std::env::var("POSTGRES_DB").unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let domains = Arc::new(KnownDomains::copied_fixture(pool.clone(), &database).unwrap());
    let j = jobs(Duration::from_secs(2)).await;
    let worker = DefaultWorker::new_fixture(j.clone(), domains).unwrap();
    let m = message(Kind::RecurringMaintenance);
    publish(&j, &m).await;
    let mut control = sqlx::PgConnection::connect(&url).await.unwrap();
    let mut tx = control.begin().await.unwrap();
    sqlx::query("LOCK TABLE dispatcharr_channels_recurringrecordingrule IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await
        .unwrap();
    let (guard, dropped) = owner();
    let active = worker.clone();
    let task = tokio::spawn(async move { active.run_once(guard).await });
    wait(|| pool.num_idle() == 0 && j.available_slots() == 0).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    task.abort();
    let _ = task.await;
    assert!(!worker.shutdown(Duration::from_millis(25)).await);
    assert!(!dropped.load(Ordering::SeqCst));
    tx.rollback().await.unwrap();
    wait(|| dropped.load(Ordering::SeqCst)).await;
    assert!(worker.shutdown(Duration::from_secs(2)).await);
    assert!(metadata(m.task_id).await.is_some());
    j.shutdown(Duration::from_secs(2)).await.unwrap();
    pool.close().await;
    flush().await;
    let j = jobs(Duration::from_secs(2)).await;
    let worker = DefaultWorker::new_fixture(
        j.clone(),
        Arc::new(Stub {
            calls: Arc::new(AtomicUsize::new(0)),
            pause_result: false,
            fault: true,
        }),
    )
    .unwrap();
    let m = message(Kind::RedisBackendCleanup);
    publish(&j, &m).await;
    let app = DefaultApplication::prepare(worker, Duration::from_millis(25)).unwrap();
    app.start().unwrap();
    wait(|| matches!(app.state(), State::Fault(_))).await;
    let drain = app.shutdown(Duration::from_secs(2)).await;
    assert!(drain.drained);
    assert!(matches!(drain.state, State::Fault(_)));
    assert!(metadata(m.task_id).await.is_none());
    j.shutdown(Duration::from_secs(2)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn production_family_authorities_cannot_publish_or_peek_defaults() {
    flush().await;
    let broker = std::env::var("CELERY_BROKER_URL").unwrap();
    let backend = std::env::var("CELERY_RESULT_BACKEND").unwrap();
    let config = Config {
        operations: 1,
        command_timeout: Duration::from_secs(2),
        max_queued_jobs: 64,
    };
    let index = Jobs::connect_index(&broker, &backend, config.clone())
        .await
        .unwrap();
    let refresh = Jobs::connect_refresh(&broker, &backend, config.clone())
        .await
        .unwrap();
    let series = Jobs::connect_series(&broker, &backend, config)
        .await
        .unwrap();
    for jobs in [index, refresh, series] {
        assert!(jobs.copied_database().is_none());
        assert!(matches!(
            jobs.publish_default(&message(Kind::RedisBackendCleanup), Arc::new(()))
                .await,
            Err(Error::UnsupportedTask)
        ));
        assert!(matches!(
            jobs.peek_default(Arc::new(())).await,
            Err(Error::UnsupportedTask)
        ));
        assert_eq!(jobs.available_slots(), 1);
        jobs.shutdown(Duration::from_secs(2)).await.unwrap();
    }
    let mut control = connection("CELERY_BROKER_URL").await;
    let count: i64 = redis::cmd("LLEN")
        .arg("celery")
        .query_async(&mut control)
        .await
        .unwrap();
    assert_eq!(count, 0);
}
