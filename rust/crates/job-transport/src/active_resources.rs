//! Owned actual SQL/Channels reply tests. No new Source failure-payload claim.
use crate::{
    default_domains::KnownDomains,
    default_protocol::{Kind, Message},
    default_worker::{DefaultWorker, Run},
    transport::{Config, Error, Jobs},
};
use dispatcharr_contracts::{BoxFuture, UpdateEventError, UpdateEventSink};
use dispatcharr_realtime::{BusConfig, RealtimeBus};
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, Connection, PgPool};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};
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
async fn redis() -> redis::aio::MultiplexedConnection {
    redis::Client::open(std::env::var("CELERY_BROKER_URL").unwrap())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}
async fn pool() -> PgPool {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    assert!(std::env::var("POSTGRES_DB")
        .unwrap()
        .starts_with("rust_probe_backup_jobs_"));
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    let _validated =
        KnownDomains::copied_fixture(pool.clone(), &std::env::var("POSTGRES_DB").unwrap()).unwrap();
    pool
}
async fn jobs() -> Jobs {
    crate::default_peer::validate_fixture_redis();
    Jobs::connect_defaults(
        &std::env::var("CELERY_BROKER_URL").unwrap(),
        &std::env::var("CELERY_RESULT_BACKEND").unwrap(),
        Config {
            operations: 2,
            command_timeout: Duration::from_secs(2),
            max_queued_jobs: 64,
        },
    )
    .await
    .unwrap()
}
async fn reset(pool: &PgPool) {
    sqlx::query(
        "UPDATE m3u_m3uaccountprofile SET is_active=false,name='Owned Resource Profile-'||id::text",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("UPDATE m3u_m3uaccount SET is_active=false")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE m3u_m3uaccountprofile SET is_active=true,name='Owned Resource Profile',exp_date=now()+interval '1 day' WHERE id=(SELECT min(id) FROM m3u_m3uaccountprofile)").execute(pool).await.unwrap();
    sqlx::query("UPDATE m3u_m3uaccount SET is_active=true WHERE id IN (SELECT m3u_account_id FROM m3u_m3uaccountprofile WHERE is_active)").execute(pool).await.unwrap();
    let mut c = redis().await;
    let _: String = redis::cmd("FLUSHDB").query_async(&mut c).await.unwrap();
    sqlx::query("DELETE FROM core_notificationdismissal WHERE notification_id IN (SELECT id FROM core_systemnotification WHERE notification_key LIKE 'xc-exp-%')").execute(pool).await.unwrap();
    sqlx::query("DELETE FROM core_systemnotification WHERE notification_key LIKE 'xc-exp-%'")
        .execute(pool)
        .await
        .unwrap();
}
async fn insert(pool: &PgPool, key: &str) {
    sqlx::query("INSERT INTO core_systemnotification(notification_key,notification_type,priority,source,title,message,action_data,is_active,admin_only,created_at,updated_at) VALUES($1,'warning','normal','system','Owned','Owned','{}',true,false,now(),now())").bind(key).execute(pool).await.unwrap();
}
async fn bus() -> RealtimeBus {
    RealtimeBus::connect(
        "redis://:playback-fixture-only@localhost:6379/0",
        BusConfig {
            command_timeout: Duration::from_millis(50),
            ..BusConfig::default()
        },
    )
    .await
    .unwrap()
}
async fn wait(test: impl Fn() -> bool) {
    let until = tokio::time::Instant::now() + Duration::from_secs(3);
    while !test() {
        assert!(tokio::time::Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
async fn publish(jobs: &Jobs) -> Message {
    let m = Message {
        task_id: Uuid::new_v4(),
        kind: Kind::InactiveAccountNotifications,
        expires: None,
    };
    jobs.publish_default(&m, Arc::new(())).await.unwrap();
    m
}
struct Gate {
    bus: RealtimeBus,
    entered: Notify,
    release: Semaphore,
}
impl UpdateEventSink for Gate {
    fn send<'a>(
        &'a self,
        group: &'a str,
        message: Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        self.bus.send(group, message)
    }
    fn send_retained<'a>(
        &'a self,
        group: &'a str,
        message: Value,
        guard: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            if message["data"]["type"] != "system_notification" {
                return self.bus.send_retained(group, message, guard).await;
            }
            self.entered.notify_one();
            let _permit = self
                .release
                .acquire()
                .await
                .map_err(|_| UpdateEventError::Unavailable)?;
            self.bus.send_retained(group, message, guard).await
        })
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn actual_sql_preflight_cancellation_retains_owner() {
    let pool = pool().await;
    reset(&pool).await;
    insert(&pool, "xc-exp-warning-owned-resource").await;
    let jobs = jobs().await;
    let m = publish(&jobs).await;
    let worker = DefaultWorker::new(
        jobs.clone(),
        Arc::new(KnownDomains::with_active_notifications(
            pool.clone(),
            Arc::new(bus().await),
        )),
    )
    .unwrap();
    let mut lock = sqlx::PgConnection::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    let mut tx = lock.begin().await.unwrap();
    sqlx::query("LOCK TABLE core_systemnotification IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await
        .unwrap();
    let (held, flag) = owner();
    let own = worker.clone();
    let task = tokio::spawn(async move { own.run_once(held).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    task.abort();
    assert!(!worker.shutdown(Duration::from_millis(25)).await);
    assert!(!flag.load(Ordering::SeqCst));
    tx.rollback().await.unwrap();
    lock.close().await.unwrap();
    assert!(worker.shutdown(Duration::from_secs(3)).await);
    wait(|| flag.load(Ordering::SeqCst)).await;
    assert!(jobs.read(m.task_id, Arc::new(())).await.unwrap().is_some());
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    pool.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn actual_channels_late_reply_retains_committed_task() {
    let pool = pool().await;
    reset(&pool).await;
    insert(&pool, "xc-exp-warning-owned-event").await;
    let gate = Arc::new(Gate {
        bus: bus().await,
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let jobs = jobs().await;
    let m = publish(&jobs).await;
    let worker = DefaultWorker::new(
        jobs.clone(),
        Arc::new(KnownDomains::with_active_notifications(
            pool.clone(),
            gate.clone(),
        )),
    )
    .unwrap();
    let (held, flag) = owner();
    let own = worker.clone();
    let mut task = tokio::spawn(async move { own.run_once(held).await });
    tokio::select! {
        _=gate.entered.notified()=>{},
        outcome=&mut task=>panic!("Worker returned before held update event: {outcome:?}"),
        _=tokio::time::sleep(Duration::from_secs(5))=>panic!("No update event within owned deadline"),
    };
    let count:i64=sqlx::query_scalar("SELECT count(*) FROM core_systemnotification WHERE notification_key='xc-exp-warning-owned-event'").fetch_one(&pool).await.unwrap();
    assert_eq!(count, 1); // final stale collector has not run while update event is held.
    let active:i64=sqlx::query_scalar("SELECT count(*) FROM core_systemnotification WHERE title='Account Expiring: Owned Resource Profile'").fetch_one(&pool).await.unwrap();
    assert_eq!(active, 1);
    let mut c = redis().await;
    let _: String = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(500)
        .query_async(&mut c)
        .await
        .unwrap();
    gate.release.add_permits(1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    task.abort();
    assert!(!worker.shutdown(Duration::from_millis(25)).await);
    assert!(!flag.load(Ordering::SeqCst));
    assert!(worker.shutdown(Duration::from_secs(3)).await);
    wait(|| flag.load(Ordering::SeqCst)).await;
    assert!(jobs.read(m.task_id, Arc::new(())).await.unwrap().is_some());
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    pool.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn bounded_rows_refuse_before_ack_and_default_authority_isolated() {
    let pool = pool().await;
    reset(&pool).await;
    for n in 0..129 {
        insert(&pool, &format!("xc-exp-warning-owned-cap-{n}")).await;
    }
    let jobs = jobs().await;
    let _m = publish(&jobs).await;
    let worker = DefaultWorker::new(
        jobs.clone(),
        Arc::new(KnownDomains::with_active_notifications(
            pool.clone(),
            Arc::new(bus().await),
        )),
    )
    .unwrap();
    assert!(matches!(
        worker.run_once(Arc::new(())).await.unwrap(),
        Run::Unsupported(Kind::InactiveAccountNotifications)
    ));
    let mut c = redis().await;
    let len: i64 = redis::cmd("LLEN")
        .arg("celery")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(len, 1);
    assert!(matches!(
        jobs.publish(
            &crate::protocol::Task::CreateBackup,
            Uuid::new_v4(),
            Arc::new(())
        )
        .await,
        Err(Error::UnsupportedTask)
    ));
    assert!(matches!(
        jobs.publish_index(
            &crate::index_protocol::IndexTask::Initial { source_id: 1 },
            Uuid::new_v4(),
            Arc::new(())
        )
        .await,
        Err(Error::UnsupportedTask)
    ));
    assert!(worker.shutdown(Duration::from_secs(3)).await);
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    reset(&pool).await;
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn actual_collector_error_rolls_back_children_before_owner_release() {
    let pool = pool().await;
    reset(&pool).await;
    insert(&pool, "xc-exp-warning-owned-rollback").await;
    sqlx::query("INSERT INTO core_notificationdismissal(user_id,notification_id,dismissed_at,action_taken) SELECT (SELECT id FROM accounts_user ORDER BY id LIMIT 1),id,now(),NULL FROM core_systemnotification WHERE notification_key='xc-exp-warning-owned-rollback'").execute(&pool).await.unwrap();
    sqlx::query("CREATE FUNCTION rust_inactive_resource_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(691149); RAISE EXCEPTION 'owned inactive rollback' USING ERRCODE='P0001'; END $$").execute(&pool).await.unwrap();
    sqlx::query("CREATE TRIGGER rust_inactive_resource_fail BEFORE DELETE ON core_systemnotification FOR EACH ROW EXECUTE FUNCTION rust_inactive_resource_fail()").execute(&pool).await.unwrap();
    let mut control = sqlx::PgConnection::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_lock(691149)")
        .execute(&mut control)
        .await
        .unwrap();
    let jobs = jobs().await;
    let m = publish(&jobs).await;
    let worker = DefaultWorker::new(
        jobs.clone(),
        Arc::new(KnownDomains::with_active_notifications(
            pool.clone(),
            Arc::new(bus().await),
        )),
    )
    .unwrap();
    let (held, flag) = owner();
    let own = worker.clone();
    let task = tokio::spawn(async move { own.run_once(held).await });
    let until = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let waiting:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory'").fetch_one(&mut control).await.unwrap();
        if waiting > 0 {
            break;
        }
        assert!(tokio::time::Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    task.abort();
    assert!(!worker.shutdown(Duration::from_millis(25)).await);
    assert!(!flag.load(Ordering::SeqCst));
    sqlx::query("SELECT pg_advisory_unlock(691149)")
        .execute(&mut control)
        .await
        .unwrap();
    assert!(worker.shutdown(Duration::from_secs(3)).await);
    wait(|| flag.load(Ordering::SeqCst)).await;
    let notices:i64=sqlx::query_scalar("SELECT count(*) FROM core_systemnotification WHERE notification_key='xc-exp-warning-owned-rollback'").fetch_one(&pool).await.unwrap();
    let dismissals:i64=sqlx::query_scalar("SELECT count(*) FROM core_notificationdismissal WHERE notification_id IN (SELECT id FROM core_systemnotification WHERE notification_key='xc-exp-warning-owned-rollback')").fetch_one(&pool).await.unwrap();
    assert_eq!((notices, dismissals), (1, 1));
    let written:i64=sqlx::query_scalar("SELECT count(*) FROM core_systemnotification WHERE title='Account Expiring: Owned Resource Profile'").fetch_one(&pool).await.unwrap();
    assert_eq!(written, 1);
    assert!(jobs.read(m.task_id, Arc::new(())).await.unwrap().is_none());
    sqlx::query("DROP TRIGGER rust_inactive_resource_fail ON core_systemnotification")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION rust_inactive_resource_fail()")
        .execute(&pool)
        .await
        .unwrap();
    control.close().await.unwrap();
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    reset(&pool).await;
    pool.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn actual_upsert_reply_cancel_retains_domain_and_partial_commit() {
    let pool = pool().await;
    reset(&pool).await;
    let profile: i64 =
        sqlx::query_scalar("SELECT id::bigint FROM m3u_m3uaccountprofile WHERE is_active")
            .fetch_one(&pool)
            .await
            .unwrap();
    let warning = format!("xc-exp-warning-{profile}");
    let expired = format!("xc-exp-expired-{profile}");
    insert(&pool, &warning).await;
    insert(&pool, &expired).await;
    sqlx::query("CREATE FUNCTION rust_active_resource_hold() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(691150); RETURN NEW; END $$").execute(&pool).await.unwrap();
    sqlx::query("CREATE TRIGGER rust_active_resource_hold BEFORE UPDATE ON core_systemnotification FOR EACH ROW EXECUTE FUNCTION rust_active_resource_hold()").execute(&pool).await.unwrap();
    let mut control = sqlx::PgConnection::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_lock(691150)")
        .execute(&mut control)
        .await
        .unwrap();
    let jobs = jobs().await;
    let m = publish(&jobs).await;
    let worker = DefaultWorker::new(
        jobs.clone(),
        Arc::new(KnownDomains::with_active_notifications(
            pool.clone(),
            Arc::new(bus().await),
        )),
    )
    .unwrap();
    let (held, flag) = owner();
    let own = worker.clone();
    let task = tokio::spawn(async move { own.run_once(held).await });
    let until = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let waiting:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory'").fetch_one(&mut control).await.unwrap();
        if waiting > 0 {
            break;
        }
        assert!(tokio::time::Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let gone: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core_systemnotification WHERE notification_key=$1",
    )
    .bind(&expired)
    .fetch_one(&mut control)
    .await
    .unwrap();
    assert_eq!(gone, 0);
    task.abort();
    assert!(!worker.shutdown(Duration::from_millis(25)).await);
    assert!(!flag.load(Ordering::SeqCst));
    assert!(jobs.read(m.task_id, Arc::new(())).await.unwrap().is_none());
    sqlx::query("SELECT pg_advisory_unlock(691150)")
        .execute(&mut control)
        .await
        .unwrap();
    assert!(worker.shutdown(Duration::from_secs(3)).await);
    wait(|| flag.load(Ordering::SeqCst)).await;
    assert!(jobs.read(m.task_id, Arc::new(())).await.unwrap().is_some());
    sqlx::query("DROP TRIGGER rust_active_resource_hold ON core_systemnotification")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION rust_active_resource_hold()")
        .execute(&pool)
        .await
        .unwrap();
    control.close().await.unwrap();
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    reset(&pool).await;
    pool.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn profile_cap_and_legacy_constructor_refuse_before_ack() {
    let pool = pool().await;
    reset(&pool).await;
    let jobs = jobs().await;
    let m = publish(&jobs).await;
    let legacy = DefaultWorker::new(
        jobs.clone(),
        Arc::new(KnownDomains::with_notifications(
            pool.clone(),
            Arc::new(bus().await),
        )),
    )
    .unwrap();
    assert!(matches!(
        legacy.run_once(Arc::new(())).await.unwrap(),
        Run::Unsupported(Kind::InactiveAccountNotifications)
    ));
    assert!(legacy.shutdown(Duration::from_secs(1)).await);
    sqlx::query("INSERT INTO m3u_m3uaccountprofile(m3u_account_id,name,is_active,exp_date,max_streams,current_viewers,search_pattern,replace_pattern,custom_properties,is_default) SELECT p.m3u_account_id,'Owned Extra Profile-'||n::text,true,now(),0,0,'','','{}',false FROM m3u_m3uaccountprofile p CROSS JOIN generate_series(1,32) n WHERE p.is_active").execute(&pool).await.unwrap();
    let worker = DefaultWorker::new(
        jobs.clone(),
        Arc::new(KnownDomains::with_active_notifications(
            pool.clone(),
            Arc::new(bus().await),
        )),
    )
    .unwrap();
    assert!(matches!(
        worker.run_once(Arc::new(())).await.unwrap(),
        Run::Unsupported(Kind::InactiveAccountNotifications)
    ));
    let mut c = redis().await;
    let len: i64 = redis::cmd("LLEN")
        .arg("celery")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(len, 1);
    assert!(jobs.read(m.task_id, Arc::new(())).await.unwrap().is_none());
    assert!(worker.shutdown(Duration::from_secs(1)).await);
    jobs.shutdown(Duration::from_secs(1)).await.unwrap();
    reset(&pool).await;
    pool.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn four_domain_admissions_retain_waiting_sql_owners_after_abort() {
    use crate::default_domains::{Domains, Error as DomainError};
    let pool = pool().await;
    reset(&pool).await;
    let domains = Arc::new(KnownDomains::with_active_notifications(
        pool.clone(),
        Arc::new(bus().await),
    ));
    let mut lock = sqlx::PgConnection::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    let mut tx = lock.begin().await.unwrap();
    sqlx::query("LOCK TABLE core_systemnotification IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await
        .unwrap();
    let mut tasks = Vec::new();
    let mut flags = Vec::new();
    for _ in 0..4 {
        let own = domains.clone();
        let (held, flag) = owner();
        flags.push(flag);
        tasks.push(tokio::spawn(async move {
            own.preflight(Kind::InactiveAccountNotifications, held)
                .await
        }));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(matches!(
        domains
            .preflight(Kind::InactiveAccountNotifications, Arc::new(()))
            .await,
        Err(DomainError::Unsupported)
    ));
    for task in tasks {
        task.abort();
    }
    assert!(flags.iter().all(|v| !v.load(Ordering::SeqCst)));
    tx.rollback().await.unwrap();
    lock.close().await.unwrap();
    wait(|| flags.iter().all(|v| v.load(Ordering::SeqCst))).await;
    domains
        .preflight(Kind::InactiveAccountNotifications, Arc::new(()))
        .await
        .unwrap();
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn snapshot_sql_guard_cancellation_retains_owner() {
    let pool = pool().await;
    reset(&pool).await;
    insert(&pool, "xc-exp-warning-owned-resource").await;
    let jobs = jobs().await;
    let m = publish(&jobs).await;
    let worker = DefaultWorker::new(
        jobs.clone(),
        Arc::new(KnownDomains::with_active_notifications(
            pool.clone(),
            Arc::new(bus().await),
        )),
    )
    .unwrap();
    let mut lock = sqlx::PgConnection::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    let mut tx = lock.begin().await.unwrap();
    sqlx::query("LOCK TABLE m3u_m3uaccountprofile IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await
        .unwrap();
    let (held, flag) = owner();
    let own = worker.clone();
    let task = tokio::spawn(async move { own.run_once(held).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    task.abort();
    assert!(!worker.shutdown(Duration::from_millis(25)).await);
    assert!(!flag.load(Ordering::SeqCst));
    tx.rollback().await.unwrap();
    lock.close().await.unwrap();
    assert!(worker.shutdown(Duration::from_secs(3)).await);
    wait(|| flag.load(Ordering::SeqCst)).await;
    assert!(jobs.read(m.task_id, Arc::new(())).await.unwrap().is_some());
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    pool.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn fetched_profile_bytes_refuse_before_ack_without_mutation() {
    let pool = pool().await;
    reset(&pool).await;
    sqlx::query("UPDATE m3u_m3uaccountprofile SET custom_properties=jsonb_build_object('owned',repeat('x',70000)) WHERE is_active").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO m3u_m3uaccountprofile(m3u_account_id,name,is_active,exp_date,max_streams,current_viewers,search_pattern,replace_pattern,custom_properties,is_default) SELECT m3u_account_id,'Owned Byte Boundary Extra',true,now(),0,0,'','','{}',false FROM m3u_m3uaccountprofile WHERE is_active").execute(&pool).await.unwrap();
    let jobs = jobs().await;
    let m = publish(&jobs).await;
    let worker = DefaultWorker::new(
        jobs.clone(),
        Arc::new(KnownDomains::with_active_notifications(
            pool.clone(),
            Arc::new(bus().await),
        )),
    )
    .unwrap();
    assert!(matches!(
        worker.run_once(Arc::new(())).await.unwrap(),
        Run::Unsupported(Kind::InactiveAccountNotifications)
    ));
    let mut c = redis().await;
    let len: i64 = redis::cmd("LLEN")
        .arg("celery")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(len, 1);
    assert!(jobs.read(m.task_id, Arc::new(())).await.unwrap().is_none());
    let retained:i64=sqlx::query_scalar("SELECT count(*) FROM m3u_m3uaccountprofile WHERE is_active AND length(custom_properties->>'owned')=70000").fetch_one(&pool).await.unwrap();
    assert_eq!(retained, 1);
    assert!(worker.shutdown(Duration::from_secs(1)).await);
    jobs.shutdown(Duration::from_secs(1)).await.unwrap();
    sqlx::query("UPDATE m3u_m3uaccountprofile SET custom_properties='{}' WHERE is_active")
        .execute(&pool)
        .await
        .unwrap();
    reset(&pool).await;
    pool.close().await;
}
