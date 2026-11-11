//! Disposable actual-SQL ownership/reply-budget and retained timer controls.
//! Original Dispatcharr/framework authors retain credit and licenses.
use super::*;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use std::{str::FromStr, sync::atomic::AtomicUsize};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
fn fixture() -> String {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let db = std::env::var("POSTGRES_DB").unwrap();
    assert!(
        db.starts_with("rust_probe_backup_jobs_")
            && db.len() <= 63
            && db
                .bytes()
                .all(|v| v.is_ascii_lowercase() || v.is_ascii_digit() || v == b'_')
    );
    let url = std::env::var("SCHEDULER_DATABASE_URL").unwrap();
    assert!(url.ends_with(&format!("/{db}")));
    url
}
async fn until(mut condition: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "Physical resource wait"
        );
        tokio::time::sleep(Duration::from_millis(5)).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Requires guarded copied PostgreSQL fixture"]
async fn cancelled_setup_retains_two_admissions_and_rollback_until_actual_sql_settles() {
    let url = fixture();
    let blocker_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .unwrap();
    let options = PgConnectOptions::from_str(&url)
        .unwrap()
        .application_name("rust-owned-scheduler-physical");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    let service = DatabaseSchedule::new(pool.clone());
    service.setup(Arc::new(())).await.unwrap();
    sqlx::query("UPDATE django_celery_beat_periodictask SET task='owned.kept.after.failure' WHERE name='check-account-expirations'").execute(&blocker_pool).await.unwrap();
    sqlx::query("CREATE FUNCTION rust_owned_scheduler_physical_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.name='check-account-expirations' THEN RAISE EXCEPTION 'owned physical scheduler failure'; END IF; RETURN NEW; END $$").execute(&blocker_pool).await.unwrap();
    sqlx::query("CREATE TRIGGER rust_owned_scheduler_physical_failure BEFORE INSERT OR UPDATE ON django_celery_beat_periodictask FOR EACH ROW EXECUTE FUNCTION rust_owned_scheduler_physical_fail()").execute(&blocker_pool).await.unwrap();
    let mut blocker = blocker_pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE django_celery_beat_periodictask IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let first = Arc::new(());
    let first_weak = Arc::downgrade(&first);
    let s = service.clone();
    let task = tokio::spawn(async move { s.setup(first).await });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let waiting:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name='rust-owned-scheduler-physical' AND wait_event_type='Lock'").fetch_one(&blocker_pool).await.unwrap();
        if waiting > 0 {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let second = Arc::new(());
    let second_weak = Arc::downgrade(&second);
    let s = service.clone();
    let other = tokio::spawn(async move { s.setup(second).await });
    until(|| service.inner.slots.available_permits() == 0).await;
    assert!(matches!(
        service.setup(Arc::new(())).await,
        Err(Error::Capacity)
    ));
    task.abort();
    other.abort();
    let _ = task.await;
    let _ = other.await;
    assert!(first_weak.upgrade().is_some() && second_weak.upgrade().is_some());
    assert!(!service.shutdown(Duration::from_millis(30)).await);
    assert!(matches!(
        service.refresh(Utc::now(), Arc::new(())).await,
        Err(Error::Closed)
    ));
    blocker.commit().await.unwrap();
    assert!(service.shutdown(Duration::from_secs(5)).await);
    until(|| first_weak.upgrade().is_none() && second_weak.upgrade().is_none()).await;
    let task: String = sqlx::query_scalar(
        "SELECT task FROM django_celery_beat_periodictask WHERE name='check-account-expirations'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(task, "owned.kept.after.failure");
    let healthy: i32 = sqlx::query_scalar("SELECT 1::integer")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(healthy, 1);
    sqlx::query(
        "DROP TRIGGER rust_owned_scheduler_physical_failure ON django_celery_beat_periodictask",
    )
    .execute(&blocker_pool)
    .await
    .unwrap();
    sqlx::query("DROP FUNCTION rust_owned_scheduler_physical_fail()")
        .execute(&blocker_pool)
        .await
        .unwrap();
    pool.close().await;
    blocker_pool.close().await;
}
async fn pump<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    mut reader: R,
    mut writer: W,
    count: Arc<AtomicUsize>,
) {
    let mut buf = [0u8; 4096];
    while let Ok(read) = reader.read(&mut buf).await {
        if read == 0 {
            break;
        }
        count.fetch_add(read, Ordering::AcqRel);
        if writer.write_all(&buf[..read]).await.is_err() {
            break;
        }
    }
    let _ = writer.shutdown().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Requires guarded copied PostgreSQL fixture"]
async fn oversized_fields_aggregate_and_count_return_only_bounded_wire_sentinel() {
    let url = fixture();
    let direct = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query("UPDATE django_celery_beat_periodictask SET enabled=false")
        .execute(&direct)
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let bytes = Arc::new(AtomicUsize::new(0));
    let reply_bytes = bytes.clone();
    let (stop_proxy, stop_requested) = tokio::sync::oneshot::channel::<()>();
    let forwarding = tokio::spawn(async move {
        let (client, _) = listener.accept().await.unwrap();
        let upstream = TcpStream::connect("127.0.0.1:5432").await.unwrap();
        let (cr, cw) = client.into_split();
        let (sr, sw) = upstream.into_split();
        let mut left = tokio::spawn(pump(cr, sw, Arc::new(AtomicUsize::new(0))));
        let mut right = tokio::spawn(pump(sr, cw, reply_bytes));
        // Once either peer closes, discard the fixture proxy's remaining
        // half-open socket and await its actual task drop. No workload command
        // is outstanding: the caller already observed every query completion.
        tokio::select! {
            _ = &mut left => { right.abort(); let _ = right.await; },
            _ = &mut right => { left.abort(); let _ = left.await; },
            _ = stop_requested => { left.abort(); right.abort(); let _ = tokio::join!(left,right); },
        }
    });
    let options = PgConnectOptions::from_str(&url)
        .unwrap()
        .host("127.0.0.1")
        .port(address.port())
        .ssl_mode(PgSslMode::Disable);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    let service = DatabaseSchedule::new(pool.clone());
    for (kind, count, arg) in [
        ("field", 1, "x".repeat(65537)),
        ("aggregate", 32, format!("\"{}\"", "x".repeat(40000))),
        ("count", 257, "[]".to_owned()),
    ] {
        sqlx::query(
            "DELETE FROM django_celery_beat_periodictask WHERE name LIKE 'rust-owned-wire-%'",
        )
        .execute(&direct)
        .await
        .unwrap();
        sqlx::query("INSERT INTO django_celery_beat_periodictask(name,task,args,kwargs,headers,enabled,total_run_count,date_changed,description,one_off,interval_id) SELECT 'rust-owned-wire-'||$1||'-'||n,'apps.epg.tasks.refresh_epg_data',$2,'{}','{}',true,0,now(),'',false,(SELECT id FROM django_celery_beat_intervalschedule ORDER BY id LIMIT 1) FROM generate_series(1,$3::integer) n").bind(kind).bind(&arg).bind(count).execute(&direct).await.unwrap();
        let before = bytes.load(Ordering::Acquire);
        assert!(matches!(
            service.refresh(Utc::now(), Arc::new(())).await,
            Err(Error::Capacity)
        ));
        let received = bytes.load(Ordering::Acquire) - before;
        assert!(
            received < 8192,
            "Oversized data crossed actual PostgreSQL wire: {kind}: {received}"
        );
        println!(
            "SCHEDULER_BUDGET_WIRE_JSON={}",
            serde_json::json!({"case":kind,"server_reply_bytes":received,"row_count":count,"argument_bytes":arg.len()})
        );
    }
    sqlx::query("DELETE FROM django_celery_beat_periodictask WHERE name LIKE 'rust-owned-wire-%'")
        .execute(&direct)
        .await
        .unwrap();
    let result = service.refresh(Utc::now(), Arc::new(())).await.unwrap();
    assert!(result.is_empty());
    assert!(service.shutdown(Duration::from_secs(2)).await);
    pool.close().await;
    // Explicitly stop the fixture proxy only after all SQL and pool shutdown
    // futures completed. Natural half-close timing is not a workload assertion.
    let _ = stop_proxy.send(());
    tokio::time::timeout(Duration::from_secs(2), forwarding)
        .await
        .unwrap()
        .unwrap();
    direct.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Requires guarded copied PG plus isolated Redis broker8/result9"]
async fn tick_deadlines_and_aborted_callers_retain_job_admission_until_real_publish_and_final_sync()
{
    let url = fixture();
    let broker = std::env::var("CELERY_BROKER_URL").unwrap();
    let backend = std::env::var("CELERY_RESULT_BACKEND").unwrap();
    for (raw, db) in [(&broker, 8), (&backend, 9)] {
        let client = redis::Client::open(raw.as_str()).unwrap();
        let info = client.get_connection_info();
        assert_eq!(info.redis.db, db);
        assert!(
            matches!(&info.addr,redis::ConnectionAddr::Tcp(host,port)if matches!(host.as_str(),"127.0.0.1"|"localhost"|"::1")&&*port==6379)
        );
        assert_eq!(
            info.redis.password.as_deref(),
            Some("playback-fixture-only")
        );
    }
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query("UPDATE django_celery_beat_periodictask SET enabled=false")
        .execute(&pool)
        .await
        .unwrap();
    let interval:i64=sqlx::query_scalar("INSERT INTO django_celery_beat_intervalschedule(every,period) VALUES(0,'seconds') RETURNING id::bigint").fetch_one(&pool).await.unwrap();
    sqlx::query("INSERT INTO django_celery_beat_periodictask(name,task,args,kwargs,headers,enabled,total_run_count,date_changed,description,one_off,interval_id,last_run_at) VALUES('rust-owned-held-tick','apps.epg.tasks.refresh_epg_data','[1]','{\"force\":true}','{}',true,0,now(),'',false,$1,now()-interval '1 hour')").bind(interval).execute(&pool).await.unwrap();
    let config = crate::transport::Config {
        operations: 2,
        command_timeout: Duration::from_millis(50),
        max_queued_jobs: 64,
    };
    let publisher = Arc::new(tick::KnownPublisher {
        refresh: crate::transport::Jobs::connect_refresh(&broker, &backend, config.clone())
            .await
            .unwrap(),
        series: crate::transport::Jobs::connect_series(&broker, &backend, config)
            .await
            .unwrap(),
    });
    let service = DatabaseSchedule::new(pool.clone());
    let client = redis::Client::open(broker.as_str()).unwrap();
    let mut control = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB")
        .query_async(&mut control)
        .await
        .unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(1500)
        .arg("WRITE")
        .query_async(&mut control)
        .await
        .unwrap();
    let first = Arc::new(());
    let first_weak = Arc::downgrade(&first);
    let s = service.clone();
    let p = publisher.clone();
    let waiter = tokio::spawn(async move { s.tick(Utc::now(), p, first).await });
    until(|| service.inner.slots.available_permits() == 1).await;
    let second = Arc::new(());
    let second_weak = Arc::downgrade(&second);
    let s = service.clone();
    let p = publisher.clone();
    let other = tokio::spawn(async move { s.tick(Utc::now(), p, second).await });
    until(|| service.inner.slots.available_permits() == 0).await;
    waiter.abort();
    let _ = waiter.await;
    let logical = tokio::time::timeout(Duration::from_millis(500), other)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(logical.publication, Some(Err(_))));
    assert!(first_weak.upgrade().is_some() && second_weak.upgrade().is_some());
    assert_eq!(service.inner.slots.available_permits(), 0);
    assert!(matches!(
        service
            .tick(Utc::now(), publisher.clone(), Arc::new(()))
            .await,
        Err(Error::Capacity)
    ));
    assert!(!service.shutdown(Duration::from_millis(30)).await);
    assert!(service.shutdown(Duration::from_secs(5)).await);
    until(|| first_weak.upgrade().is_none() && second_weak.upgrade().is_none()).await;
    let count:i64=sqlx::query_scalar("SELECT total_run_count::bigint FROM django_celery_beat_periodictask WHERE name='rust-owned-held-tick'").fetch_one(&pool).await.unwrap();
    assert_eq!(count, 2);
    let queued: i64 = redis::cmd("LLEN")
        .arg("celery")
        .query_async(&mut control)
        .await
        .unwrap();
    assert_eq!(queued, 2);
    println!(
        "SCHEDULER_RETAINED_TICK_JSON={}",
        serde_json::json!({"logical_return_before_actual_publication":true,"actual_publications":queued,"persisted_after_shutdown":count,"callers_released_after_actual_reply":true})
    );
    publisher
        .refresh
        .shutdown(Duration::from_secs(2))
        .await
        .unwrap();
    publisher
        .series
        .shutdown(Duration::from_secs(2))
        .await
        .unwrap();
    let _: () = redis::cmd("FLUSHDB")
        .query_async(&mut control)
        .await
        .unwrap();
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Requires guarded copied PostgreSQL fixture"]
async fn driver_shutdown_deadline_and_cancelled_wait_retain_actual_sql_tick() {
    struct NoPublish(Arc<std::sync::atomic::AtomicBool>);
    impl tick::PeriodicPublisher for NoPublish {
        fn publish<'a>(
            &'a self,
            _: tick::KnownTask,
            _: &'a str,
            _: Arc<dyn Send + Sync>,
        ) -> tick::PublishFuture<'a> {
            self.0.store(true, Ordering::Release);
            Box::pin(async { Err(crate::transport::Error::UnsupportedTask) })
        }
    }
    let url = fixture();
    let blocker_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            PgConnectOptions::from_str(&url)
                .unwrap()
                .application_name("rust-owned-scheduler-driver-held"),
        )
        .await
        .unwrap();
    let service = DatabaseSchedule::new(pool.clone());
    service.setup(Arc::new(())).await.unwrap();
    // Setup populated the cache. Establish its marker before the fixture bulk
    // edit, then send the real Beat sentinel; bulk UPDATE alone is deliberately
    // invisible to the captured Source cache behavior.
    service.refresh(Utc::now(), Arc::new(())).await.unwrap();
    sqlx::query("UPDATE django_celery_beat_periodictask SET enabled=false")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO django_celery_beat_periodictasks(ident,last_update) VALUES(1,clock_timestamp()) ON CONFLICT(ident) DO UPDATE SET last_update=EXCLUDED.last_update").execute(&pool).await.unwrap();
    let mut lock = blocker_pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE django_celery_beat_periodictask IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let mut driver = driver::Driver::start(
        service.clone(),
        Arc::new(NoPublish(called.clone())),
        Duration::from_secs(1),
        caller,
    )
    .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let waiting: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name='rust-owned-scheduler-driver-held' AND wait_event_type='Lock'").fetch_one(&blocker_pool).await.unwrap();
        if waiting > 0 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            let rows = sqlx::query("SELECT application_name,state,COALESCE(wait_event_type,'') AS wait_type,COALESCE(wait_event,'') AS wait_event,left(query,160) AS query FROM pg_stat_activity WHERE datname=current_database() ORDER BY pid").fetch_all(&blocker_pool).await.unwrap();
            for row in rows {
                println!(
                    "SCHEDULER_DRIVER_WAIT_DIAGNOSTIC={}",
                    serde_json::json!({"application_name":row.get::<String,_>("application_name"),"state":row.get::<String,_>("state"),"wait_type":row.get::<String,_>("wait_type"),"wait_event":row.get::<String,_>("wait_event"),"query":row.get::<String,_>("query")})
                );
            }
            panic!("Expected actual held SQL tick before shutdown");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(tokio::time::timeout(
        Duration::from_millis(10),
        driver.shutdown(Duration::from_secs(5))
    )
    .await
    .is_err());
    assert!(weak.upgrade().is_some());
    assert!(!driver.shutdown(Duration::from_millis(30)).await.unwrap());
    assert_eq!(service.inner.slots.available_permits(), 1);
    lock.commit().await.unwrap();
    assert!(driver.shutdown(Duration::from_secs(5)).await.unwrap());
    until(|| weak.upgrade().is_none()).await;
    assert_eq!(service.inner.slots.available_permits(), 2);
    assert!(!called.load(Ordering::Acquire));
    println!("SCHEDULER_RETAINED_DRIVER_JSON={{\"cancelled_shutdown_preserved_join\":true,\"held_SQL_ownership\":true,\"single_deadline_incomplete\":true,\"no_publication_after_stop\":true}}");
    pool.close().await;
    blocker_pool.close().await;
}
