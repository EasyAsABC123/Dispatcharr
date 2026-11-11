//! Native physical-owner controls, not Original response/performance acceptance.
//! Only explicit copied PostgreSQL/Redis fixtures are authorized.
use super::*;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

fn guards() -> (String, String) {
    assert_eq!(
        std::env::var("VOD_API_NATIVE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    assert_eq!(
        std::env::var("VOD_FAIR_PHYSICAL_DISPOSABLE").as_deref(),
        Ok("playback-fixture-only")
    );
    let pg = std::env::var("DATABASE_URL").unwrap();
    let redis = std::env::var("REDIS_TEST_URL").unwrap();
    assert!(pg.contains("playback-fixture-only") && redis.contains("playback-fixture-only"));
    (pg, redis)
}
fn output(name: &str, value: serde_json::Value) {
    let out = std::path::PathBuf::from(std::env::var("VOD_NATIVE_EVIDENCE").unwrap());
    assert!(out.is_dir());
    std::fs::write(out.join(name), serde_json::to_vec_pretty(&value).unwrap()).unwrap();
}
fn body(work: &ReadWork) -> Result<Vec<u8>, ReadError> {
    let mut d = work.document();
    d.write_all(b"{}").map_err(|_| ReadError::Capacity)?;
    Ok(d.finish())
}
async fn zero(runtime: &ReadRuntime) {
    tokio::time::timeout(Duration::from_secs(6), async {
        while runtime.retained_memory_bytes() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "Requires explicit fresh owned PostgreSQL/Redis fixture; actual pending SQL reply"]
async fn actual_sql_deadline_abort_retains_phase_until_reply_and_cleanup() {
    let (pg, _) = guards();
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .after_connect(|conn, _| {
            Box::pin(async move {
                sqlx::query("SET statement_timeout='7s'")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&pg)
        .await
        .unwrap();
    let runtime = ReadRuntime::new_fair(ReadLimits {
        logical_deadline: Duration::from_millis(150),
        statement_deadline: Duration::from_secs(8),
        ..ReadLimits::default()
    })
    .unwrap();
    let own = runtime.clone();
    let sql = pool.clone();
    let pending = tokio::spawn(async move {
        own.run(10, move |work| async move {
            let mut connection = work.connection(&sql).await?;
            sqlx::query("SELECT pg_sleep(3.2) /* rust-vod-fair-physical-reply */")
                .persistent(false)
                .execute(connection.raw())
                .await?;
            connection.finish().await?;
            body(&work)
        })
        .await
    });
    tokio::time::timeout(Duration::from_secs(1),async{loop{let active:i64=sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND state='active' AND query LIKE 'SELECT pg_sleep(3.2) /* rust-vod-fair-physical-reply */%'").fetch_one(&pool).await.unwrap();if active==1{break}tokio::time::sleep(Duration::from_millis(5)).await}}).await.unwrap();
    assert!(matches!(pending.await.unwrap(), Err(ReadError::Deadline)));
    assert_eq!(runtime.physical_workers(), 1);
    let retained = runtime.retained_memory_bytes();
    assert_eq!(
        retained,
        ReadLimits::default().input_bytes
            + ReadLimits::default().work_bytes
            + ReadLimits::default().response_bytes
    );
    assert!(!runtime.shutdown(Duration::from_millis(20)).await);
    zero(&runtime).await;
    assert!(runtime.shutdown(Duration::from_secs(1)).await);
    let original: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(original, "7s");
    pool.close().await;
    output(
        "fair-actual-sql-owner.json",
        json!({"passed":true,"scope":"Native actual PG physical ownership only; no Source HTTP/performance claim","logical_deadline_ms":150,"actual_pg_sleep_seconds":3.2,"retained_bytes_after_deadline":retained,"shutdown_was_undrained_until_actual_reply":true,"pool_timeout_restored":original,"final_memory_bytes":runtime.retained_memory_bytes()}),
    );
}

#[tokio::test]
#[ignore = "Requires explicit fresh owned Redis; production NotificationCache logical timeout and late real reply"]
async fn actual_cache_late_reply_prevents_refresh_credit_loan() {
    let (_, url) = guards();
    let cache = crate::NotificationCache::connect(&url).await.unwrap();
    let mut control = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let key = format!("rust-vod-fair-physical:{}", uuid::Uuid::new_v4());
    redis::cmd("SET")
        .arg(&key)
        .arg(crate::encode_source_cache(&json!("owned-fixture")).unwrap())
        .arg("PX")
        .arg(15000)
        .query_async::<()>(&mut control)
        .await
        .unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(3200)
        .arg("ALL")
        .query_async::<()>(&mut control)
        .await
        .unwrap();
    let began = tokio::time::Instant::now();
    let runtime = ReadRuntime::new_fair(ReadLimits::default()).unwrap();
    let own = runtime.clone();
    let cache_task = cache.clone();
    let task_key = key.clone();
    let (logical, logical_seen) = oneshot::channel();
    let (header, header_seen) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let task = tokio::spawn(async move {
        own.run(10, move |work| async move {
            let snapshot = work.freeze_snapshot(vec![0; 1024])?;
            let scoped = cache_task.with_retained_scope(work.io_guard()?.erased());
            let result = scoped.get(&task_key).await;
            assert!(result.is_err());
            drop(result);
            drop(scoped);
            let _ = logical.send(began.elapsed().as_millis());
            let snapshot = work.admit_refresh(snapshot).await?;
            drop(work.consume_snapshot(snapshot)?);
            let transport = work.transport_guard()?;
            work.before_headers().await?;
            let _ = header.send(began.elapsed().as_millis());
            let _ = wait.await;
            work.resume_active().await?;
            drop(transport);
            body(&work)
        })
        .await
    });
    let logical_ms = logical_seen.await.unwrap();
    assert!((1800..3000).contains(&logical_ms));
    assert_eq!(cache.physical_operations(), 1);
    assert_eq!(runtime.physical_workers(), 1);
    let retained = runtime.retained_memory_bytes();
    assert_eq!(
        retained,
        ReadLimits::default().input_bytes
            + ReadLimits::default().work_bytes
            + ReadLimits::default().response_bytes
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(runtime.physical_workers(), 1);
    let header_ms = tokio::time::timeout(Duration::from_secs(4), header_seen)
        .await
        .unwrap()
        .unwrap();
    assert!(header_ms >= 3000);
    assert_eq!(cache.physical_operations(), 0);
    assert_eq!(runtime.physical_workers(), 0);
    assert_eq!(
        runtime.retained_memory_bytes(),
        ReadLimits::default().input_bytes + ReadLimits::default().work_bytes
    );
    let _ = release.send(());
    drop(task.await.unwrap().unwrap());
    assert!(runtime.shutdown(Duration::from_secs(1)).await);
    zero(&runtime).await;
    let deleted: i64 = redis::cmd("DEL")
        .arg(&key)
        .query_async(&mut control)
        .await
        .unwrap();
    assert_eq!(deleted, 1);
    output(
        "fair-actual-cache-quiescence.json",
        json!({"passed":true,"scope":"Production NotificationCache real Redis physical reply vs explicit phase loan; no provider HTTP/Source parity claim","redis_pause_ms":3200,"cache_logical_observer_ms":logical_ms,"credit_loan_after_ms":header_ms,"retained_before_actual_reply":retained,"owned_key_removed":true,"final_memory_bytes":runtime.retained_memory_bytes()}),
    );
}

#[tokio::test]
#[ignore = "Requires explicit fresh owned Redis; caller abort/shutdown after logical cache timeout"]
async fn actual_cache_caller_abort_and_shutdown_keep_late_reply_owner() {
    let (_, url) = guards();
    let cache = crate::NotificationCache::connect(&url).await.unwrap();
    let mut control = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let key = format!("rust-vod-fair-physical:{}", uuid::Uuid::new_v4());
    redis::cmd("SET")
        .arg(&key)
        .arg(crate::encode_source_cache(&json!("owned-fixture")).unwrap())
        .arg("PX")
        .arg(15000)
        .query_async::<()>(&mut control)
        .await
        .unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(3200)
        .arg("ALL")
        .query_async::<()>(&mut control)
        .await
        .unwrap();
    let runtime = ReadRuntime::new_fair(ReadLimits::default()).unwrap();
    let own = runtime.clone();
    let cache_task = cache.clone();
    let task_key = key.clone();
    let (logical, seen) = oneshot::channel();
    let task = tokio::spawn(async move {
        own.run(10, move |work| async move {
            let snapshot = work.freeze_snapshot(vec![0; 1024])?;
            let scoped = cache_task.with_retained_scope(work.io_guard()?.erased());
            assert!(scoped.get(&task_key).await.is_err());
            drop(scoped);
            let _ = logical.send(());
            let _snapshot = work.admit_refresh(snapshot).await?;
            body(&work)
        })
        .await
    });
    seen.await.unwrap();
    assert_eq!(cache.physical_operations(), 1);
    task.abort();
    let _ = task.await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(runtime.physical_workers(), 1);
    let retained = runtime.retained_memory_bytes();
    assert_eq!(
        retained,
        ReadLimits::default().input_bytes
            + ReadLimits::default().work_bytes
            + ReadLimits::default().response_bytes
    );
    assert!(!runtime.shutdown(Duration::from_millis(20)).await);
    zero(&runtime).await;
    assert_eq!(cache.physical_operations(), 0);
    assert!(runtime.shutdown(Duration::from_secs(1)).await);
    let deleted: i64 = redis::cmd("DEL")
        .arg(&key)
        .query_async(&mut control)
        .await
        .unwrap();
    assert_eq!(deleted, 1);
    output(
        "fair-actual-cache-abort.json",
        json!({"passed":true,"scope":"Native real Redis caller cancellation/shutdown physical owner; no Source HTTP/performance claim","redis_pause_ms":3200,"retained_after_abort":retained,"shutdown_was_undrained_until_actual_reply":true,"owned_key_removed":true,"final_memory_bytes":runtime.retained_memory_bytes()}),
    );
}

#[tokio::test]
#[ignore = "Requires explicit fresh owned Redis; production NotificationCache logical timeout and late real reply"]
async fn actual_cache_late_reply_prevents_fresh_epoch_header_credit_loan() {
    let (_, url) = guards();
    let cache = crate::NotificationCache::connect(&url).await.unwrap();
    let mut control = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let key = format!("rust-vod-fair-physical:{}", uuid::Uuid::new_v4());
    redis::cmd("SET")
        .arg(&key)
        .arg(crate::encode_source_cache(&json!("owned-fixture")).unwrap())
        .arg("PX")
        .arg(15000)
        .query_async::<()>(&mut control)
        .await
        .unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(3200)
        .arg("ALL")
        .query_async::<()>(&mut control)
        .await
        .unwrap();
    let began = tokio::time::Instant::now();
    let runtime = ReadRuntime::new_fair(ReadLimits::default()).unwrap();
    let own = runtime.clone();
    let cache_task = cache.clone();
    let task_key = key.clone();
    let (logical, logical_seen) = oneshot::channel();
    let (header, header_seen) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let task = tokio::spawn(async move {
        own.run(10, move |work| async move {
            let snapshot = work.freeze_snapshot(vec![0; 1024])?;
            let snapshot = work.admit_refresh(snapshot).await?;
            drop(work.consume_snapshot(snapshot)?);
            let scoped = cache_task.with_retained_scope(work.io_guard()?.erased());
            let result = scoped.get(&task_key).await;
            assert!(result.is_err());
            drop(result);
            drop(scoped);
            let _ = logical.send(began.elapsed().as_millis());
            let transport = work.transport_guard()?;
            work.before_headers().await?;
            let _ = header.send(began.elapsed().as_millis());
            let _ = wait.await;
            work.resume_active().await?;
            drop(transport);
            body(&work)
        })
        .await
    });
    let logical_ms = logical_seen.await.unwrap();
    assert!((1800..3000).contains(&logical_ms));
    assert_eq!(cache.physical_operations(), 1);
    assert_eq!(runtime.physical_workers(), 1);
    let retained = runtime.retained_memory_bytes();
    assert_eq!(
        retained,
        ReadLimits::default().input_bytes
            + ReadLimits::default().work_bytes
            + ReadLimits::default().response_bytes
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(runtime.physical_workers(), 1);
    let header_ms = tokio::time::timeout(Duration::from_secs(4), header_seen)
        .await
        .unwrap()
        .unwrap();
    assert!(header_ms >= 3000);
    assert_eq!(cache.physical_operations(), 0);
    assert_eq!(runtime.physical_workers(), 0);
    assert_eq!(
        runtime.retained_memory_bytes(),
        ReadLimits::default().input_bytes + ReadLimits::default().work_bytes
    );
    let _ = release.send(());
    drop(task.await.unwrap().unwrap());
    assert!(runtime.shutdown(Duration::from_secs(1)).await);
    zero(&runtime).await;
    let deleted: i64 = redis::cmd("DEL")
        .arg(&key)
        .query_async(&mut control)
        .await
        .unwrap();
    assert_eq!(deleted, 1);
    output(
        "fair-actual-cache-header-quiescence.json",
        json!({"passed":true,"scope":"Production NotificationCache actual late reply in fresh RefreshActive epoch blocks before_headers; no provider HTTP/Source parity claim","fresh_refresh_epoch":true,"snapshot_consumed_before_cache":true,"redis_pause_ms":3200,"cache_logical_observer_ms":logical_ms,"credit_loan_after_ms":header_ms,"retained_before_actual_reply":retained,"owned_key_removed":true,"final_memory_bytes":runtime.retained_memory_bytes()}),
    );
}
