//! Copied-DB Source producer/result integration peer, never production startup.
use super::*;
use dispatcharr_coordination::TaskLockConfig;
use dispatcharr_store::Store;
use serde_json::{json, Value};
use std::io::{self, Read};
#[tokio::test]
#[ignore = "Actual copied DB/Original Series producer+AsyncResult, no-recording subset"]
async fn native_series_worker_peer() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let database = std::env::var("POSTGRES_DB").unwrap();
    assert!(
        database.starts_with("rust_probe_backup_jobs_")
            && database.len() <= 63
            && database
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
    );
    let mut raw = String::new();
    io::stdin().take(16385).read_to_string(&mut raw).unwrap();
    assert!(raw.len() <= 16384);
    let input: Value = serde_json::from_str(&raw).unwrap();
    let db = format!("postgres://dispatch:playback-fixture-only@127.0.0.1:5432/{database}");
    assert_eq!(input["database_url"], db);
    let cache = "redis://:playback-fixture-only@127.0.0.1:6379/0";
    let jobs = Jobs::connect_series(
        input["broker_url"].as_str().unwrap(),
        input["result_url"].as_str().unwrap(),
        crate::transport::Config {
            operations: 2,
            command_timeout: Duration::from_secs(2),
            max_queued_jobs: 64,
        },
    )
    .await
    .unwrap();
    assert!(jobs.copied_database().is_none());
    assert!(matches!(
        jobs.publish(
            &crate::protocol::Task::CreateBackup,
            Uuid::new_v4(),
            Arc::new(())
        )
        .await,
        Err(TransportError::UnsupportedTask)
    ));
    assert!(matches!(
        jobs.publish_index(
            &crate::index_protocol::IndexTask::Initial { source_id: 1 },
            Uuid::new_v4(),
            Arc::new(())
        )
        .await,
        Err(TransportError::UnsupportedTask)
    ));
    let refresh = crate::refresh_publish::message(
        Uuid::new_v4(),
        crate::refresh_protocol::RefreshCall::initial(1),
    );
    assert!(matches!(
        jobs.publish_refresh(&refresh, Arc::new(())).await,
        Err(TransportError::UnsupportedTask)
    ));
    let store = Store::connect(&db, 1).await.unwrap();
    let locks = TaskLocks::connect(cache, TaskLockConfig::default())
        .await
        .unwrap();
    let policy = Arc::new(CachedSwapPolicy::new(store.clone(), Some(cache.into())));
    let worker = SeriesWorker::new(jobs.clone(), locks.clone(), policy.clone()).unwrap();
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let held_sql = input["held_sql"] == true;
    let prepared = input["prepared"] == true;
    let outcome = if prepared {
        let service = crate::series_application::SeriesApplication::prepare(
            store.clone(),
            crate::series_application::Options {
                broker: input["broker_url"].as_str().unwrap().into(),
                backend: input["result_url"].as_str().unwrap().into(),
                cache: cache.into(),
                poll_interval: Duration::from_millis(25),
            },
        )
        .await
        .unwrap();
        assert!(service.state().is_none());
        let mut broker = redis::Client::open(input["broker_url"].as_str().unwrap())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let before: Vec<u8> = redis::cmd("LINDEX")
            .arg("celery")
            .arg(-1)
            .query_async(&mut broker)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let after: Vec<u8> = redis::cmd("LINDEX")
            .arg("celery")
            .arg(-1)
            .query_async(&mut broker)
            .await
            .unwrap();
        assert_eq!(before, after);
        assert!(service.state().is_none());
        service.start().unwrap();
        assert_eq!(service.start(), Err(Error::Configuration));
        let mut backend = redis::Client::open(input["result_url"].as_str().unwrap())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let key = format!(
            "celery-tasks:celery-task-meta-{}",
            input["task_id"].as_str().unwrap()
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let exists: bool = redis::cmd("EXISTS")
                    .arg(&key)
                    .query_async(&mut backend)
                    .await
                    .unwrap();
                if exists {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        service.shutdown(Duration::from_secs(3)).await.unwrap();
        drop(caller);
        None
    } else if held_sql {
        let control = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&db)
            .await
            .unwrap();
        let mut transaction = control.begin().await.unwrap();
        sqlx::query("LOCK TABLE core_coresettings IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *transaction)
            .await
            .unwrap();
        let owned = worker.clone();
        let waiter = tokio::spawn(async move { owned.run_once(caller).await });
        tokio::time::timeout(Duration::from_secs(3),async {
            loop {
                sqlx::query("SELECT pg_stat_clear_snapshot()").execute(&mut *transaction).await.unwrap();
                let waiting:i64=sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%FROM core_coresettings WHERE key=$1%'").fetch_one(&mut *transaction).await.unwrap();
                if waiting==1 {break}
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.unwrap();
        assert!(weak.upgrade().is_some());
        assert_eq!(worker.available_slots(), 0);
        assert_eq!(policy.physical_operations(), 1);
        assert_eq!(locks.available_leases(), 3);
        waiter.abort();
        let _ = waiter.await;
        assert_eq!(
            worker.shutdown(Duration::from_millis(20)).await,
            Err(Error::DrainTimeout)
        );
        assert!(weak.upgrade().is_some());
        transaction.rollback().await.unwrap();
        control.close().await;
        worker.shutdown(Duration::from_secs(3)).await.unwrap();
        None
    } else {
        Some(worker.run_once(caller).await)
    };

    match input["expected"].as_str().unwrap() {
        "unsupported-rules" => assert_eq!(outcome, Some(Err(Error::UnsupportedRules))),
        "foreign" => assert_eq!(
            outcome,
            Some(Err(Error::Transport(TransportError::UnsupportedTask)))
        ),
        "completed" => {
            if held_sql || prepared {
                assert_eq!(outcome, None);
            } else {
                assert_eq!(
                    outcome,
                    Some(Ok(Run::Completed(
                        Uuid::parse_str(input["task_id"].as_str().unwrap()).unwrap()
                    )))
                );
            }
        }
        _ => panic!("Known fixture expectation required"),
    }
    worker.shutdown(Duration::from_secs(3)).await.unwrap();
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    assert_eq!(locks.available_leases(), 4);
    assert_eq!(policy.physical_operations(), 0);
    assert!(weak.upgrade().is_none());
    store.pool().close().await;
    println!(
        "NATIVE_SERIES_WORKER_JSON={}",
        json!({"outcome":format!("{outcome:?}"),"caller_released":true,"physical_operations":0,"family_drained":true,"transport_drained":true,"held_actual_SQL":held_sql,"prepared_no_intake_then_started":prepared,"caller_aborted":held_sql,"aborted_waiter_return_observed":false})
    );
}
