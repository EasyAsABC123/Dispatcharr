//! Exact Source SeriesRules global suffix, no-renewer and ownership tests.
//! Original Dispatcharr authors retain application/algorithm credit.
use dispatcharr_coordination::{TaskLockConfig, TaskLockDetailedError, TaskLockError, TaskLocks};
use std::{sync::Arc, time::Duration};

#[tokio::test]
#[ignore = "Owned disposable Source fixture Redis global SeriesRules key only"]
async fn global_series_key_no_renewal_and_foreign_owner_fencing() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let database = std::env::var("POSTGRES_DB").unwrap();
    assert!(database.starts_with("rust_probe_backup_jobs_"));
    let url = std::env::var("JOB_REDIS_TEST_URL").unwrap();
    let client = redis::Client::open(url.as_str()).unwrap();
    let info = client.get_connection_info();
    assert_eq!(info.redis.db, 0);
    assert_eq!(
        info.redis.password.as_deref(),
        Some("playback-fixture-only")
    );
    assert!(
        matches!(&info.addr,redis::ConnectionAddr::Tcp(host,port) if matches!(host.as_str(),"127.0.0.1"|"localhost") && *port==6379)
    );
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let key = "task_lock_evaluate_series_rules_all";
    let _: i64 = redis::cmd("DEL")
        .arg(key)
        .query_async(&mut connection)
        .await
        .unwrap();
    let locks = TaskLocks::connect(
        &url,
        TaskLockConfig {
            ttl: Duration::from_secs(2),
            renewal_interval: Duration::from_millis(100),
            max_leases: 2,
            max_commands: 2,
        },
    )
    .await
    .unwrap();
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let lease = locks.acquire_series_rules_retained(caller).await.unwrap();
    assert_eq!(lease.key(), key);
    assert!(weak.upgrade().is_some());
    let busy = locks.acquire_series_rules_retained(Arc::new(())).await;
    assert!(matches!(
        busy,
        Err(TaskLockDetailedError::Operational(TaskLockError::Held))
    ));
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let remaining: i64 = redis::cmd("PTTL")
        .arg(key)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(
        remaining <= 1000,
        "Source SeriesRules has no renewer; measured PTTL={remaining}"
    );
    let _: String = redis::cmd("SET")
        .arg(key)
        .arg("owned-foreign-series")
        .arg("EX")
        .arg(30)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(!lease.release_owned_detailed().await.unwrap());
    assert!(weak.upgrade().is_none());
    let foreign: String = redis::cmd("GET")
        .arg(key)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(foreign, "owned-foreign-series");
    assert_eq!(locks.available_leases(), 2);
    let _: i64 = redis::cmd("DEL")
        .arg(key)
        .query_async(&mut connection)
        .await
        .unwrap();
}
