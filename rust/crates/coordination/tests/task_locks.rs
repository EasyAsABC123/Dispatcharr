//! Disposable original-runtime Redis fixture only; no deployment credentials.
use dispatcharr_coordination::{TaskLockConfig, TaskLockError, TaskLocks};
use redis::AsyncCommands;
use std::time::Duration;

async fn wait_for(mut check: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !check().await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("bounded fixture wait");
}
#[tokio::test]
#[ignore = "requires disposable TASK_LOCK_DISPOSABLE_FIXTURE plus original helper peer"]
async fn original_mutual_exclusion_renewal_fencing_and_cancelled_commands() {
    assert_eq!(
        std::env::var("TASK_LOCK_DISPOSABLE_FIXTURE").as_deref(),
        Ok("local-test-only")
    );
    let prefix = std::env::var("TASK_LOCK_FIXTURE_PREFIX").unwrap();
    assert!(prefix.starts_with("rust_task_fixture_"));
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut redis = redis::Client::open(url.clone())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let config = TaskLockConfig {
        ttl: Duration::from_secs(2),
        renewal_interval: Duration::from_millis(200),
        max_leases: 2,
        max_commands: 4,
    };
    let locks = TaskLocks::connect(&url, config.clone()).await.unwrap();
    let defaults = TaskLocks::connect(&url, TaskLockConfig::default())
        .await
        .unwrap();
    let default_lease = defaults
        .acquire(&format!("{prefix}_default"), 8)
        .await
        .unwrap();
    assert!((295..=300).contains(&redis.ttl::<_, i64>(default_lease.key()).await.unwrap()));
    default_lease.release().await.unwrap();
    let source = format!("{prefix}_source");
    let source_key = format!("task_lock_{source}_1");
    wait_for(async || redis.exists::<_, bool>(&source_key).await.unwrap()).await;
    assert!(matches!(
        locks.acquire(&source, 1).await,
        Err(TaskLockError::Held)
    ));
    assert_eq!(redis.get::<_, String>(&source_key).await.unwrap(), "locked");
    let native = format!("{prefix}_native");
    let lease = locks.acquire(&native, 2).await.unwrap();
    let native_key = lease.key().to_owned();
    assert!(redis
        .get::<_, String>(&native_key)
        .await
        .unwrap()
        .starts_with("rust:"));
    let _: () = redis.set(format!("{prefix}:ready"), 1).await.unwrap();
    wait_for(async || {
        redis
            .exists::<_, bool>(format!("{prefix}:checked"))
            .await
            .unwrap()
    })
    .await;
    assert!(!redis.exists::<_, bool>(&source_key).await.unwrap());
    lease.release().await.unwrap();
    assert!(!redis.exists::<_, bool>(&native_key).await.unwrap());
    let _: () = redis.set(format!("{prefix}:released"), 1).await.unwrap();
    wait_for(async || {
        redis
            .exists::<_, bool>(format!("{prefix}:done"))
            .await
            .unwrap()
    })
    .await;

    // Renewal lasts beyond the initial lease; external replacement loses the
    // lease and neither renewal nor old-owner release may touch that successor.
    let task = format!("{prefix}_renew");
    let lease = locks.acquire(&task, 3).await.unwrap();
    let key = lease.key().to_owned();
    tokio::time::sleep(Duration::from_millis(2300)).await;
    assert!(!lease.is_lost());
    assert!(redis.ttl::<_, i64>(&key).await.unwrap() > 0);
    let _: () = redis.set_ex(&key, "successor", 30).await.unwrap();
    wait_for(async || lease.is_lost()).await;
    lease.release().await.unwrap();
    assert_eq!(redis.get::<_, String>(&key).await.unwrap(), "successor");
    assert!(redis.ttl::<_, i64>(&key).await.unwrap() <= 30);
    let _: usize = redis.del(&key).await.unwrap();

    // Hold actual Redis write replies, cancel both acquisition receivers, and
    // verify physical admission remains held through late SET and cleanup.
    wait_for(async || locks.available_leases() == 2).await;
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(300)
        .arg("WRITE")
        .query_async(&mut redis)
        .await
        .unwrap();
    let first = locks.clone();
    let first_task = format!("{prefix}_cancel_a");
    let a = tokio::spawn(async move { first.acquire(&first_task, 4).await });
    let second = locks.clone();
    let second_task = format!("{prefix}_cancel_b");
    let b = tokio::spawn(async move { second.acquire(&second_task, 5).await });
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(locks.available_leases(), 0);
    a.abort();
    b.abort();
    let _ = a.await;
    let _ = b.await;
    assert!(matches!(
        locks.acquire(&format!("{prefix}_overload"), 6).await,
        Err(TaskLockError::Capacity)
    ));
    wait_for(async || locks.available_leases() == 2).await;
    for (suffix, id) in [("cancel_a", 4), ("cancel_b", 5)] {
        assert!(!redis
            .exists::<_, bool>(format!("task_lock_{prefix}_{suffix}_{id}"))
            .await
            .unwrap());
    }

    // Cancelling a release waiter must not cancel its physical cleanup.
    let lease = locks
        .acquire(&format!("{prefix}_release"), 7)
        .await
        .unwrap();
    let key = lease.key().to_owned();
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(300)
        .arg("WRITE")
        .query_async(&mut redis)
        .await
        .unwrap();
    let released = tokio::spawn(lease.release());
    tokio::time::sleep(Duration::from_millis(40)).await;
    released.abort();
    let _ = released.await;
    wait_for(async || locks.available_leases() == 2).await;
    assert!(!redis.exists::<_, bool>(&key).await.unwrap());
    for suffix in ["ready", "checked", "released", "done"] {
        let _: usize = redis.del(format!("{prefix}:{suffix}")).await.unwrap();
    }
}
